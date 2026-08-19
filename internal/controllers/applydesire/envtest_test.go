//go:build envtest

package applydesire

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	discomemory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

var (
	envTestEnvironment *envtest.Environment
	envDynamicClient   dynamic.Interface
	envRESTMapper      *restmapper.DeferredDiscoveryRESTMapper
)

func TestMain(m *testing.M) {
	envTestEnvironment = &envtest.Environment{}

	cfg, err := envTestEnvironment.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest: failed to start test environment: %v\n", err)
		os.Exit(1)
	}

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		if stopErr := envTestEnvironment.Stop(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "envtest: failed to stop test environment: %v\n", stopErr)
		}
		fmt.Fprintf(os.Stderr, "envtest: failed to build dynamic client: %v\n", err)
		os.Exit(1)
	}
	envDynamicClient = dyn

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		if stopErr := envTestEnvironment.Stop(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "envtest: failed to stop test environment: %v\n", stopErr)
		}
		fmt.Fprintf(os.Stderr, "envtest: failed to build discovery client: %v\n", err)
		os.Exit(1)
	}
	envRESTMapper = restmapper.NewDeferredDiscoveryRESTMapper(discomemory.NewMemCacheClient(discoveryClient))

	code := m.Run()

	if err := envTestEnvironment.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "envtest: failed to stop test environment: %v\n", err)
		if code == 0 {
			// A teardown failure (leaked apiserver/etcd processes) must not be
			// masked by passing tests.
			code = 1
		}
	}
	os.Exit(code)
}

// TestEnvtest_UnchangedReconcileIsClusterNoOp asserts that reconciling the
// same ApplyDesire content twice in a row against a real apiserver does not
// mutate the object a second time: resourceVersion must be identical before
// and after the second pass.
func TestEnvtest_UnchangedReconcileIsClusterNoOp(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	const name = "cm-envtest-noop"

	r := New(store, store, envDynamicClient, envRESTMapper, testManagementCluster)

	id := applyIdentity("", "configmaps", defaultNamespace, name)
	content := newConfigMapContent(t, name, defaultNamespace, map[string]string{"k": "v"})
	seedApplyDesire(t, store, id, "owner-1", content)

	if err := r.ReconcileAll(ctx); err != nil {
		t.Fatalf("ReconcileAll() [pass 1] error = %v, want nil", err)
	}

	obj1, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get after pass 1: %v", err)
	}
	rv1 := obj1.GetResourceVersion()
	if rv1 == "" {
		t.Fatalf("resourceVersion is empty after pass 1; object was not created as expected")
	}

	if rcErr := r.ReconcileAll(ctx); rcErr != nil {
		t.Fatalf("ReconcileAll() [pass 2] error = %v, want nil", rcErr)
	}

	obj2, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get after pass 2: %v", err)
	}
	rv2 := obj2.GetResourceVersion()

	if rv1 != rv2 {
		t.Errorf(
			"resourceVersion changed from %q to %q across an unchanged reconcile pass, want a true cluster no-op",
			rv1, rv2,
		)
	}
}

// TestEnvtest_ForceReclaimsContestedField asserts that when another field
// manager already owns a field, the Reconciler's SSA apply (FieldManager,
// Force: true) reclaims ownership of that field from the other manager.
func TestEnvtest_ForceReclaimsContestedField(t *testing.T) {
	ctx := context.Background()
	const name = "cm-envtest-force"

	externalObj := &unstructured.Unstructured{Object: map[string]interface{}{
		fieldAPIVersion: "v1",
		fieldKind:       kindConfigMap,
		fieldMetadata: map[string]interface{}{
			fieldName:      name,
			fieldNamespace: defaultNamespace,
			"labels":       map[string]interface{}{"contested": "external-value"},
		},
	}}

	if _, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Apply(
		ctx, name, externalObj, metav1.ApplyOptions{FieldManager: "external-owner", Force: true},
	); err != nil {
		t.Fatalf("seeding external field ownership: %v", err)
	}

	store := memory.New()
	r := New(store, store, envDynamicClient, envRESTMapper, testManagementCluster)

	content, err := json.Marshal(map[string]interface{}{
		fieldAPIVersion: "v1",
		fieldKind:       kindConfigMap,
		fieldMetadata: map[string]interface{}{
			fieldName:      name,
			fieldNamespace: defaultNamespace,
			"labels":       map[string]interface{}{"contested": "reconciler-value"},
		},
	})
	if err != nil {
		t.Fatalf("marshal content: %v", err)
	}

	id := applyIdentity("", "configmaps", defaultNamespace, name)
	seedApplyDesire(t, store, id, "owner-1", content)

	if rcErr := r.ReconcileAll(ctx); rcErr != nil {
		t.Fatalf("ReconcileAll() error = %v, want nil", rcErr)
	}

	got, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v := got.GetLabels()["contested"]; v != "reconciler-value" {
		t.Errorf(
			"labels[contested] = %q, want %q: Force must reclaim a field owned by another field manager",
			v, "reconciler-value",
		)
	}

	got2, err := store.GetApplyDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetApplyDesire: %v", err)
	}
	if c := findCondition(got2.Status, desire.TypeSuccessful); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("desire status condition = %+v, want Successful=True after the reconcile", c)
	}
}

// TestEnvtest_ClusterScopedStrayNamespace verifies against a real kube-apiserver
// that cluster-scoped manifests with metadata.namespace still reach SSA; the test
// records whether the API accepts or rejects them.
func TestEnvtest_ClusterScopedStrayNamespace(t *testing.T) {
	ctx := context.Background()
	const name = "cr-envtest-stray-ns"

	store := memory.New()
	r := New(store, store, envDynamicClient, envRESTMapper, testManagementCluster)

	id := applyIdentity(rbacGroup, "clusterroles", "", name)
	content := newClusterRoleContentWithNamespace(t, name, "stray-namespace")
	seedApplyDesire(t, store, id, "owner-1", content)

	t.Cleanup(func() {
		_ = envDynamicClient.Resource(clusterRoleGVR).Delete(ctx, name, metav1.DeleteOptions{})
	})

	if err := r.ReconcileAll(ctx); err != nil {
		t.Fatalf("ReconcileAll() error = %v, want nil", err)
	}

	got, err := store.GetApplyDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetApplyDesire: %v", err)
	}
	c := findCondition(got.Status, desire.TypeSuccessful)
	if c == nil {
		t.Fatalf("status has no %q condition: %+v", desire.TypeSuccessful, got.Status)
	}
	if c.Reason == desire.ReasonPreCheckFailed {
		t.Fatalf(
			"Reason = %q, want not %q: applier must forward stray namespace to the apiserver",
			c.Reason, desire.ReasonPreCheckFailed,
		)
	}

	switch c.Reason {
	case desire.ReasonApplied:
		obj, getErr := envDynamicClient.Resource(clusterRoleGVR).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			t.Fatalf("Get ClusterRole after Applied: %v", getErr)
		}
		if ns := obj.GetNamespace(); ns != "" {
			t.Errorf("live ClusterRole namespace = %q, want empty (apiserver must not persist namespace)", ns)
		}
	case desire.ReasonKubeAPIError:
		t.Logf(
			"apiserver rejected cluster-scoped manifest with metadata.namespace (Message=%q); applier recorded KubeAPIError",
			c.Message,
		)
	default:
		t.Fatalf("unexpected condition after reconcile: %+v", c)
	}
}
