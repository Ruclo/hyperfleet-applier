//go:build envtest

package readdesire

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
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

// TestMain starts a shared envtest apiserver for this package's envtest tests.
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

// waitForReason polls store for id's ReadDesire until its Successful
// condition's Reason matches want, or ctx's deadline is hit. Real watch
// delivery has real (if small) latency against envtest's apiserver, unlike
// the fakes the rest of this package's tests use.
func waitForReason(
	t *testing.T, ctx context.Context, store desire.SpecStore, id desire.Identity, want string,
) desire.ReadDesire {
	t.Helper()
	var last desire.ReadDesire
	condition := func(ctx context.Context) (bool, error) {
		got, err := store.GetReadDesire(ctx, id)
		if err != nil {
			return false, err
		}
		last = got
		cond := findCondition(got.Status.Status, desire.TypeSuccessful)
		return cond != nil && cond.Reason == want, nil
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, condition); err != nil {
		t.Fatalf("waiting for Reason=%q: %v (last status: %+v)", want, err, last.Status)
	}
	return last
}

// waitForReasonAndContent polls until id's ReadDesire is Synced and its
// KubeContent contains want - an update can land as two rapid, distinct
// UpdateFunc-driven syncs (the old content synced again from a race, then
// the new one), so waiting on Reason alone could observe the wrong one.
func waitForReasonAndContent(
	t *testing.T, ctx context.Context, store desire.SpecStore, id desire.Identity, want string,
) desire.ReadDesire {
	t.Helper()
	var last desire.ReadDesire
	condition := func(ctx context.Context) (bool, error) {
		got, err := store.GetReadDesire(ctx, id)
		if err != nil {
			return false, err
		}
		last = got
		cond := findCondition(got.Status.Status, desire.TypeSuccessful)
		synced := cond != nil && cond.Reason == desire.ReasonSynced
		return synced && strings.Contains(string(got.Status.KubeContent), want), nil
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, condition); err != nil {
		t.Fatalf("waiting for Synced content containing %q: %v (last status: %+v)", want, err, last.Status)
	}
	return last
}

// TestEnvtest_FullLifecycle proves the whole readdesire mechanism against a
// real apiserver, not dynamicfake - which silently ignores the
// metadata.name field selector InformerManager.start scopes every informer
// with, so none of this package's other tests can prove informers actually
// isolate the way the design assumes. A target that doesn't exist yet
// reports NotFound; appears as Synced once created; an unrelated object in
// the same namespace/GVR never affects it (the field-selector isolation
// itself); a real update is reflected; and it reports NotFound again once
// deleted. GVR-resolution/status-computation branches (PreCheckFailed,
// KubeAPIError, the TargetVersion mismatch fallback, retry dispatch) are
// pure Go control flow already covered by the unit tests and are
// deliberately not repeated here.
func TestEnvtest_FullLifecycle(t *testing.T) {
	const namespace = "default"
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store := memory.New()
	id := readIdentity(namespace, "cm-envtest-lifecycle")
	if _, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: id, Owner: testOwner, TargetVersion: testTargetVersion,
	}); err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	c := New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, 100*time.Millisecond)
	done := make(chan error, 1)
	go func() {
		done <- c.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Start() error = %v, want nil after cancellation", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Start did not return after context cancellation")
		}
	})

	// 1. Target doesn't exist yet.
	waitForReason(t, ctx, store, id, desire.ReasonNotFound)

	// 2. Create the target - status must transition to Synced and mirror it.
	target := newUnstructuredConfigMap(id.Name, namespace, map[string]any{"k": "v1"})
	if _, err := envDynamicClient.Resource(configMapGVR).Namespace(namespace).Create(
		ctx, target, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("create target: %v", err)
	}
	t.Cleanup(func() {
		_ = envDynamicClient.Resource(configMapGVR).Namespace(namespace).Delete(
			context.Background(), id.Name, metav1.DeleteOptions{},
		)
	})
	got := waitForReason(t, ctx, store, id, desire.ReasonSynced)
	if !strings.Contains(string(got.Status.KubeContent), `"k":"v1"`) {
		t.Errorf("KubeContent = %s, want it to contain the initial data", got.Status.KubeContent)
	}

	// 3. An unrelated object in the same namespace/GVR must never affect
	// this desire's status - the one thing dynamicfake can't prove, since it
	// ignores field selectors entirely.
	unrelated := newUnstructuredConfigMap("cm-envtest-unrelated", namespace, map[string]any{"k": "other"})
	if _, err := envDynamicClient.Resource(configMapGVR).Namespace(namespace).Create(
		ctx, unrelated, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("create unrelated object: %v", err)
	}
	t.Cleanup(func() {
		_ = envDynamicClient.Resource(configMapGVR).Namespace(namespace).Delete(
			context.Background(), "cm-envtest-unrelated", metav1.DeleteOptions{},
		)
	})
	time.Sleep(500 * time.Millisecond) // give a wrongly-unfiltered informer a chance to react
	stillGot, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	if string(stillGot.Status.KubeContent) != string(got.Status.KubeContent) {
		t.Errorf("KubeContent changed after creating an unrelated object in the same namespace/GVR - "+
			"field-selector isolation failed: got %s, want unchanged %s",
			stillGot.Status.KubeContent, got.Status.KubeContent)
	}

	// 4. Update the target - status must reflect the real update, not just
	// the create.
	live, err := envDynamicClient.Resource(configMapGVR).Namespace(namespace).Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get target before update: %v", err)
	}
	if err := unstructured.SetNestedStringMap(live.Object, map[string]string{"k": "v2"}, "data"); err != nil {
		t.Fatalf("set updated data: %v", err)
	}
	if _, err := envDynamicClient.Resource(configMapGVR).Namespace(namespace).Update(
		ctx, live, metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("update target: %v", err)
	}
	got = waitForReasonAndContent(t, ctx, store, id, `"k":"v2"`)
	if strings.Contains(string(got.Status.KubeContent), `"k":"v1"`) {
		t.Errorf("KubeContent = %s, still contains the stale value after a real update", got.Status.KubeContent)
	}

	// 5. Delete the target - status must go back to NotFound.
	if err := envDynamicClient.Resource(configMapGVR).Namespace(namespace).Delete(
		ctx, id.Name, metav1.DeleteOptions{},
	); err != nil {
		t.Fatalf("delete target: %v", err)
	}
	waitForReason(t, ctx, store, id, desire.ReasonNotFound)
}
