package readdesire

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamiclister"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// ---- shared fixtures, used across this package's test files --------------

const testManagementCluster = "test-cluster"

// testTargetVersion matches newUnstructuredConfigMap's hardcoded "apiVersion":
// "v1", so success-path tests' declared TargetVersion and fixture object
// version agree and don't spuriously trip observe's mismatch path.
const testTargetVersion = "v1"

var configMapGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

func readIdentity(namespace, name string) desire.Identity {
	return desire.Identity{
		ManagementCluster: testManagementCluster,
		Type:              desire.TypeRead,
		Group:             "",
		Resource:          "configmaps",
		Namespace:         namespace,
		Name:              name,
	}
}

func newUnstructuredConfigMap(name, namespace string, data map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"data": data,
	}}
}

// newLister builds a cache.GenericLister backed by a plain indexer seeded
// with objs directly - no real informer/watch needed, so sync/observe tests
// are deterministic and don't depend on informer startup timing.
func newLister(t *testing.T, gvr schema.GroupVersionResource, objs ...*unstructured.Unstructured) cache.GenericLister {
	t.Helper()
	indexer := cache.NewIndexer(
		cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	for _, obj := range objs {
		if err := indexer.Add(obj); err != nil {
			t.Fatalf("indexer.Add: %v", err)
		}
	}
	return dynamiclister.NewRuntimeObjectShim(dynamiclister.New(indexer, gvr))
}

func seedReadDesire(t *testing.T, store desire.SpecStore, id desire.Identity, owner string) desire.ReadDesire {
	t.Helper()
	d, err := store.CreateReadDesire(context.Background(), desire.ReadDesire{
		Identity: id, Owner: owner, TargetVersion: testTargetVersion,
	})
	if err != nil {
		t.Fatalf("CreateReadDesire(%+v): %v", id, err)
	}
	return d
}

func findCondition(status desire.Status, condType string) *metav1.Condition {
	for i := range status.Conditions {
		if status.Conditions[i].Type == condType {
			return &status.Conditions[i]
		}
	}
	return nil
}

func newFakeDynamicClient(t *testing.T, objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register corev1: %v", err)
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, nil, objs...)
}

// testMapper is a meta.ResettableRESTMapper that resolves configMapGVR (group
// "", version "v1", resource "configmaps") - real enough to exercise
// resolveGVR's success path in tests, unlike noMatchMapper.
type testMapper struct {
	meta.RESTMapper
}

func (testMapper) Reset() {}

func newTestMapper() testMapper {
	dm := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Group: "", Version: "v1"}})
	dm.AddSpecific(
		schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
		configMapGVR,
		schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmap"},
		meta.RESTScopeNamespace,
	)
	return testMapper{RESTMapper: dm}
}

// noMatchMapper is a meta.ResettableRESTMapper with nothing registered, so
// every ResourceFor call returns a NoMatchError - used to exercise
// pollOnce's unresolvable-GVR path deterministically.
type noMatchMapper struct {
	meta.RESTMapper
}

func (noMatchMapper) Reset() {}

func newNoMatchMapper() noMatchMapper {
	return noMatchMapper{RESTMapper: meta.NewDefaultRESTMapper(nil)}
}

// ---- pollOnce ---------------------------------------------------------

// TestPollOnce_UnresolvableGVRRecordsPreCheckFailed proves that a ReadDesire
// whose Group/Resource can never be mapped to a real GroupVersionResource
// gets Successful=False/PreCheckFailed recorded on its status - mirroring
// applydesire's preCheckFailed for an unmappable manifest - rather than
// being silently skipped with only a log line.
func TestPollOnce_UnresolvableGVRRecordsPreCheckFailed(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-bad-gvr")
	seedReadDesire(t, store, id, "owner-1")

	c := NewController(store, store, newFakeDynamicClient(t), newNoMatchMapper(), testManagementCluster, time.Second)

	c.pollOnce(ctx)

	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonPreCheckFailed {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonPreCheckFailed)
	}
	if _, ok := c.informers.Lister(id); ok {
		t.Errorf("informer exists for a desire whose GVR never resolved, want none")
	}
}
