package readdesire

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// TestInformerManager_ReconcileStartsAndStopsInformers proves Reconcile's
// bookkeeping is synchronous: a key becomes visible via Lister as soon as
// Reconcile returns (the map write happens before start's background
// goroutines are spawned), and disappears synchronously once no longer
// wanted, without needing to wait for informer sync.
func TestInformerManager_ReconcileStartsAndStopsInformers(t *testing.T) {
	dyn := newFakeDynamicClient(t)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	m := newInformerManager(dyn, queue)
	t.Cleanup(m.shutdownAll)

	id := readIdentity("default", "cm-lifecycle")
	key := id
	target := informerTarget{gvr: configMapGVR, namespace: "default", name: "cm-lifecycle"}
	seen := map[desire.Identity]struct{}{key: {}}
	want := map[desire.Identity]informerTarget{key: target}

	m.Reconcile(seen, want)
	if _, ok := m.Lister(key); !ok {
		t.Fatalf("Lister(key) ok = false immediately after Reconcile started it, want true")
	}

	m.Reconcile(map[desire.Identity]struct{}{}, map[desire.Identity]informerTarget{}) // nothing left at all
	if _, ok := m.Lister(key); ok {
		t.Errorf("Lister(key) ok = true after Reconcile removed it, want false")
	}
}

// TestInformerManager_TransientResolveFailureDoesNotStopInformer proves that
// a key present in seen but absent from want (a desire still listed, but
// whose GVR failed to resolve this tick) does not tear down its already
// -running informer - only a key missing from seen entirely (the desire
// itself is gone) should.
func TestInformerManager_TransientResolveFailureDoesNotStopInformer(t *testing.T) {
	dyn := newFakeDynamicClient(t)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	m := newInformerManager(dyn, queue)
	t.Cleanup(m.shutdownAll)

	id := readIdentity("default", "cm-flaky-gvr")
	key := id
	target := informerTarget{gvr: configMapGVR, namespace: "default", name: "cm-flaky-gvr"}

	m.Reconcile(map[desire.Identity]struct{}{key: {}}, map[desire.Identity]informerTarget{key: target})
	if _, ok := m.Lister(key); !ok {
		t.Fatalf("Lister(key) ok = false immediately after Reconcile started it, want true")
	}

	// key is still seen (the desire is still listed) but resolveGVR failed
	// this tick, so want is empty - the informer must survive.
	m.Reconcile(map[desire.Identity]struct{}{key: {}}, map[desire.Identity]informerTarget{})
	if _, ok := m.Lister(key); !ok {
		t.Errorf("Lister(key) ok = false after a transient resolve failure, want true: " +
			"an already-running informer must not be torn down just because want no longer contains its key")
	}
}

// TestInformerManager_StartEnqueuesKeyEvenWhenTargetAbsent proves that
// starting an informer for a target that doesn't currently exist still
// results in the key being enqueued (via the post-cache-sync enqueue in
// start), even though AddFunc never fires for an object that was never
// there to begin with. Without this, such a desire would never get a single
// sync - not even a NotFound write - until/unless the object is later
// created.
func TestInformerManager_StartEnqueuesKeyEvenWhenTargetAbsent(t *testing.T) {
	const namespace = "default"
	dyn := newFakeDynamicClient(t) // nothing seeded: the target doesn't exist
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	m := newInformerManager(dyn, queue)
	t.Cleanup(m.shutdownAll)
	t.Cleanup(queue.ShutDown)

	id := readIdentity(namespace, "cm-absent")
	key := id
	target := informerTarget{gvr: configMapGVR, namespace: namespace, name: "cm-absent"}

	m.Reconcile(map[desire.Identity]struct{}{key: {}}, map[desire.Identity]informerTarget{key: target})

	got := make(chan desire.Identity, 1)
	go func() {
		k, shutdown := queue.Get()
		if !shutdown {
			got <- k
		}
	}()

	select {
	case k := <-got:
		if k != key {
			t.Errorf("enqueued key = %+v, want %+v", k, key)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for start to enqueue key for an absent target: " +
			"AddFunc never fires for an object that doesn't exist, so this must come from the post-sync enqueue")
	}
}

// TestInformerManager_RebuildsInformerOnVersionChange proves Reconcile tears
// down and rebuilds an informer when the resolved GVR for an already-tracked
// key changes (e.g. TargetVersion was updated), rather than leaving the
// stale-version informer running indefinitely.
func TestInformerManager_RebuildsInformerOnVersionChange(t *testing.T) {
	const namespace = "default"
	dyn := newFakeDynamicClient(t)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	m := newInformerManager(dyn, queue)
	t.Cleanup(m.shutdownAll)

	id := readIdentity(namespace, "cm-version-change")
	key := id
	seen := map[desire.Identity]struct{}{key: {}}

	v1GVR := configMapGVR
	v2GVR := schema.GroupVersionResource{Group: configMapGVR.Group, Version: "v2", Resource: configMapGVR.Resource}

	m.Reconcile(seen, map[desire.Identity]informerTarget{
		key: {gvr: v1GVR, namespace: namespace, name: "cm-version-change"},
	})
	firstLister, ok := m.Lister(key)
	if !ok {
		t.Fatalf("Lister(key) ok = false after first Reconcile, want true")
	}

	m.Reconcile(seen, map[desire.Identity]informerTarget{
		key: {gvr: v2GVR, namespace: namespace, name: "cm-version-change"},
	})
	secondLister, ok := m.Lister(key)
	if !ok {
		t.Fatalf("Lister(key) ok = false after version-change Reconcile, want true")
	}
	if firstLister == secondLister {
		t.Errorf("Lister unchanged after a version change - informer was not rebuilt")
	}
}
