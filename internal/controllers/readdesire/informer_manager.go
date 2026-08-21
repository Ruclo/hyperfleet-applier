package readdesire

import (
	"fmt"
	"log/slog"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// informerTarget is what InformerManager needs to build and scope one
// per-desire informer: the resolved GVR to watch, and the specific
// namespace/name to filter to via a metadata.name field selector.
type informerTarget struct {
	gvr       schema.GroupVersionResource
	namespace string
	name      string
}

// trackedInformer pairs a running informer and its lister with the stop
// channel that controls only its own lifecycle, so one desire's informer can
// be shut down independently of the others. lister is retained so sync can
// read the cached object without hitting the apiserver: see
// InformerManager.Lister.
type trackedInformer struct {
	informer cache.SharedIndexInformer
	lister   cache.GenericLister
	stopCh   chan struct{}
	gvr      schema.GroupVersionResource
}

// InformerManager keeps exactly one running, name-scoped informer per
// ReadDesire currently known to Controller. When a desire drops out of the
// wanted set (it was deleted), Reconcile stops that desire's informer only.
type InformerManager struct {
	dyn       dynamic.Interface
	queue     workqueue.TypedRateLimitingInterface[desire.Identity]
	informers map[desire.Identity]*trackedInformer
	mu        sync.Mutex
}

func newInformerManager(
	dyn dynamic.Interface, queue workqueue.TypedRateLimitingInterface[desire.Identity],
) *InformerManager {
	return &InformerManager{
		dyn:       dyn,
		queue:     queue,
		informers: make(map[desire.Identity]*trackedInformer),
	}
}

// Reconcile starts an informer for every key in want that isn't already
// running on the exact target GVR (rebuilding it first if a tracked informer
// exists but on a stale GVR - e.g. the desire's declared TargetVersion
// changed), and stops every currently-running informer whose key is not in
// seen. seen is every desire currently listed (regardless of whether its GVR
// resolved this tick); want is the subset with a resolved target. Using seen rather
// than want for the stop decision means a transient GVR-resolution failure
// for an already-running desire only skips starting a new informer for it -
// it does not tear down (and lose the cache of) an already-healthy one; see
// Controller.pollOnce.
//
// The returned map holds one entry per key whose informer failed to start
// this call (nil if none failed) - the caller decides how to report each
// failure (e.g. Controller.pollOnce persists it via applyStatus, the same
// path used for a resolveGVR failure). A failed key stays absent from
// m.informers, so it's retried on the next Reconcile call as long as it
// remains in want.
//
// Called once per poll tick from a single goroutine (Controller.pollOnce), so
// it does not need to guard against concurrent Reconcile calls - only
// against shutdownAll/Lister running during Run's teardown or a concurrent
// sync call, hence the mutex.
func (m *InformerManager) Reconcile(
	seen map[desire.Identity]struct{}, want map[desire.Identity]informerTarget,
) map[desire.Identity]error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, ti := range m.informers {
		if _, ok := seen[key]; ok {
			continue
		}
		close(ti.stopCh)
		delete(m.informers, key)
	}

	var failed map[desire.Identity]error
	for key, target := range want {
		if ti, ok := m.informers[key]; ok {
			if ti.gvr == target.gvr {
				continue // already watching the right thing
			}
			// Declared TargetVersion changed for this ResourceKey since the
			// informer was built - tear down the stale one before starting a
			// fresh one on the new GVR.
			close(ti.stopCh)
			delete(m.informers, key)
		}
		if err := m.start(key, target); err != nil {
			if failed == nil {
				failed = make(map[desire.Identity]error)
			}
			failed[key] = err
		}
	}
	return failed
}

// Lister returns the cache.GenericLister for key's informer, if one is
// currently running for it (false otherwise - e.g. not started this poll
// tick yet, or already torn down). Intended for sync to read the cached
// object without an apiserver round trip, e.g.
// lister.ByNamespace(key.Namespace).Get(key.Name).
func (m *InformerManager) Lister(key desire.Identity) (cache.GenericLister, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ti, ok := m.informers[key]
	if !ok {
		return nil, false
	}
	return ti.lister, true
}

// start builds a single-object-scoped informer for target - a field selector
// on metadata.name, plus target.namespace (empty for cluster-scoped kinds,
// same convention applyToCluster relies on) - wires a handler that enqueues
// key on any Add/Update/Delete, and runs it in its own goroutine under its
// own stop channel. Because this informer only ever observes the one object
// it's scoped to, the handler needs no object introspection at all: any event
// it receives is by construction about key's desire, so it just enqueues key
// directly. Caller must hold m.mu.
//
// Returns an error (leaving key untracked, so Reconcile retries it next
// call) only if wiring the event handler fails - this can only happen if the
// informer has already stopped, which cannot happen here since it was just
// created, so in practice this is not expected to fire.
//
// cache.WaitForCacheSync is awaited in a background goroutine (not inline
// here) so that starting many new informers in one Reconcile call doesn't
// serialize on each other's initial List completing; its failure can't be
// returned here (this function has already returned by the time it runs) so
// it's log-only.
func (m *InformerManager) start(key desire.Identity, target informerTarget) error {
	tweakListOptions := func(opts *metav1.ListOptions) {
		opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", target.name).String()
	}
	gi := dynamicinformer.NewFilteredDynamicInformer(
		m.dyn, target.gvr, target.namespace, resyncPeriod, cache.Indexers{}, tweakListOptions,
	)
	informer := gi.Informer()

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ any) { m.queue.Add(key) },
		UpdateFunc: func(_, _ any) { m.queue.Add(key) },
		DeleteFunc: func(_ any) { m.queue.Add(key) },
	}); err != nil {
		return fmt.Errorf("readdesire: add event handler failed: %w", err)
	}

	stopCh := make(chan struct{})
	m.informers[key] = &trackedInformer{gvr: target.gvr, informer: informer, lister: gi.Lister(), stopCh: stopCh}
	go informer.Run(stopCh)
	go func() {
		if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
			slog.Error("readdesire: informer cache sync did not complete before shutdown",
				"namespace", key.Namespace, "name", key.Name)
			return
		}
		// AddFunc only fires if the target exists - Kubernetes has no event for
		// "object still absent", so a target that doesn't exist yet would
		// otherwise never get an initial sync at all (not even a NotFound
		// write) until/unless it's later created.
		m.queue.Add(key)
	}()
	return nil
}

// shutdownAll stops every currently-running informer. Called once, from
// Controller.Run's teardown after the poll loop has returned.
func (m *InformerManager) shutdownAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, ti := range m.informers {
		close(ti.stopCh)
		delete(m.informers, key)
	}
}
