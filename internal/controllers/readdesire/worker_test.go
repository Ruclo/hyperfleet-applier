package readdesire

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// ---- fixtures & helpers -------------------------------------------------

// newControllerWithLister builds a Controller whose InformerManager has at
// most one pre-seeded tracked entry for key, wired to lister - bypassing real
// informer startup entirely so sync/observe can be tested in isolation. A
// nil lister leaves no tracked entry, exercising the "no informer yet" path.
func newControllerWithLister(status statusStore, key desire.Identity, lister cache.GenericLister) *Controller {
	return newControllerWithListerAndDyn(status, key, lister, nil, nil)
}

// newControllerWithListerAndDyn is newControllerWithLister plus dyn/mapper,
// needed only by tests that exercise observeLive's direct-Get fallback (which
// calls resolveGVR against mapper and Get against dyn) - every other test
// only ever reaches the Lister, so nil dyn/mapper there are never dereferenced.
func newControllerWithListerAndDyn(
	status statusStore, key desire.Identity, lister cache.GenericLister,
	dyn dynamic.Interface, mapper meta.ResettableRESTMapper,
) *Controller {
	im := &InformerManager{informers: map[desire.Identity]*trackedInformer{}}
	if lister != nil {
		im.informers[key] = &trackedInformer{lister: lister}
	}
	return &Controller{status: status, informers: im, dyn: dyn, mapper: mapper}
}

// ---- decorators used to observe/inject store behavior -------------------

// countingStatusStore counts UpdateReadDesireStatus calls, to prove the
// no-op path never reaches the store.
type countingStatusStore struct {
	statusStore
	updateCalls int
}

func (c *countingStatusStore) UpdateReadDesireStatus(
	ctx context.Context, id desire.Identity, status desire.ReadStatus,
) (desire.ReadDesire, error) {
	c.updateCalls++
	return c.statusStore.UpdateReadDesireStatus(ctx, id, status)
}

// erroringStatusStore fails every UpdateReadDesireStatus call with err.
type erroringStatusStore struct {
	statusStore
	err error
}

func (e *erroringStatusStore) UpdateReadDesireStatus(
	context.Context, desire.Identity, desire.ReadStatus,
) (desire.ReadDesire, error) {
	return desire.ReadDesire{}, e.err
}

// getErroringStatusStore fails every GetReadDesire call with err - used to
// drive sync() into returning a specific error (including context.Canceled/
// DeadlineExceeded, which never occurs naturally against memory.Store) so
// processNextWorkItem's Forget-vs-AddRateLimited dispatch can be tested
// deterministically without needing a real canceled context.
type getErroringStatusStore struct {
	statusStore
	err error
}

func (g *getErroringStatusStore) GetReadDesire(context.Context, desire.Identity) (desire.ReadDesire, error) {
	return desire.ReadDesire{}, g.err
}

// ---- tests ---------------------------------------------------------------

// TestSync_ObserveOutcomes covers observe's three read outcomes, which
// differ only in what's tracked by InformerManager: a found object, a
// tracked informer with an empty cache, and no tracked informer at all. The
// latter two both record ReasonNotFound but exercise different branches
// (observe's !ok check vs. the lister's own NotFound), which is why they're
// kept as distinct cases rather than merged into one.
func TestSync_ObserveOutcomes(t *testing.T) {
	const namespace = "default"
	cases := []struct {
		obj        *unstructured.Unstructured
		name       string
		cmName     string
		wantStatus metav1.ConditionStatus
		wantReason string
		noInformer bool
	}{
		{
			name:       "FoundObjectRecordsSynced",
			cmName:     "cm-found",
			obj:        newUnstructuredConfigMap("cm-found", namespace, map[string]any{"k": "v"}),
			wantStatus: metav1.ConditionTrue,
			wantReason: desire.ReasonSynced,
		},
		{
			name:       "TrackedInformerEmptyCacheRecordsNotFound",
			cmName:     "cm-empty-cache",
			wantStatus: metav1.ConditionFalse,
			wantReason: desire.ReasonNotFound,
		},
		{
			name:       "NoTrackedInformerRecordsNotFound",
			cmName:     "cm-no-informer",
			noInformer: true,
			wantStatus: metav1.ConditionFalse,
			wantReason: desire.ReasonNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := memory.New()
			id := readIdentity(namespace, tc.cmName)
			seedReadDesire(t, store, id, "owner-1")

			var lister cache.GenericLister
			switch {
			case tc.noInformer:
				// lister stays nil: newControllerWithLister leaves no tracked entry.
			case tc.obj != nil:
				lister = newLister(t, configMapGVR, tc.obj)
			default:
				lister = newLister(t, configMapGVR) // tracked, but empty
			}
			c := newControllerWithLister(store, id, lister)

			if err := c.sync(ctx, id); err != nil {
				t.Fatalf("sync() error = %v, want nil", err)
			}

			got, err := store.GetReadDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			cond := findCondition(got.Status.Status, desire.TypeSuccessful)
			if cond == nil || cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
				t.Errorf("condition = %+v, want Status=%s Reason=%q", cond, tc.wantStatus, tc.wantReason)
			}

			if tc.obj != nil {
				wantContent, err := json.Marshal(tc.obj)
				if err != nil {
					t.Fatalf("marshal want content: %v", err)
				}
				if string(got.Status.KubeContent) != string(wantContent) {
					t.Errorf("KubeContent = %s, want %s", got.Status.KubeContent, wantContent)
				}
			} else if got.Status.KubeContent != nil {
				t.Errorf("KubeContent = %s, want nil", got.Status.KubeContent)
			}
		})
	}
}

func TestSync_DeletedDesireIsNoop(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-gone") // never seeded: GetReadDesire returns ErrNotFound

	c := newControllerWithLister(store, id, nil)
	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil for a desire that no longer exists", err)
	}
}

func TestSync_UnchangedObjectSuppressesStatusWrite(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	counting := &countingStatusStore{statusStore: base}
	id := readIdentity("default", "cm-noop")
	seedReadDesire(t, base, id, "owner-1")

	obj := newUnstructuredConfigMap("cm-noop", "default", map[string]any{"k": "v"})
	c := newControllerWithLister(counting, id, newLister(t, configMapGVR, obj))

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() [1st] error = %v, want nil", err)
	}
	if counting.updateCalls != 1 {
		t.Fatalf("updateCalls after 1st sync = %d, want 1", counting.updateCalls)
	}

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() [2nd] error = %v, want nil", err)
	}
	if counting.updateCalls != 1 {
		t.Errorf(
			"updateCalls after 2nd sync (unchanged) = %d, want still 1: reconciling an unchanged object must suppress the write",
			counting.updateCalls,
		)
	}
}

// TestSync_UpdateFailureIsPropagatedForRetry proves that any
// UpdateReadDesireStatus failure is returned by sync (not swallowed), so
// processNextWorkItem's AddRateLimited retries it promptly. Read status
// writes are decoupled from the shared per-resource Version (no CAS, never
// ErrVersionConflict - see statusStore), so this covers a generic backend
// failure rather than a version race specifically.
func TestSync_UpdateFailureIsPropagatedForRetry(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	id := readIdentity("default", "cm-update-fails")
	seedReadDesire(t, base, id, "owner-1")

	updateErr := errors.New("status store unavailable")
	failing := &erroringStatusStore{statusStore: base, err: updateErr}
	obj := newUnstructuredConfigMap("cm-update-fails", "default", map[string]any{"k": "v"})
	c := newControllerWithLister(failing, id, newLister(t, configMapGVR, obj))

	err := c.sync(ctx, id)
	if !errors.Is(err, updateErr) {
		t.Fatalf("sync() error = %v, want it to wrap %v so the workqueue retries", err, updateErr)
	}
}

// staleDataValue marks a cached configmap fixture as the stale one being
// superseded by a live re-Get, across the observeLive fallback tests below.
const staleDataValue = "stale"

// TestSync_VersionMismatchFallback covers observeLive's outcomes when the
// informer cache's object disagrees with the declared TargetVersion "v1":
// recovering via a live Get that agrees, escalating when the live Get still
// disagrees or is genuinely gone, and escalating when the fallback GVR
// itself can't be resolved.
func TestSync_VersionMismatchFallback(t *testing.T) {
	const namespace = "default"
	cases := []struct {
		mapper          meta.ResettableRESTMapper
		name            string
		cmName          string
		liveAPIVersion  string
		wantStatus      metav1.ConditionStatus
		wantReason      string
		liveExists      bool
		wantLiveContent bool
	}{
		{
			name:            "RecoversViaLiveGet",
			cmName:          "cm-recovers-via-live-get",
			liveExists:      true,
			mapper:          newTestMapper(),
			wantStatus:      metav1.ConditionTrue,
			wantReason:      desire.ReasonSynced,
			wantLiveContent: true,
		},
		{
			name:           "EscalatesWhenLiveGetStillWrong",
			cmName:         "cm-escalates-when-live-get-still-wrong",
			liveExists:     true,
			liveAPIVersion: "v2",
			mapper:         newTestMapper(),
			wantStatus:     metav1.ConditionFalse,
			wantReason:     desire.ReasonKubeAPIError,
		},
		{
			name:       "LiveGetNotFoundRecordsNotFound",
			cmName:     "cm-live-get-not-found",
			mapper:     newTestMapper(),
			wantStatus: metav1.ConditionFalse,
			wantReason: desire.ReasonNotFound,
		},
		{
			name:       "EscalatesWhenGVRResolutionFails",
			cmName:     "cm-escalates-when-gvr-resolution-fails",
			mapper:     newNoMatchMapper(),
			wantStatus: metav1.ConditionFalse,
			wantReason: desire.ReasonKubeAPIError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			base := memory.New()
			id := readIdentity(namespace, tc.cmName)
			seedReadDesire(t, base, id, "owner-1") // declares TargetVersion "v1"

			// The cache is always stale in this scenario: it reports apiVersion "v2".
			stale := newUnstructuredConfigMap(id.Name, namespace, map[string]any{"k": staleDataValue})
			stale.SetAPIVersion("v2")

			dyn := newFakeDynamicClient(t)
			var createdLive *unstructured.Unstructured
			if tc.liveExists {
				live := newUnstructuredConfigMap(id.Name, namespace, map[string]any{"k": "live"})
				if tc.liveAPIVersion != "" {
					live.SetAPIVersion(tc.liveAPIVersion)
				}
				created, err := dyn.Resource(configMapGVR).Namespace(namespace).Create(ctx, live, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("seed live object: %v", err)
				}
				createdLive = created
			}

			c := newControllerWithListerAndDyn(base, id, newLister(t, configMapGVR, stale), dyn, tc.mapper)

			if err := c.sync(ctx, id); err != nil {
				t.Fatalf("sync() error = %v, want nil", err)
			}

			got, err := base.GetReadDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			cond := findCondition(got.Status.Status, desire.TypeSuccessful)
			if cond == nil || cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
				t.Errorf("condition = %+v, want Status=%s Reason=%q", cond, tc.wantStatus, tc.wantReason)
			}

			if tc.wantLiveContent {
				wantContent, err := json.Marshal(createdLive)
				if err != nil {
					t.Fatalf("marshal want content: %v", err)
				}
				if string(got.Status.KubeContent) != string(wantContent) {
					t.Errorf("KubeContent = %s, want the live object's content %s, not the stale cached one",
						got.Status.KubeContent, wantContent)
				}
			}
		})
	}
}

// ---- processNextWorkItem retry dispatch ----------------------------------

// TestProcessNextWorkItem_SuccessForgetsKey proves a successful sync forgets
// the key, resetting any prior backoff, rather than leaving it rate-limited.
func TestProcessNextWorkItem_SuccessForgetsKey(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-success")
	seedReadDesire(t, store, id, "owner-1")
	obj := newUnstructuredConfigMap("cm-success", "default", map[string]any{"k": "v"})

	c := newControllerWithLister(store, id, newLister(t, configMapGVR, obj))
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	defer queue.ShutDown()
	c.queue = queue

	// Simulate a prior failed attempt so Forget's effect (resetting the
	// backoff counter) is actually observable, instead of trivially true.
	queue.AddRateLimited(id)
	if n := queue.NumRequeues(id); n != 1 {
		t.Fatalf("test setup: NumRequeues = %d, want 1", n)
	}

	if !c.processNextWorkItem(ctx) {
		t.Fatal("processNextWorkItem() = false, want true (queue not shut down)")
	}
	if n := queue.NumRequeues(id); n != 0 {
		t.Errorf("NumRequeues = %d, want 0: a successful sync must Forget the key, resetting backoff", n)
	}
}

// TestProcessNextWorkItem_ContextCanceledForgetsKey proves context
// cancellation is treated as caller-driven shutdown, not a resource
// failure: the key is forgotten, never retried with backoff.
func TestProcessNextWorkItem_ContextCanceledForgetsKey(t *testing.T) {
	ctx := context.Background()
	id := readIdentity("default", "cm-canceled")
	failing := &getErroringStatusStore{statusStore: memory.New(), err: context.Canceled}

	c := newControllerWithLister(failing, id, nil)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	defer queue.ShutDown()
	c.queue = queue
	queue.Add(id)

	c.processNextWorkItem(ctx)

	if n := queue.NumRequeues(id); n != 0 {
		t.Errorf("NumRequeues = %d, want 0: context cancellation must not trigger a backoff retry", n)
	}
}

// TestProcessNextWorkItem_GenericErrorRetries proves a generic sync failure
// (backend unavailable, not context cancellation) is retried via
// AddRateLimited rather than being dropped.
func TestProcessNextWorkItem_GenericErrorRetries(t *testing.T) {
	ctx := context.Background()
	id := readIdentity("default", "cm-retry")
	failing := &getErroringStatusStore{statusStore: memory.New(), err: errors.New("backend unavailable")}

	c := newControllerWithLister(failing, id, nil)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	defer queue.ShutDown()
	c.queue = queue
	queue.Add(id)

	c.processNextWorkItem(ctx)

	if n := queue.NumRequeues(id); n != 1 {
		t.Errorf("NumRequeues = %d, want 1: a generic sync failure must be retried with backoff", n)
	}
}
