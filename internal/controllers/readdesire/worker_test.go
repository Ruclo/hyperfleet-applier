package readdesire

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

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

func TestSync_FoundObjectRecordsSynced(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-1")
	seedReadDesire(t, store, id, "owner-1")

	obj := newUnstructuredConfigMap("cm-1", "default", map[string]any{"k": "v"})
	c := newControllerWithLister(store, id, newLister(t, configMapGVR, obj))

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != desire.ReasonSynced {
		t.Errorf("condition = %+v, want Status=True Reason=%q", cond, desire.ReasonSynced)
	}
	wantContent, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal want content: %v", err)
	}
	if string(got.Status.KubeContent) != string(wantContent) {
		t.Errorf("KubeContent = %s, want %s", got.Status.KubeContent, wantContent)
	}
}

func TestSync_MissingObjectRecordsNotFound(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-missing")
	seedReadDesire(t, store, id, "owner-1")

	c := newControllerWithLister(store, id, newLister(t, configMapGVR)) // empty lister

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonNotFound {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonNotFound)
	}
	if got.Status.KubeContent != nil {
		t.Errorf("KubeContent = %s, want nil", got.Status.KubeContent)
	}
}

func TestSync_NoInformerYetRecordsNotFound(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-no-informer")
	seedReadDesire(t, store, id, "owner-1")

	c := newControllerWithLister(store, id, nil) // no tracked informer at all

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}
	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Reason != desire.ReasonNotFound {
		t.Errorf("condition = %+v, want Reason=%q", cond, desire.ReasonNotFound)
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

// TestSync_VersionMismatchRecoversViaLiveGet proves that when the cached
// object's version disagrees with the declared TargetVersion, a direct Get
// that finds the object at the declared version is treated as a resolved
// informer-cache staleness: the live content is synced, not discarded.
func TestSync_VersionMismatchRecoversViaLiveGet(t *testing.T) {
	const namespace = "default"
	ctx := context.Background()
	base := memory.New()
	id := readIdentity(namespace, "cm-live-recovery")
	seedReadDesire(t, base, id, "owner-1") // declares TargetVersion "v1"

	// The cache is stale: it reports apiVersion "v2".
	stale := newUnstructuredConfigMap("cm-live-recovery", namespace, map[string]any{"k": staleDataValue})
	stale.SetAPIVersion("v2")

	// The live cluster actually has the object at the declared "v1".
	dyn := newFakeDynamicClient(t)
	live := newUnstructuredConfigMap("cm-live-recovery", namespace, map[string]any{"k": "live"})
	created, err := dyn.Resource(configMapGVR).Namespace(namespace).Create(ctx, live, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("seed live object: %v", err)
	}

	c := newControllerWithListerAndDyn(
		base, id, newLister(t, configMapGVR, stale), dyn, newTestMapper(),
	)

	if err = c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := base.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != desire.ReasonSynced {
		t.Errorf("condition = %+v, want Status=True Reason=%q", cond, desire.ReasonSynced)
	}
	wantContent, err := json.Marshal(created)
	if err != nil {
		t.Fatalf("marshal want content: %v", err)
	}
	if string(got.Status.KubeContent) != string(wantContent) {
		t.Errorf("KubeContent = %s, want the live object's content %s, not the stale cached one",
			got.Status.KubeContent, wantContent)
	}
}

// TestSync_VersionMismatchEscalatesWhenLiveGetStillWrong proves that when a
// direct Get still disagrees with the declared TargetVersion, this is no
// longer treated as transient cache staleness - it's escalated to a real
// KubeAPIError status write.
func TestSync_VersionMismatchEscalatesWhenLiveGetStillWrong(t *testing.T) {
	const namespace = "default"
	ctx := context.Background()
	base := memory.New()
	id := readIdentity(namespace, "cm-persistent-mismatch")
	seedReadDesire(t, base, id, "owner-1") // declares TargetVersion "v1"

	stale := newUnstructuredConfigMap("cm-persistent-mismatch", namespace, map[string]any{"k": staleDataValue})
	stale.SetAPIVersion("v2")

	// The live object also disagrees with the declared TargetVersion.
	dyn := newFakeDynamicClient(t)
	live := newUnstructuredConfigMap("cm-persistent-mismatch", namespace, map[string]any{"k": "live"})
	live.SetAPIVersion("v2")
	if _, err := dyn.Resource(configMapGVR).Namespace(namespace).Create(ctx, live, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed live object: %v", err)
	}

	c := newControllerWithListerAndDyn(
		base, id, newLister(t, configMapGVR, stale), dyn, newTestMapper(),
	)

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := base.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonKubeAPIError {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonKubeAPIError)
	}
}

// TestSync_VersionMismatchLiveGetNotFoundRecordsNotFound proves that if the
// direct Get following a cache version mismatch finds the object gone
// entirely, that's a genuine NotFound, not a KubeAPIError.
func TestSync_VersionMismatchLiveGetNotFoundRecordsNotFound(t *testing.T) {
	const namespace = "default"
	ctx := context.Background()
	base := memory.New()
	id := readIdentity(namespace, "cm-live-gone")
	seedReadDesire(t, base, id, "owner-1") // declares TargetVersion "v1"

	stale := newUnstructuredConfigMap("cm-live-gone", namespace, map[string]any{"k": staleDataValue})
	stale.SetAPIVersion("v2")

	dyn := newFakeDynamicClient(t) // nothing live: the object is genuinely gone
	c := newControllerWithListerAndDyn(
		base, id, newLister(t, configMapGVR, stale), dyn, newTestMapper(),
	)

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := base.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonNotFound {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonNotFound)
	}
}

// TestSync_VersionMismatchEscalatesWhenGVRResolutionFails proves that if
// observeLive can't even resolve a GVR for the fallback Get, that's reported
// as a KubeAPIError rather than silently discarded like the cache-only path
// used to be.
func TestSync_VersionMismatchEscalatesWhenGVRResolutionFails(t *testing.T) {
	const namespace = "default"
	ctx := context.Background()
	base := memory.New()
	id := readIdentity(namespace, "cm-unresolvable-fallback")
	seedReadDesire(t, base, id, "owner-1")

	stale := newUnstructuredConfigMap("cm-unresolvable-fallback", namespace, map[string]any{"k": staleDataValue})
	stale.SetAPIVersion("v2")

	c := newControllerWithListerAndDyn(
		base, id, newLister(t, configMapGVR, stale), newFakeDynamicClient(t), newNoMatchMapper(),
	)

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := base.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonKubeAPIError {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonKubeAPIError)
	}
}
