package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/conformance"
)

const (
	testCluster   = "cluster-a"
	testGroup     = "apps"
	testResource  = "configmaps"
	testNamespace = "default"
	testOwner     = "owner-a"
)

// newMini creates a Store backed by miniredis. Caller must close client and mr.
func newMini() (*Store, *redis.Client, *miniredis.Miniredis, error) {
	mr, err := miniredis.Run()
	if err != nil {
		return nil, nil, nil, err
	}
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	return New(client), client, mr, nil
}

func testIdentity(typ desire.DesireType, name string) desire.Identity {
	return desire.Identity{
		ManagementCluster: testCluster,
		Type:              typ,
		Group:             testGroup,
		Resource:          testResource,
		Namespace:         testNamespace,
		Name:              name,
	}
}

func TestRedisStore_SpecStoreConformance(t *testing.T) {
	conformance.RunSpecStoreSuite(t, func(t *testing.T) desire.SpecStore {
		store, client, mr, err := newMini()
		if err != nil {
			t.Fatalf("NewMini: %v", err)
		}
		t.Cleanup(func() { _ = client.Close(); mr.Close() })
		return store
	})
}

func TestRedisStore_StatusStoreConformance(t *testing.T) {
	conformance.RunStatusStoreSuite(t, func(t *testing.T) desire.StatusStore {
		store, client, mr, err := newMini()
		if err != nil {
			t.Fatalf("NewMini: %v", err)
		}
		t.Cleanup(func() { _ = client.Close(); mr.Close() })
		return store
	})
}

func TestUpdateApplyDesireSpec_ConcurrentCAS(t *testing.T) {
	store, client, mr, err := newMini()
	if err != nil {
		t.Fatalf("newMini: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(); mr.Close() })

	ctx := context.Background()
	id := testIdentity(desire.TypeApply, "cas-race")
	created, err := store.CreateApplyDesire(ctx, desire.ApplyDesire{
		Identity: id,
		Owner:    testOwner,
		Spec:     desire.ApplySpec{KubeContent: json.RawMessage(`{"v":1}`)},
	})
	if err != nil {
		t.Fatalf("CreateApplyDesire: %v", err)
	}

	const goroutines = 8
	var (
		wg       sync.WaitGroup
		success  atomic.Int32
		conflict atomic.Int32
	)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			content := json.RawMessage(fmt.Sprintf(`{"v":%d}`, i))
			_, updateErr := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: content}, testOwner, created.Version,
			)
			switch {
			case updateErr == nil:
				success.Add(1)
			case errors.Is(updateErr, desire.ErrVersionConflict):
				conflict.Add(1)
			default:
				t.Errorf("unexpected error: %v", updateErr)
			}
		}(i)
	}
	wg.Wait()

	if success.Load() != 1 {
		t.Fatalf("expected exactly one successful CAS write, got %d", success.Load())
	}
	if conflict.Load() != goroutines-1 {
		t.Fatalf("expected %d version conflicts, got %d", goroutines-1, conflict.Load())
	}

	got, err := store.GetApplyDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetApplyDesire: %v", err)
	}
	if got.Version != created.Version+1 {
		t.Fatalf("expected version %d after one winner, got %d", created.Version+1, got.Version)
	}
}

// oddMGetClient uses a real client for SCAN but forces MGET to return a non-string value.
type oddMGetClient struct {
	*redis.Client
}

func (c *oddMGetClient) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	args := make([]interface{}, 0, 1+len(keys))
	args = append(args, "mget")
	for _, k := range keys {
		args = append(args, k)
	}
	cmd := redis.NewSliceCmd(ctx, args...)
	vals := make([]interface{}, len(keys))
	for i := range keys {
		vals[i] = 42 // non-string, non-nil
	}
	cmd.SetVal(vals)
	return cmd
}

func TestLoadClusterRecords_UnexpectedValueType(t *testing.T) {
	// Redis MGET returns nil for non-string keys, so exercise the defensive
	// type check with a Cmdable that yields a non-string, non-nil value.
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	badKey := desire.ResourceKey{
		ManagementCluster: testCluster,
		Group:             testGroup,
		Resource:          testResource,
		Namespace:         testNamespace,
		Name:              "bad-type",
	}.String()
	if setErr := mr.Set(badKey, "placeholder"); setErr != nil {
		t.Fatalf("seed key: %v", setErr)
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := New(&oddMGetClient{Client: client})

	_, listErr := store.ListApplyDesires(context.Background(), testCluster)
	if listErr == nil {
		t.Fatal("expected error for unexpected Redis value type")
	}
	if !strings.Contains(listErr.Error(), "unexpected value type") || !strings.Contains(listErr.Error(), badKey) {
		t.Fatalf("expected typed key error, got %v", listErr)
	}
}

func TestLoadClusterRecords_ScanPagesManyKeys(t *testing.T) {
	store, client, mr, err := newMini()
	if err != nil {
		t.Fatalf("newMini: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(); mr.Close() })

	ctx := context.Background()
	n := int(scanCount) + 20 // exceed one SCAN page so paging is exercised
	for i := 0; i < n; i++ {
		id := testIdentity(desire.TypeApply, fmt.Sprintf("cm-%03d", i))
		if _, createErr := store.CreateApplyDesire(ctx, desire.ApplyDesire{
			Identity: id,
			Owner:    testOwner,
			Spec:     desire.ApplySpec{KubeContent: json.RawMessage(`{}`)},
		}); createErr != nil {
			t.Fatalf("CreateApplyDesire(%s): %v", id.Name, createErr)
		}
	}

	list, listErr := store.ListApplyDesires(ctx, testCluster)
	if listErr != nil {
		t.Fatalf("ListApplyDesires: %v", listErr)
	}
	if len(list) != n {
		t.Fatalf("expected %d desires after paged SCAN, got %d", n, len(list))
	}
}

func TestCreateReadDesire_AttachesAndIncrementsSharedVersion(t *testing.T) {
	store, client, mr, err := newMini()
	if err != nil {
		t.Fatalf("newMini: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(); mr.Close() })

	ctx := context.Background()
	idApply := testIdentity(desire.TypeApply, "shared-read")
	idRead := idApply
	idRead.Type = desire.TypeRead

	applied, err := store.CreateApplyDesire(ctx, desire.ApplyDesire{
		Identity: idApply,
		Owner:    testOwner,
		Spec:     desire.ApplySpec{KubeContent: json.RawMessage(`{}`)},
	})
	if err != nil {
		t.Fatalf("CreateApplyDesire: %v", err)
	}

	read, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: idRead,
		Owner:    testOwner,
	})
	if err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}
	if read.Version != applied.Version+1 {
		t.Fatalf("expected CreateReadDesire to bump shared version to %d, got %d", applied.Version+1, read.Version)
	}
}

// txFailClient forces Watch to return TxFailedErr a fixed number of times,
// then delegates to the real client.
type txFailClient struct {
	*redis.Client
	remaining atomic.Int32
}

func (c *txFailClient) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	if c.remaining.Add(-1) >= 0 {
		return redis.TxFailedErr
	}
	return c.Client.Watch(ctx, fn, keys...)
}

func TestCASMutate_NoopReturnsNilRecord(t *testing.T) {
	store, client, mr, err := newMini()
	if err != nil {
		t.Fatalf("newMini: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(); mr.Close() })

	rec, casErr := store.casMutate(context.Background(), "cas-noop-key", func(*resourceRecord, bool) error {
		return errCASNoop
	})
	if casErr != nil {
		t.Fatalf("casMutate: expected nil error on noop, got %v", casErr)
	}
	if rec != nil {
		t.Fatalf("casMutate: expected nil record on noop, got %+v", rec)
	}
}

func TestProjectDesire_NilRecordIsSafe(t *testing.T) {
	var s Store
	if got := s.projectApplyDesire(nil); got.Version != 0 || got.Owner != "" || got.Spec.KubeContent != nil {
		t.Errorf("projectApplyDesire(nil) = %+v, want zero value", got)
	}
	if got := s.projectApplyDesire(&resourceRecord{}); got.Version != 0 || got.Spec.KubeContent != nil {
		t.Errorf("projectApplyDesire(nil Apply) = %+v, want zero value", got)
	}
	if got := s.projectDeleteDesire(nil); got.Version != 0 || got.Owner != "" {
		t.Errorf("projectDeleteDesire(nil) = %+v, want zero value", got)
	}
	if got := s.projectReadDesire(nil); got.Version != 0 || got.Owner != "" {
		t.Errorf("projectReadDesire(nil) = %+v, want zero value", got)
	}
}

func TestCASMutate_RetriesTxFailedErrThenSucceeds(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := &txFailClient{
		Client: rdb,
	}
	client.remaining.Store(2)
	store := New(client)
	ctx := context.Background()
	key := "cas-retry-key"

	rec, casErr := store.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if exists {
			t.Fatal("expected missing key on first successful Watch")
		}
		*rec = resourceRecord{
			Key:     desire.ResourceKey{ManagementCluster: testCluster, Name: "cas-retry"},
			Owner:   testOwner,
			Version: 1,
			Read:    true,
		}
		return nil
	})
	if casErr != nil {
		t.Fatalf("casMutate after TxFailedErr retries: %v", casErr)
	}
	if rec == nil || !rec.Read || rec.Version != 1 {
		t.Fatalf("unexpected record after retry success: %+v", rec)
	}
}

func TestCASMutate_TxFailedErrExhaustsRetries(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := &txFailClient{
		Client: rdb,
	}
	client.remaining.Store(int32(maxCASRetries + 1))
	store := New(client)

	_, casErr := store.casMutate(context.Background(), "cas-exhaust-key", func(*resourceRecord, bool) error {
		t.Fatal("mutate must not run when Watch always returns TxFailedErr")
		return nil
	})
	if !errors.Is(casErr, desire.ErrAborted) {
		t.Fatalf("expected ErrAborted after repeated TxFailedErr, got %v", casErr)
	}
}
