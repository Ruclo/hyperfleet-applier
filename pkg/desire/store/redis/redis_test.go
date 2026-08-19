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

func newMiniStore(t *testing.T) *Store {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("redis client close: %v", err)
		}
		mr.Close()
	})
	return New(client)
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
		return newMiniStore(t)
	})
}

func TestRedisStore_StatusStoreConformance(t *testing.T) {
	conformance.RunStatusStoreSuite(t, func(t *testing.T) desire.StatusStore {
		return newMiniStore(t)
	})
}

func TestUpdateApplyDesireSpec_ConcurrentCAS(t *testing.T) {
	store := newMiniStore(t)

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
	for i := range goroutines {
		wg.Go(func() {
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
		})
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

func TestCreateDesire_ConcurrentCrossTypeOwnerMultiKeyCAS(t *testing.T) {
	// Race CreateApply (owner A) against CreateRead/CreateDelete (owner B) on
	// the same target. The multi-key WATCH must serialize sibling creates so
	// exactly one owner wins.
	cases := []struct {
		name string
		sib  desire.DesireType
	}{
		{name: "ApplyVsRead", sib: desire.TypeRead},
		{name: "ApplyVsDelete", sib: desire.TypeDelete},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMiniStore(t)
			ctx := context.Background()
			applyID := testIdentity(desire.TypeApply, "create-race-"+string(tc.sib))
			sibID := applyID
			sibID.Type = tc.sib

			const goroutines = 8
			var (
				wg       sync.WaitGroup
				success  atomic.Int32
				conflict atomic.Int32
			)
			for i := range goroutines {
				wg.Go(func() {
					var createErr error
					switch {
					case i%2 == 0:
						_, createErr = store.CreateApplyDesire(ctx, desire.ApplyDesire{
							Identity: applyID,
							Owner:    "owner-a",
							Spec:     desire.ApplySpec{KubeContent: json.RawMessage(`{}`)},
						})
					case tc.sib == desire.TypeRead:
						_, createErr = store.CreateReadDesire(ctx, desire.ReadDesire{
							Identity: sibID,
							Owner:    "owner-b",
						})
					default:
						_, createErr = store.CreateDeleteDesire(ctx, desire.DeleteDesire{
							Identity: sibID,
							Owner:    "owner-b",
						})
					}
					switch {
					case createErr == nil:
						success.Add(1)
					case errors.Is(createErr, desire.ErrOwnerConflict),
						errors.Is(createErr, desire.ErrAlreadyExists),
						errors.Is(createErr, desire.ErrDeletePending),
						errors.Is(createErr, desire.ErrAborted):
						conflict.Add(1)
					default:
						t.Errorf("unexpected error: %v", createErr)
					}
				})
			}
			wg.Wait()

			if success.Load() != 1 {
				t.Fatalf("expected exactly one successful create, got %d", success.Load())
			}
			if conflict.Load() != goroutines-1 {
				t.Fatalf("expected %d owner/type conflicts, got %d", goroutines-1, conflict.Load())
			}
		})
	}
}

// oddMGetClient uses a real SCAN client but forces MGET to return a non-string.
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
	// Exercise the defensive type check for a non-string MGET value.
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	badKey := redisKey(testIdentity(desire.TypeApply, "bad-type"))
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
	store := newMiniStore(t)

	ctx := context.Background()
	n := int(scanCount) + 20 // exceed one SCAN page so paging is exercised
	for i := range n {
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

func TestCreateReadDesire_IndependentOfExistingApply(t *testing.T) {
	store := newMiniStore(t)

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
	// Read has its own Version and does not change Apply's.
	if read.Version != 1 {
		t.Fatalf("expected new Read to start at Version 1, got %d", read.Version)
	}

	gotApply, err := store.GetApplyDesire(ctx, idApply)
	if err != nil {
		t.Fatalf("GetApplyDesire: %v", err)
	}
	if gotApply.Version != applied.Version {
		t.Fatalf("expected Apply version unchanged at %d, got %d", applied.Version, gotApply.Version)
	}
}

// txFailClient injects a fixed number of TxFailedErr results.
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
	store := newMiniStore(t)

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
			Identity: testIdentity(desire.TypeRead, "cas-retry"),
			Owner:    testOwner,
			Version:  1,
		}
		return nil
	})
	if casErr != nil {
		t.Fatalf("casMutate after TxFailedErr retries: %v", casErr)
	}
	if rec == nil || rec.Identity.Type != desire.TypeRead || rec.Version != 1 {
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

func TestLoadClusterRecords_GlobMetacharactersIsolated(t *testing.T) {
	// List/DeleteByPrefix take a raw managementCluster; SCAN must treat glob
	// metacharacters as literals so clusters cannot cross-match.
	store := newMiniStore(t)
	ctx := context.Background()

	seed := func(mc, name string) {
		t.Helper()
		id := desire.Identity{
			ManagementCluster: mc,
			Type:              desire.TypeApply,
			Group:             testGroup,
			Resource:          testResource,
			Namespace:         testNamespace,
			Name:              name,
		}
		rec := &resourceRecord{
			Identity: id,
			Owner:    testOwner,
			Version:  1,
			Apply:    &desire.ApplySpec{KubeContent: json.RawMessage(`{}`)},
		}
		b, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := store.client.Set(ctx, redisKey(id), b, 0).Err(); err != nil {
			t.Fatalf("seed %q: %v", mc, err)
		}
	}

	seed("mc-a", "plain")
	seed("mc-a*", "globish")
	seed("*", "star")

	assertList := func(mc string, wantNames ...string) {
		t.Helper()
		list, err := store.ListApplyDesires(ctx, mc)
		if err != nil {
			t.Fatalf("ListApplyDesires(%q): %v", mc, err)
		}
		got := make(map[string]struct{}, len(list))
		for _, d := range list {
			got[d.Identity.Name] = struct{}{}
		}
		if len(got) != len(wantNames) {
			t.Fatalf("ListApplyDesires(%q): got %d desires %v, want %v", mc, len(got), got, wantNames)
		}
		for _, name := range wantNames {
			if _, ok := got[name]; !ok {
				t.Fatalf("ListApplyDesires(%q): missing %q in %v", mc, name, got)
			}
		}
	}

	assertList("mc-a", "plain")
	assertList("mc-a*", "globish")
	assertList("*", "star")

	if err := store.DeleteByPrefix(ctx, "*", desire.PrefixSelector{Type: desire.TypeApply}); err != nil {
		t.Fatalf("DeleteByPrefix(*): %v", err)
	}
	assertList("*")
	assertList("mc-a", "plain")
	assertList("mc-a*", "globish")
}
