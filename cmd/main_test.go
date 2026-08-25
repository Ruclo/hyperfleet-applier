package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/readdesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/reconciler"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

type countingResettableMapper struct {
	meta.RESTMapper
	resets atomic.Int32
}

func (m *countingResettableMapper) Reset() {
	m.resets.Add(1)
}

func TestDiscoveryRefresher_RefreshesPeriodicallyAndStopsCleanly(t *testing.T) {
	mapper := &countingResettableMapper{}
	refresher := newDiscoveryRefresher(mapper, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- refresher.Start(ctx)
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for mapper.resets.Load() < 2 {
		select {
		case <-ticker.C:
		case <-deadline.C:
			cancel()
			t.Fatal("discovery mapper was not refreshed periodically")
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil after cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("discovery refresher did not stop after cancellation")
	}
}

type funcRunnable func(context.Context) error

func (f funcRunnable) Start(ctx context.Context) error { return f(ctx) }

func TestStartRunnablesCancelsSiblingsWhenRunnableReturns(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		testStartRunnablesCancelsSiblings(t, nil)
	})
	t.Run("error", func(t *testing.T) {
		testStartRunnablesCancelsSiblings(t, errors.New("controller failed"))
	})
}

func testStartRunnablesCancelsSiblings(t *testing.T, exitErr error) {
	t.Helper()
	siblingReleased := make(chan struct{})
	runnables := []reconciler.Runnable{
		funcRunnable(func(context.Context) error { return exitErr }),
		funcRunnable(func(ctx context.Context) error {
			<-ctx.Done()
			close(siblingReleased)
			return nil
		}),
	}
	done := make(chan error, 1)
	go func() {
		done <- startRunnables(context.Background(), runnables)
	}()
	select {
	case err := <-done:
		if exitErr == nil && err != nil {
			t.Fatalf("startRunnables() error = %v, want nil", err)
		}
		if exitErr != nil && !errors.Is(err, exitErr) {
			t.Fatalf("startRunnables() error = %v, want %v", err, exitErr)
		}
	case <-time.After(time.Second):
		t.Fatal("startRunnables did not return after a runnable exited")
	}
	select {
	case <-siblingReleased:
	default:
		t.Fatal("sibling runnable was not canceled")
	}
}

const (
	testManagementCluster = "test-cluster"
	testOwner             = "test-owner"
)

var testConfigMapGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

type concurrentStore struct {
	*memory.Store
	allStarted chan struct{}
	started    map[desire.DesireType]struct{}
	mu         sync.Mutex
}

func newConcurrentStore() *concurrentStore {
	return &concurrentStore{
		Store:      memory.New(),
		allStarted: make(chan struct{}),
		started:    make(map[desire.DesireType]struct{}),
	}
}

func (s *concurrentStore) markStarted(typ desire.DesireType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.started[typ]; exists {
		return
	}
	s.started[typ] = struct{}{}
	if len(s.started) == 3 {
		close(s.allStarted)
	}
}

func (s *concurrentStore) waitForPeers(ctx context.Context, typ desire.DesireType) error {
	s.markStarted(typ)
	select {
	case <-s.allStarted:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *concurrentStore) ListApplyDesires(
	ctx context.Context, managementCluster string,
) ([]desire.ApplyDesire, error) {
	if err := s.waitForPeers(ctx, desire.TypeApply); err != nil {
		return nil, err
	}
	return s.Store.ListApplyDesires(ctx, managementCluster)
}

func (s *concurrentStore) ListDeleteDesires(
	ctx context.Context, managementCluster string,
) ([]desire.DeleteDesire, error) {
	if err := s.waitForPeers(ctx, desire.TypeDelete); err != nil {
		return nil, err
	}
	return s.Store.ListDeleteDesires(ctx, managementCluster)
}

func (s *concurrentStore) ListReadDesires(
	ctx context.Context, managementCluster string,
) ([]desire.ReadDesire, error) {
	if err := s.waitForPeers(ctx, desire.TypeRead); err != nil {
		return nil, err
	}
	return s.Store.ListReadDesires(ctx, managementCluster)
}

func TestControllersReconcileSeededDesiresInParallel(t *testing.T) {
	store := newConcurrentStore()
	applyID := identity(desire.TypeApply, "apply-target")
	deleteID := identity(desire.TypeDelete, "delete-target")
	readID := identity(desire.TypeRead, "read-target")

	applyContent, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      applyID.Name,
			"namespace": applyID.Namespace,
		},
		"data": map[string]any{"key": "desired"},
	})
	if err != nil {
		t.Fatalf("marshal apply content: %v", err)
	}
	if _, err := store.CreateApplyDesire(t.Context(), desire.ApplyDesire{
		Identity: applyID, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: applyContent},
	}); err != nil {
		t.Fatalf("seed ApplyDesire: %v", err)
	}
	if _, err := store.CreateDeleteDesire(t.Context(), desire.DeleteDesire{
		Identity: deleteID, Owner: testOwner,
	}); err != nil {
		t.Fatalf("seed DeleteDesire: %v", err)
	}
	if _, err := store.CreateReadDesire(t.Context(), desire.ReadDesire{
		Identity: readID, Owner: testOwner, TargetVersion: "v1",
	}); err != nil {
		t.Fatalf("seed ReadDesire: %v", err)
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core types: %v", err)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme, nil,
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: applyID.Name, Namespace: applyID.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: deleteID.Name, Namespace: deleteID.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: readID.Name, Namespace: readID.Namespace}},
	)
	mapper := newTestMapper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		done <- startRunnables(ctx, []reconciler.Runnable{
			applydesire.New(store, store, dyn, mapper, testManagementCluster, time.Hour),
			deletedesire.New(store, store, dyn, mapper, testManagementCluster, time.Hour),
			readdesire.New(store, store, dyn, mapper, testManagementCluster, time.Hour),
		})
	}()

	select {
	case <-store.allStarted:
		// Each first pass reached its store list call while its peers were
		// blocked there, proving the three lifecycle loops run concurrently.
	case <-time.After(time.Second):
		cancel()
		t.Fatal("controllers did not enter their first reconciliation passes concurrently")
	}

	waitForReason(t, store, applyID, desire.ReasonApplied)
	waitForReason(t, store, deleteID, desire.ReasonDeleted)
	waitForReason(t, store, readID, desire.ReasonSynced)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("startRunnables() error = %v, want nil after caller cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("controllers did not stop cleanly after context cancellation")
	}
}

func identity(typ desire.DesireType, name string) desire.Identity {
	return desire.Identity{
		ManagementCluster: testManagementCluster,
		Type:              typ,
		Resource:          "configmaps",
		Namespace:         "default",
		Name:              name,
	}
}

func newTestMapper() meta.ResettableRESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
	mapper.AddSpecific(
		schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
		testConfigMapGVR,
		schema.GroupVersionResource{Version: "v1", Resource: "configmap"},
		meta.RESTScopeNamespace,
	)
	return &resettableMapper{RESTMapper: mapper}
}

type resettableMapper struct {
	meta.RESTMapper
}

func (*resettableMapper) Reset() {}

func waitForReason(t *testing.T, store *concurrentStore, id desire.Identity, want string) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var last desire.Status
	for {
		var status desire.Status
		var err error
		switch id.Type {
		case desire.TypeApply:
			var got desire.ApplyDesire
			got, err = store.GetApplyDesire(t.Context(), id)
			status = got.Status
		case desire.TypeDelete:
			var got desire.DeleteDesire
			got, err = store.GetDeleteDesire(t.Context(), id)
			status = got.Status
		case desire.TypeRead:
			var got desire.ReadDesire
			got, err = store.GetReadDesire(t.Context(), id)
			status = got.Status.Status
		default:
			t.Fatalf("unsupported desire type %q", id.Type)
		}
		if err != nil {
			t.Fatalf("get %s desire: %v", id.Type, err)
		}
		last = status
		for _, condition := range status.Conditions {
			if condition.Type == desire.TypeSuccessful && condition.Reason == want {
				return
			}
		}

		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("waiting for %s reason %s; last status: %+v", id.Type, want, last)
		}
	}
}
