//go:build envtest

package deletedesire

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	memstore "github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// TestEnvtest_DeletePod_Success tests successful deletion of a pod.
func TestEnvtest_DeletePod_Success(t *testing.T) {
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("failed to start test environment: %v", err)
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("failed to stop test environment: %v", err)
		}
	}()

	// Create clients
	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	dynClient, mapper, err := createClientsFromConfig(cfg)
	if err != nil {
		t.Fatalf("failed to create dynamic client: %v", err)
	}

	// Create namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-ns",
		},
	}
	if err := k8sClient.Create(context.Background(), ns); err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create a pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test-ns",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx:latest",
				},
			},
		},
	}
	if err := k8sClient.Create(context.Background(), pod); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	// Create store and reconciler
	store := memstore.New()
	specStore := store
	statusStore := store

	reconciler := New(
		specStore,
		statusStore,
		dynClient,
		mapper,
		"test-cluster",
		time.Hour,
	)

	// Create a DeleteDesire
	d := desire.DeleteDesire{
		Identity: desire.Identity{
			ManagementCluster: "test-cluster",
			Type:              desire.TypeDelete,
			Group:             "",
			Resource:          "pods",
			Namespace:         "test-ns",
			Name:              "test-pod",
		},
		Owner:   "test-owner",
		Version: 1,
	}

	// Create the desire in the store
	_, err = store.CreateDeleteDesire(context.Background(), d)
	if err != nil {
		t.Fatalf("failed to create desire in store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Reconcile
	err = reconciler.reconcileOne(ctx, d)
	if err != nil {
		t.Fatalf("reconcileOne failed: %v", err)
	}

	// Verify pod is deleted
	var deletedPod corev1.Pod
	err = k8sClient.Get(context.Background(), client.ObjectKey{
		Namespace: "test-ns",
		Name:      "test-pod",
	}, &deletedPod)
	if err == nil {
		t.Error("expected pod to be deleted, but it still exists")
	}
}

// TestEnvtest_DeletePod_WithFinalizers tests deletion when finalizers are present.
func TestEnvtest_DeletePod_WithFinalizers(t *testing.T) {
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("failed to start test environment: %v", err)
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("failed to stop test environment: %v", err)
		}
	}()

	// Create clients
	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	dynClient, mapper, err := createClientsFromConfig(cfg)
	if err != nil {
		t.Fatalf("failed to create dynamic client: %v", err)
	}

	// Create namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-ns-finalizers",
		},
	}
	if err := k8sClient.Create(context.Background(), ns); err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create a pod with a finalizer
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-pod-finalizer",
			Namespace:  "test-ns-finalizers",
			Finalizers: []string{"test.finalizer/block-deletion"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx:latest",
				},
			},
		},
	}
	if err := k8sClient.Create(context.Background(), pod); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	// Create store and reconciler
	store := memstore.New()
	reconciler := New(
		store,
		store,
		dynClient,
		mapper,
		"test-cluster",
		time.Hour,
	)

	// Create a DeleteDesire
	d := desire.DeleteDesire{
		Identity: desire.Identity{
			ManagementCluster: "test-cluster",
			Type:              desire.TypeDelete,
			Group:             "",
			Resource:          "pods",
			Namespace:         "test-ns-finalizers",
			Name:              "test-pod-finalizer",
		},
		Owner:   "test-owner",
		Version: 1,
	}

	// Create the desire in the store first
	_, err = store.CreateDeleteDesire(context.Background(), d)
	if err != nil {
		t.Fatalf("failed to create desire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Reconcile
	err = reconciler.reconcileOne(ctx, d)
	if err != nil {
		t.Fatalf("reconcileOne failed: %v", err)
	}

	// Fetch the updated desire from the store
	updated, err := store.GetDeleteDesire(context.Background(), d.Identity)
	if err != nil {
		t.Fatalf("failed to get updated desire: %v", err)
	}

	// Verify status is WaitingForDeletion
	if len(updated.Status.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(updated.Status.Conditions))
	}

	cond := updated.Status.Conditions[0]
	if cond.Type != desire.TypeSuccessful {
		t.Errorf("condition type = %q, want %q", cond.Type, desire.TypeSuccessful)
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("condition status = %q, want %q", cond.Status, metav1.ConditionFalse)
	}
	if cond.Reason != desire.ReasonWaitingForDeletion {
		t.Errorf("condition reason = %q, want %q", cond.Reason, desire.ReasonWaitingForDeletion)
	}

	// Verify message contains deletion timestamp and UID
	if cond.Message == "" {
		t.Error("expected message to contain deletion timestamp and UID")
	}

	// Verify pod still exists with deletion timestamp
	var stillExistingPod corev1.Pod
	err = k8sClient.Get(context.Background(), client.ObjectKey{
		Namespace: "test-ns-finalizers",
		Name:      "test-pod-finalizer",
	}, &stillExistingPod)
	if err != nil {
		t.Errorf("expected pod to still exist with deletion timestamp, but got error: %v", err)
	}
	if stillExistingPod.DeletionTimestamp == nil {
		t.Error("expected pod to have deletion timestamp set")
	}
}

// TestEnvtest_DeleteNonExistentPod tests deleting a pod that doesn't exist.
func TestEnvtest_DeleteNonExistentPod(t *testing.T) {
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("failed to start test environment: %v", err)
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("failed to stop test environment: %v", err)
		}
	}()

	dynClient, mapper, err := createClientsFromConfig(cfg)
	if err != nil {
		t.Fatalf("failed to create dynamic client: %v", err)
	}

	// Create store and reconciler
	store := memstore.New()
	reconciler := New(
		store,
		store,
		dynClient,
		mapper,
		"test-cluster",
		time.Hour,
	)

	// Create a DeleteDesire for a non-existent pod
	d := desire.DeleteDesire{
		Identity: desire.Identity{
			ManagementCluster: "test-cluster",
			Type:              desire.TypeDelete,
			Group:             "",
			Resource:          "pods",
			Namespace:         "default",
			Name:              "non-existent-pod",
		},
		Owner:   "test-owner",
		Version: 1,
	}

	// Create the desire in the store
	_, err = store.CreateDeleteDesire(context.Background(), d)
	if err != nil {
		t.Fatalf("failed to create desire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Reconcile
	err = reconciler.reconcileOne(ctx, d)
	if err != nil {
		t.Fatalf("reconcileOne failed: %v", err)
	}

	// Fetch the updated desire
	updated, err := store.GetDeleteDesire(context.Background(), d.Identity)
	if err != nil {
		t.Fatalf("failed to get updated desire: %v", err)
	}

	// Verify status is Deleted (already gone)
	if len(updated.Status.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(updated.Status.Conditions))
	}

	cond := updated.Status.Conditions[0]
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("condition status = %q, want %q", cond.Status, metav1.ConditionTrue)
	}
	if cond.Reason != desire.ReasonDeleted {
		t.Errorf("condition reason = %q, want %q", cond.Reason, desire.ReasonDeleted)
	}
}

// Helper to create dynamic client and mapper from rest config
func createClientsFromConfig(cfg *rest.Config) (dynamic.Interface, *restmapper.DeferredDiscoveryRESTMapper, error) {
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))
	return dynClient, mapper, nil
}
