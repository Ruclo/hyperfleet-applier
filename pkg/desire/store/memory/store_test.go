package memory

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/conformance"
)

func TestMemoryStore_SpecStoreConformance(t *testing.T) {
	conformance.RunSpecStoreSuite(t, func(t *testing.T) desire.SpecStore {
		return New()
	})
}

func TestMemoryStore_StatusStoreConformance(t *testing.T) {
	conformance.RunStatusStoreSuite(t, func(t *testing.T) desire.StatusStore {
		return New()
	})
}

func TestProjectApplyDesire_NilSafe(t *testing.T) {
	s := New()
	if got := s.projectApplyDesire(nil); got.Version != 0 || got.Owner != "" || got.Spec.KubeContent != nil {
		t.Errorf("projectApplyDesire(nil) = %+v, want zero value", got)
	}
	if got := s.projectApplyDesire(&resourceRecord{}); got.Version != 0 || got.Spec.KubeContent != nil {
		t.Errorf("projectApplyDesire(nil Apply) = %+v, want zero value", got)
	}
}

func TestCreateApplyDesire_AttachesToExistingRead(t *testing.T) {
	store := New()
	ctx := context.Background()

	idRead := desire.Identity{
		ManagementCluster: "cluster-a",
		Type:              desire.TypeRead,
		Group:             "apps",
		Resource:          "configmaps",
		Namespace:         "default",
		Name:              "shared",
	}
	idApply := idRead
	idApply.Type = desire.TypeApply

	read, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: idRead,
		Owner:    "owner-a",
	})
	if err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	applied, err := store.CreateApplyDesire(ctx, desire.ApplyDesire{
		Identity: idApply,
		Owner:    "owner-a",
		Spec:     desire.ApplySpec{KubeContent: json.RawMessage(`{"v":1}`)},
	})
	if err != nil {
		t.Fatalf("CreateApplyDesire: %v", err)
	}
	if applied.Version != read.Version+1 {
		t.Fatalf("expected attached Apply to bump shared version to %d, got %d", read.Version+1, applied.Version)
	}

	got, err := store.GetApplyDesire(ctx, idApply)
	if err != nil {
		t.Fatalf("GetApplyDesire: %v", err)
	}
	if got.Version != applied.Version {
		t.Fatalf("expected Get to return version %d, got %d", applied.Version, got.Version)
	}
}
