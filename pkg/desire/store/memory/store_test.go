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

func TestCreateApplyDesire_IndependentOfExistingRead(t *testing.T) {
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
	// Apply has its own Version and does not change Read's.
	if applied.Version != 1 {
		t.Fatalf("expected new Apply to start at Version 1, got %d", applied.Version)
	}

	got, err := store.GetApplyDesire(ctx, idApply)
	if err != nil {
		t.Fatalf("GetApplyDesire: %v", err)
	}
	if got.Version != applied.Version {
		t.Fatalf("expected Get to return version %d, got %d", applied.Version, got.Version)
	}

	gotRead, err := store.GetReadDesire(ctx, idRead)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	if gotRead.Version != read.Version {
		t.Fatalf("expected Read version unchanged at %d, got %d", read.Version, gotRead.Version)
	}
}
