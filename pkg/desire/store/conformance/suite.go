// Package conformance is a reusable test suite that SpecStore / StatusStore
// backends must pass. Backends implement both interfaces on one concrete type;
// RunStatusStoreSuite seeds fixtures by asserting the store to SpecStore.
package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

const (
	resourceConfigMaps = "configmaps"
	ownerA             = "owner-a"
	kubeContentV1      = `{"v":1}`
	kubeContentV2      = `{"v":2}`
)

func identity(managementCluster string, typ desire.DesireType, name string) desire.Identity {
	return desire.Identity{
		ManagementCluster: managementCluster,
		Type:              typ,
		Group:             "apps",
		Resource:          resourceConfigMaps,
		Namespace:         "default",
		Name:              name,
	}
}

func newApplyDesire(id desire.Identity, owner string, content string) desire.ApplyDesire {
	return desire.ApplyDesire{
		Identity: id,
		Owner:    owner,
		Spec:     desire.ApplySpec{KubeContent: json.RawMessage(content)},
	}
}

func newDeleteDesire(id desire.Identity, owner string) desire.DeleteDesire {
	return desire.DeleteDesire{
		Identity: id,
		Owner:    owner,
	}
}

func newReadDesire(id desire.Identity, owner string) desire.ReadDesire {
	return desire.ReadDesire{
		Identity: id,
		Owner:    owner,
	}
}

func condition(condType string, reason string) metav1.Condition {
	return metav1.Condition{
		Type:               condType,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            reason,
		LastTransitionTime: metav1.Now(),
	}
}

func seedSpecStore(t *testing.T, s desire.StatusStore) desire.SpecStore {
	t.Helper()
	sp, ok := s.(desire.SpecStore)
	if !ok {
		t.Fatalf("store returned by newStore must also implement desire.SpecStore")
	}
	return sp
}

// RunSpecStoreSuite exercises SpecStore.
func RunSpecStoreSuite(t *testing.T, newStore func(t *testing.T) desire.SpecStore) {
	ctx := context.Background()

	t.Run("ApplyDesire", func(t *testing.T) {
		t.Run("CreateGetRoundTrip", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-1")
			d := newApplyDesire(id, ownerA, `{"kind":"ConfigMap"}`)

			created, err := store.CreateApplyDesire(ctx, d)
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if created.Version != 1 {
				t.Errorf("expected Version == 1 after Create, got %d", created.Version)
			}

			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if got.Version != created.Version {
				t.Errorf("expected Get to return same Version as Create, got %d vs %d", got.Version, created.Version)
			}
		})

		t.Run("CreateExistingReturnsAlreadyExists", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-2")
			d := newApplyDesire(id, ownerA, `{}`)

			if _, err := store.CreateApplyDesire(ctx, d); err != nil {
				t.Fatalf("first CreateApplyDesire: %v", err)
			}
			_, err := store.CreateApplyDesire(ctx, d)
			if !errors.Is(err, desire.ErrAlreadyExists) {
				t.Fatalf("expected ErrAlreadyExists on duplicate create, got %v", err)
			}
		})

		t.Run("CreateDuplicateDifferentOwnerReturnsOwnerConflict", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "owner-first")
			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			_, err := store.CreateApplyDesire(ctx, newApplyDesire(id, "owner-b", `{}`))
			if !errors.Is(err, desire.ErrOwnerConflict) {
				t.Fatalf("expected ErrOwnerConflict for different owner duplicate, got %v", err)
			}
		})

		t.Run("ReturnedSpecIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "clone-spec")
			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			created.Spec.KubeContent[2] = '9'
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if string(got.Spec.KubeContent) != kubeContentV1 {
				t.Fatalf("expected store to retain original KubeContent, got %q", got.Spec.KubeContent)
			}
		})

		t.Run("InputSpecIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "clone-input")
			d := newApplyDesire(id, ownerA, kubeContentV1)
			if _, err := store.CreateApplyDesire(ctx, d); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			d.Spec.KubeContent[2] = '9'
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if string(got.Spec.KubeContent) != kubeContentV1 {
				t.Fatalf("expected store to retain original KubeContent after input mutation, got %q", got.Spec.KubeContent)
			}
		})

		t.Run("UpdateSpecRejectsEmptyKubeContent", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "empty-update")
			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			_, err = store.UpdateApplyDesireSpec(ctx, id, desire.ApplySpec{}, ownerA, created.Version)
			if err == nil {
				t.Fatal("expected UpdateApplyDesireSpec to reject empty KubeContent")
			}
		})

		t.Run("RecreateAfterDeleteClearsStatus", func(t *testing.T) {
			store := newStore(t)
			statusStore, ok := store.(desire.StatusStore)
			if !ok {
				t.Fatalf("store must also implement desire.StatusStore")
			}
			idApply := identity("cluster-a", desire.TypeApply, "status-clear")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if _, createReadErr := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA)); createReadErr != nil {
				t.Fatalf("CreateReadDesire: %v", createReadErr)
			}
			withStatus, err := statusStore.UpdateApplyDesireStatus(
				ctx, idApply,
				desire.Status{Conditions: []metav1.Condition{condition(desire.TypeAvailable, desire.ReasonApplied)}},
				created.Version+1, // bumped by CreateReadDesire
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}
			if deleteErr := store.DeleteApplyDesire(ctx, idApply, ownerA, withStatus.Version); deleteErr != nil {
				t.Fatalf("DeleteApplyDesire: %v", deleteErr)
			}

			recreated, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{"v":2}`))
			if err != nil {
				t.Fatalf("recreate CreateApplyDesire: %v", err)
			}
			if len(recreated.Status.Conditions) != 0 {
				t.Fatalf("expected recreated Apply to have empty status, got %+v", recreated.Status.Conditions)
			}
		})

		t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "does-not-exist")

			_, err := store.GetApplyDesire(ctx, id)
			if !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateSpecSuccessIncrementsVersion", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-4")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{"v":1}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			updated, err := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}, ownerA, created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireSpec: %v", err)
			}
			if updated.Version <= created.Version {
				t.Errorf(
					"expected Version to strictly increase after successful update, got %d -> %d",
					created.Version, updated.Version,
				)
			}
		})

		t.Run("DifferentOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "owner-reject")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{"v":1}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			_, err = store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}, "owner-b", created.Version,
			)
			if !errors.Is(err, desire.ErrOwnerConflict) {
				t.Fatalf("expected ErrOwnerConflict for a different owner, got %v", err)
			}
		})

		t.Run("DeleteSuccess", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-6")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if err := store.DeleteApplyDesire(ctx, id, ownerA, created.Version); err != nil {
				t.Fatalf("DeleteApplyDesire: %v", err)
			}
			if _, err := store.GetApplyDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound after delete, got %v", err)
			}
		})
	})

	t.Run("DeleteDesire", func(t *testing.T) {
		t.Run("CreateGetRoundTrip", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "del-1")

			created, err := store.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}
			if created.Version != 1 {
				t.Errorf("expected Version == 1 after Create, got %d", created.Version)
			}

			got, err := store.GetDeleteDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
			if got.Identity != id || got.Owner != ownerA {
				t.Errorf("round-tripped desire mismatch: %+v", got)
			}
		})

		t.Run("DeleteSuccess", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "del-4")

			created, err := store.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}
			if err := store.DeleteDeleteDesire(ctx, id, ownerA, created.Version); err != nil {
				t.Fatalf("DeleteDeleteDesire: %v", err)
			}
			if _, err := store.GetDeleteDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound after delete, got %v", err)
			}
		})
	})

	t.Run("ReadDesire", func(t *testing.T) {
		t.Run("CreateGetRoundTrip", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "read-1")

			created, err := store.CreateReadDesire(ctx, newReadDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if created.Version != 1 {
				t.Errorf("expected Version == 1 after Create, got %d", created.Version)
			}

			got, err := store.GetReadDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if got.Identity != id || got.Owner != ownerA {
				t.Errorf("round-tripped desire mismatch: %+v", got)
			}
		})

		t.Run("DeleteSuccess", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "read-4")

			created, err := store.CreateReadDesire(ctx, newReadDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if err := store.DeleteReadDesire(ctx, id, ownerA, created.Version); err != nil {
				t.Fatalf("DeleteReadDesire: %v", err)
			}
			if _, err := store.GetReadDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound after delete, got %v", err)
			}
		})
	})

	t.Run("ManagementClusterListing", func(t *testing.T) {
		t.Run("Apply", func(t *testing.T) {
			store := newStore(t)
			idA := identity("cluster-a", desire.TypeApply, "res-a")
			idB := identity("cluster-b", desire.TypeApply, "res-b")

			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idA, "owner", `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire(a): %v", err)
			}
			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idB, "owner", `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire(b): %v", err)
			}

			listA, err := store.ListApplyDesires(ctx, "cluster-a")
			if err != nil {
				t.Fatalf("ListApplyDesires(cluster-a): %v", err)
			}
			if len(listA) != 1 || listA[0].Identity != idA {
				t.Fatalf("expected only cluster-a's desire, got %+v", listA)
			}

			listB, err := store.ListApplyDesires(ctx, "cluster-b")
			if err != nil {
				t.Fatalf("ListApplyDesires(cluster-b): %v", err)
			}
			if len(listB) != 1 || listB[0].Identity != idB {
				t.Fatalf("expected only cluster-b's desire, got %+v", listB)
			}
		})

		t.Run("Delete", func(t *testing.T) {
			store := newStore(t)
			idA := identity("cluster-a", desire.TypeDelete, "res-a")
			idB := identity("cluster-b", desire.TypeDelete, "res-b")

			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(idA, "owner")); err != nil {
				t.Fatalf("CreateDeleteDesire(a): %v", err)
			}
			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(idB, "owner")); err != nil {
				t.Fatalf("CreateDeleteDesire(b): %v", err)
			}

			listA, err := store.ListDeleteDesires(ctx, "cluster-a")
			if err != nil {
				t.Fatalf("ListDeleteDesires(cluster-a): %v", err)
			}
			if len(listA) != 1 || listA[0].Identity != idA {
				t.Fatalf("expected only cluster-a's desire, got %+v", listA)
			}

			listB, err := store.ListDeleteDesires(ctx, "cluster-b")
			if err != nil {
				t.Fatalf("ListDeleteDesires(cluster-b): %v", err)
			}
			if len(listB) != 1 || listB[0].Identity != idB {
				t.Fatalf("expected only cluster-b's desire, got %+v", listB)
			}
		})

		t.Run("Read", func(t *testing.T) {
			store := newStore(t)
			idA := identity("cluster-a", desire.TypeRead, "res-a")
			idB := identity("cluster-b", desire.TypeRead, "res-b")

			if _, err := store.CreateReadDesire(ctx, newReadDesire(idA, "owner")); err != nil {
				t.Fatalf("CreateReadDesire(a): %v", err)
			}
			if _, err := store.CreateReadDesire(ctx, newReadDesire(idB, "owner")); err != nil {
				t.Fatalf("CreateReadDesire(b): %v", err)
			}

			listA, err := store.ListReadDesires(ctx, "cluster-a")
			if err != nil {
				t.Fatalf("ListReadDesires(cluster-a): %v", err)
			}
			if len(listA) != 1 || listA[0].Identity != idA {
				t.Fatalf("expected only cluster-a's desire, got %+v", listA)
			}

			listB, err := store.ListReadDesires(ctx, "cluster-b")
			if err != nil {
				t.Fatalf("ListReadDesires(cluster-b): %v", err)
			}
			if len(listB) != 1 || listB[0].Identity != idB {
				t.Fatalf("expected only cluster-b's desire, got %+v", listB)
			}
		})
	})

	t.Run("MutualExclusion", func(t *testing.T) {
		t.Run("ApplyThenDeleteSupersedes", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "mutual-1")

			_, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			deleteID := identity(id.ManagementCluster, desire.TypeDelete, id.Name)
			_, err = store.CreateDeleteDesire(ctx, newDeleteDesire(deleteID, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire after Apply: %v", err)
			}

			_, err = store.GetApplyDesire(ctx, id)
			if !errors.Is(err, desire.ErrNotFound) {
				t.Errorf("expected Apply to be superseded by Delete, got err=%v", err)
			}

			_, err = store.GetDeleteDesire(ctx, deleteID)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
		})

		t.Run("DeleteThenApplyRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "mutual-2")

			_, err := store.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			applyID := identity(id.ManagementCluster, desire.TypeApply, id.Name)
			_, err = store.CreateApplyDesire(ctx, newApplyDesire(applyID, ownerA, `{}`))
			if !errors.Is(err, desire.ErrDeletePending) {
				t.Fatalf("expected ErrDeletePending when Apply follows Delete, got %v", err)
			}
		})

		t.Run("ReadCoexistsWithApply", func(t *testing.T) {
			store := newStore(t)
			idApply := identity("cluster-a", desire.TypeApply, "mutual-4")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			appliedD, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			readD, err := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire alongside Apply: %v", err)
			}
			if readD.Version <= appliedD.Version {
				t.Errorf(
					"expected CreateReadDesire to increment shared Version, got %d -> %d",
					appliedD.Version, readD.Version,
				)
			}

			got, err := store.GetApplyDesire(ctx, idApply)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if got.Version != readD.Version {
				t.Errorf("expected Apply to share bumped Version %d, got %d", readD.Version, got.Version)
			}

			gotRead, err := store.GetReadDesire(ctx, idRead)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if gotRead.Version != readD.Version {
				t.Errorf("expected Read to coexist, got Version %d vs %d", gotRead.Version, readD.Version)
			}
		})

		t.Run("ReadCoexistsWithDelete", func(t *testing.T) {
			store := newStore(t)
			idDelete := identity("cluster-a", desire.TypeDelete, "mutual-5")
			idRead := identity(idDelete.ManagementCluster, desire.TypeRead, idDelete.Name)

			deletedD, err := store.CreateDeleteDesire(ctx, newDeleteDesire(idDelete, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			readD, err := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire alongside Delete: %v", err)
			}
			if readD.Version <= deletedD.Version {
				t.Errorf(
					"expected CreateReadDesire to increment shared Version, got %d -> %d",
					deletedD.Version, readD.Version,
				)
			}

			got, err := store.GetDeleteDesire(ctx, idDelete)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
			if got.Version != readD.Version {
				t.Errorf("expected Delete to share bumped Version %d, got %d", readD.Version, got.Version)
			}

			gotRead, err := store.GetReadDesire(ctx, idRead)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if gotRead.Version != readD.Version {
				t.Errorf("expected Read to coexist, got Version %d vs %d", gotRead.Version, readD.Version)
			}
		})
	})

	t.Run("DeleteByPrefix", func(t *testing.T) {
		t.Run("RemovesMatchingApplyOnly", func(t *testing.T) {
			store := newStore(t)
			keep := identity("cluster-a", desire.TypeApply, "keep")
			drop := identity("cluster-a", desire.TypeApply, "drop")
			otherCluster := identity("cluster-b", desire.TypeApply, "drop")

			for _, id := range []desire.Identity{keep, drop, otherCluster} {
				if _, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`)); err != nil {
					t.Fatalf("CreateApplyDesire(%s): %v", id.Name, err)
				}
			}

			err := store.DeleteByPrefix(ctx, "cluster-a", desire.PrefixSelector{
				Type: desire.TypeApply,
				Name: "drop",
			})
			if err != nil {
				t.Fatalf("DeleteByPrefix: %v", err)
			}

			if _, err := store.GetApplyDesire(ctx, drop); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected drop to be deleted, got %v", err)
			}
			if _, err := store.GetApplyDesire(ctx, keep); err != nil {
				t.Fatalf("expected keep to remain, got %v", err)
			}
			if _, err := store.GetApplyDesire(ctx, otherCluster); err != nil {
				t.Fatalf("expected other cluster desire to remain, got %v", err)
			}
		})

		t.Run("ClearsOneSubStateLeavesOthers", func(t *testing.T) {
			store := newStore(t)
			idApply := identity("cluster-a", desire.TypeApply, "shared")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if _, err := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA)); err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}

			err := store.DeleteByPrefix(ctx, "cluster-a", desire.PrefixSelector{
				Type: desire.TypeApply,
				Name: "shared",
			})
			if err != nil {
				t.Fatalf("DeleteByPrefix: %v", err)
			}

			if _, err := store.GetApplyDesire(ctx, idApply); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected Apply cleared, got %v", err)
			}
			if _, err := store.GetReadDesire(ctx, idRead); err != nil {
				t.Fatalf("expected Read to remain, got %v", err)
			}
		})

		t.Run("MatchesResourcePrefix", func(t *testing.T) {
			store := newStore(t)
			cm := identity("cluster-a", desire.TypeDelete, "obj-1")
			secret := identity("cluster-a", desire.TypeDelete, "obj-2")
			secret.Resource = "secrets"

			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(cm, ownerA)); err != nil {
				t.Fatalf("CreateDeleteDesire(cm): %v", err)
			}
			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(secret, ownerA)); err != nil {
				t.Fatalf("CreateDeleteDesire(secret): %v", err)
			}

			err := store.DeleteByPrefix(ctx, "cluster-a", desire.PrefixSelector{
				Type:     desire.TypeDelete,
				Resource: "config",
			})
			if err != nil {
				t.Fatalf("DeleteByPrefix: %v", err)
			}

			if _, err := store.GetDeleteDesire(ctx, cm); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected configmaps delete desire removed, got %v", err)
			}
			if _, err := store.GetDeleteDesire(ctx, secret); err != nil {
				t.Fatalf("expected secrets delete desire to remain, got %v", err)
			}
		})

		t.Run("PartialClearBumpsSharedVersion", func(t *testing.T) {
			store := newStore(t)
			idApply := identity("cluster-a", desire.TypeApply, "ver-bump")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			read, createErr := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA))
			if createErr != nil {
				t.Fatalf("CreateReadDesire: %v", createErr)
			}
			staleVersion := read.Version

			if delErr := store.DeleteByPrefix(ctx, "cluster-a", desire.PrefixSelector{
				Type: desire.TypeRead,
				Name: idRead.Name,
			}); delErr != nil {
				t.Fatalf("DeleteByPrefix: %v", delErr)
			}

			got, getErr := store.GetApplyDesire(ctx, idApply)
			if getErr != nil {
				t.Fatalf("GetApplyDesire after prefix delete: %v", getErr)
			}
			if got.Version != staleVersion+1 {
				t.Fatalf("expected shared Version %d after clearing Read, got %d", staleVersion+1, got.Version)
			}

			_, updateErr := store.UpdateApplyDesireSpec(
				ctx, idApply, desire.ApplySpec{KubeContent: json.RawMessage(`{"v":2}`)}, ownerA, staleVersion,
			)
			if !errors.Is(updateErr, desire.ErrVersionConflict) {
				t.Fatalf("expected ErrVersionConflict for stale version %d, got %v", staleVersion, updateErr)
			}
		})
	})
}

// RunStatusStoreSuite exercises StatusStore. Fixtures are seeded via seedSpecStore.
func RunStatusStoreSuite(t *testing.T, newStore func(t *testing.T) desire.StatusStore) {
	ctx := context.Background()

	t.Run("ApplyDesireStatus", func(t *testing.T) {
		t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "missing")

			if _, err := store.GetApplyDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateStatusSuccessIncrementsVersion", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeApply, "cm-2")

			created, err := spec.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			readyCondition := condition("Ready", desire.ReasonApplied)
			updated, err := store.UpdateApplyDesireStatus(
				ctx, id, desire.Status{Conditions: []metav1.Condition{readyCondition}}, created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}
			if updated.Version <= created.Version {
				t.Errorf(
					"expected Version to strictly increase after successful status update, got %d -> %d",
					created.Version, updated.Version,
				)
			}
		})

		t.Run("SpecAndStatusWritesAreIsolated", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeApply, "cm-3")

			created, err := spec.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{"v":1}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			readyCondition := condition("Ready", desire.ReasonApplied)
			afterStatus, err := store.UpdateApplyDesireStatus(
				ctx, id, desire.Status{Conditions: []metav1.Condition{readyCondition}}, created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}
			if string(afterStatus.Spec.KubeContent) != `{"v":1}` {
				t.Errorf("UpdateApplyDesireStatus must not change Spec, got %q", afterStatus.Spec.KubeContent)
			}

			afterSpec, err := spec.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}, ownerA, afterStatus.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireSpec: %v", err)
			}
			if len(afterSpec.Status.Conditions) != 1 || afterSpec.Status.Conditions[0].Reason != desire.ReasonApplied {
				t.Errorf("UpdateApplyDesireSpec must not change Status, got %+v", afterSpec.Status.Conditions)
			}
		})
	})

	t.Run("DeleteDesireStatus", func(t *testing.T) {
		t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "missing")

			if _, err := store.GetDeleteDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateStatusSuccessIncrementsVersion", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeDelete, "del-2")

			created, err := spec.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			status := desire.Status{Conditions: []metav1.Condition{condition("Ready", desire.ReasonDeleteInProgress)}}
			updated, err := store.UpdateDeleteDesireStatus(ctx, id, status, created.Version)
			if err != nil {
				t.Fatalf("UpdateDeleteDesireStatus: %v", err)
			}
			if updated.Version <= created.Version {
				t.Errorf(
					"expected Version to strictly increase after successful status update, got %d -> %d",
					created.Version, updated.Version,
				)
			}

			got, err := store.GetDeleteDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
			if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Reason != desire.ReasonDeleteInProgress {
				t.Errorf("expected persisted delete status, got %+v", got.Status.Conditions)
			}
		})

		t.Run("StaleVersionRejected", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeDelete, "del-3")

			created, err := spec.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			status := desire.Status{Conditions: []metav1.Condition{condition("Ready", desire.ReasonDeleteInProgress)}}
			if _, updateErr := store.UpdateDeleteDesireStatus(ctx, id, status, created.Version); updateErr != nil {
				t.Fatalf("UpdateDeleteDesireStatus: %v", updateErr)
			}
			_, staleErr := store.UpdateDeleteDesireStatus(ctx, id, status, created.Version)
			if !errors.Is(staleErr, desire.ErrVersionConflict) {
				t.Fatalf("expected ErrVersionConflict on stale version, got %v", staleErr)
			}
		})
	})

	t.Run("ReadDesireStatus", func(t *testing.T) {
		t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "missing")

			if _, err := store.GetReadDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateStatusSuccessIncrementsVersion", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeRead, "read-2")

			created, err := spec.CreateReadDesire(ctx, newReadDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}

			status := desire.ReadStatus{
				Status: desire.Status{Conditions: []metav1.Condition{condition("Completed", "Read")}},
			}
			updated, err := store.UpdateReadDesireStatus(ctx, id, status, created.Version)
			if err != nil {
				t.Fatalf("UpdateReadDesireStatus: %v", err)
			}
			if updated.Version <= created.Version {
				t.Errorf(
					"expected Version to strictly increase after successful status update, got %d -> %d",
					created.Version, updated.Version,
				)
			}
		})

		t.Run("KubeContentMirror", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeRead, "read-3")

			created, err := spec.CreateReadDesire(ctx, newReadDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if created.Status.KubeContent != nil {
				t.Errorf("expected KubeContent to be nil/absent before it is ever set, got %q", created.Status.KubeContent)
			}

			content := json.RawMessage(`{"kind":"ConfigMap","data":{"k":"v"}}`)
			readStatus := desire.ReadStatus{
				Status:      desire.Status{Conditions: []metav1.Condition{condition("Completed", "Read")}},
				KubeContent: content,
			}
			updated, err := store.UpdateReadDesireStatus(ctx, id, readStatus, created.Version)
			if err != nil {
				t.Fatalf("UpdateReadDesireStatus: %v", err)
			}
			if string(updated.Status.KubeContent) != string(content) {
				t.Errorf("expected returned desire to reflect KubeContent, got %q", updated.Status.KubeContent)
			}

			got2, err := store.GetReadDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if string(got2.Status.KubeContent) != string(content) {
				t.Errorf(
					"expected GetReadDesire to reflect KubeContent set via UpdateReadDesireStatus, got %q",
					got2.Status.KubeContent,
				)
			}
		})
	})
}
