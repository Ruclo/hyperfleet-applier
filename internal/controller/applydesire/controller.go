// Package applydesire reconciles ApplyDesires against the local kube-apiserver.
//
// A Reconciler is bound to one management-cluster partition. Each ReconcileAll
// pass lists ApplyDesires from the spec store and reconciles each against the
// local apiserver via SSA (Force=true, single field manager), recording outcomes
// as status conditions.
//
// The reconciler reads ApplyDesire intent and writes reconciliation status.
// It does not mutate desire intent through the spec store. Authentication and
// storage-level authorization are outside the controller's responsibility.
package applydesire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	hflog "github.com/openshift-hyperfleet/hyperfleet-logger"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controller/conditions"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// fieldManager is the single global SSA field manager for the applier,
// per the single-writer ownership model.
const fieldManager = "hyperfleet-applier"

// defaultApplyTimeout bounds a single SSA Apply call so a hung apiserver
// cannot stall an entire ReconcileAll pass indefinitely.
const defaultApplyTimeout = 30 * time.Second

// specLister is the read side of the SpecStore the reconciler consumes.
// Declaring the narrow interface here (rather than taking the full
// desire.SpecStore) documents the real dependency and keeps test fakes small.
type specLister interface {
	ListApplyDesires(ctx context.Context, managementCluster string) ([]desire.ApplyDesire, error)
}

type statusWriter interface {
	UpdateApplyDesireStatus(
		ctx context.Context, id desire.Identity, status desire.Status, version int64,
	) (desire.ApplyDesire, error)
}

type Reconciler struct {
	spec              specLister
	status            statusWriter
	dyn               dynamic.Interface
	mapper            meta.RESTMapper
	managementCluster string
	applyTimeout      time.Duration
}

// NewReconciler builds a Reconciler bound to one partition (managementCluster).
// dyn is the dynamic client used for SSA; mapper resolves GroupVersionKind to
// GroupVersionResource and whether the kind is namespaced or cluster-scoped.
//
// Hosts should inject a restmapper.DeferredDiscoveryRESTMapper (or equivalent)
// so discovery cache refresh and CRD appearance are handled by the mapper
// itself. The reconciler does not call Reset() on mapping failures.
func NewReconciler(
	spec specLister,
	status statusWriter,
	dyn dynamic.Interface,
	mapper meta.RESTMapper,
	managementCluster string,
) *Reconciler {
	return &Reconciler{
		spec:              spec,
		status:            status,
		dyn:               dyn,
		mapper:            mapper,
		managementCluster: managementCluster,
		applyTimeout:      defaultApplyTimeout,
	}
}

// ReconcileAll lists every ApplyDesire in the partition and reconciles each.
// A failure on one desire is recorded on that desire's status and does not
// abort the others; every such failure is also joined into the returned error
// so the host can drive retry/backoff and surface controller health. The error
// is nil only when the list succeeds and no desire failed.
//
// Context cancellation is treated differently: it is caller-driven control flow
// (e.g. shutdown), not a resource failure, so it aborts the pass immediately
// and is returned without being recorded on any desire's status.
func (r *Reconciler) ReconcileAll(ctx context.Context) error {
	desires, err := r.spec.ListApplyDesires(ctx, r.managementCluster)
	if err != nil {
		return fmt.Errorf("apply: list apply desires for management cluster %q: %w", r.managementCluster, err)
	}

	var errs []error
	for _, d := range desires {
		// Stop promptly on cancellation instead of recording shutdown as a
		// per-desire failure across every remaining desire.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("apply: reconcile aborted for management cluster %q: %w", r.managementCluster, ctxErr)
		}
		if reconcileErr := r.reconcileOne(ctx, d); reconcileErr != nil {
			if errors.Is(reconcileErr, context.Canceled) || errors.Is(reconcileErr, context.DeadlineExceeded) {
				return reconcileErr
			}
			// WithResourceID carries a controller-local logical description of
			// the full desire Identity. It is not a storage key and not
			// desire.ResourceID (HyperFleet provenance). The identity slog
			// attribute (LogValuer) is the primary structured identity.
			logCtx := hflog.WithResourceType(ctx, "apply_desire")
			logCtx = hflog.WithResourceID(logCtx, describeIdentity(d.Identity))
			slog.ErrorContext(logCtx, "apply: reconcile failed",
				"identity", d.Identity,
				"error", reconcileErr,
			)
			errs = append(errs, reconcileErr)
		}
	}
	return errors.Join(errs...)
}

// reconcileOne parses d's KubeContent, resolves it to a GroupVersionResource,
// applies it to the cluster via server-side apply, and writes the resulting
// condition back to the status store. It never touches the SpecStore.
//
// ReasonApplied means the kube-apiserver accepted SSA, not that the live object
// was read back and verified against the manifest.
func (r *Reconciler) reconcileOne(ctx context.Context, d desire.ApplyDesire) error {
	newStatus, err := r.applyToCluster(ctx, d)
	if err != nil {
		// Only context cancellation reaches here. It is not a resource failure,
		// so propagate it without writing status: the caller aborts the pass and
		// no healthy status is overwritten with an error.
		return err
	}

	if conditions.Equal(newStatus, d.Status) {
		return nil
	}

	if _, err := r.status.UpdateApplyDesireStatus(ctx, d.Identity, newStatus, d.Version); err != nil {
		if errors.Is(err, desire.ErrVersionConflict) {
			// Spec or status changed after ListApplyDesires; the next poll will
			// retry with the fresh version. Treat as benign, not a reconcile failure.
			slog.DebugContext(ctx, "apply: status write lost a version race, will retry next pass",
				"identity", d.Identity,
			)
			return nil
		}
		return fmt.Errorf("apply: write status for %s: %w", describeIdentity(d.Identity), err)
	}
	return nil
}

// applyToCluster returns the status condition to persist and, separately, an
// error. The error is non-nil only for context cancellation, which the caller
// treats as an abort rather than a resource failure; in that case the returned
// status is unused. Every other outcome - manifest parse/mapping/target checks
// and genuine SSA failures - is a PreCheckFailed or KubeAPIError condition with
// a nil error, so reconcileOne can always persist it.
func (r *Reconciler) applyToCluster(ctx context.Context, d desire.ApplyDesire) (desire.Status, error) {
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(d.Spec.KubeContent, obj); err != nil {
		return preCheckFailed(d.Status, fmt.Sprintf(
			"apply: manifest could not be decoded as a Kubernetes object (invalid JSON or missing kind): %v", err,
		)), nil
	}
	if obj.GetAPIVersion() == "" || obj.GetKind() == "" || obj.GetName() == "" {
		return preCheckFailed(d.Status, "apply: manifest is missing apiVersion, kind, or metadata.name"), nil
	}

	gvk := obj.GroupVersionKind()
	mapping, err := r.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return preCheckFailed(d.Status, fmt.Sprintf("apply: no resource mapping for %s: %v", gvk, err)), nil
	}
	if err := checkApplyTarget(d.Identity, obj, mapping); err != nil {
		return preCheckFailed(d.Status, err.Error()), nil
	}

	ri := r.dyn.Resource(mapping.Resource)
	var resourceClient dynamic.ResourceInterface = ri
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		resourceClient = ri.Namespace(d.Identity.Namespace)
	}

	applyCtx, cancel := context.WithTimeout(ctx, r.applyTimeout)
	defer cancel()

	if _, err := resourceClient.Apply(applyCtx, d.Identity.Name, obj, metav1.ApplyOptions{
		FieldManager: fieldManager,
		Force:        true,
	}); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// The apply failed because the caller's context ended, not because
			// the resource is unhealthy. Abort instead of recording KubeAPIError.
			return desire.Status{}, fmt.Errorf("apply %s: %w", describeIdentity(d.Identity), ctxErr)
		}
		return applyFailed(d.Status, err), nil
	}
	return applied(d.Status), nil
}

// checkApplyTarget returns an error unless obj, after REST-mapping, targets
// the same Kubernetes object as id. The store validates identity and
// KubeContent independently, so a stored desire can disagree; applying
// without this check would mutate a different object than the one status is
// written against. An omitted manifest namespace is not a disagreement: the
// apply uses Identity.Namespace.
func checkApplyTarget(id desire.Identity, obj *unstructured.Unstructured, mapping *meta.RESTMapping) error {
	gvr := mapping.Resource
	if gvr.Group != id.Group || gvr.Resource != id.Resource {
		return fmt.Errorf(
			"apply: manifest resource %q/%q does not match identity %q/%q",
			gvr.Group, gvr.Resource, id.Group, id.Resource,
		)
	}
	if obj.GetName() != id.Name {
		return fmt.Errorf("apply: manifest name %q does not match identity name %q", obj.GetName(), id.Name)
	}
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		if id.Namespace == "" {
			return fmt.Errorf(
				"apply: identity namespace must not be empty for namespaced resource %q", id.Name,
			)
		}
		if ns := obj.GetNamespace(); ns != "" && ns != id.Namespace {
			return fmt.Errorf(
				"apply: manifest namespace %q does not match identity namespace %q", ns, id.Namespace,
			)
		}
	} else if id.Namespace != "" {
		// Cluster-scoped kinds are applied through the cluster-scoped client,
		// so Identity.Namespace never reaches the apiserver. But Namespace is
		// part of the desire's target identity, so two desires that differ only
		// in Namespace are distinct records that would apply the same physical
		// object and fight each other under Force=true. Require an empty
		// namespace so a cluster-scoped object maps to exactly one identity.
		return fmt.Errorf(
			"apply: identity namespace %q must be empty for cluster-scoped resource %q", id.Namespace, id.Name,
		)
	}
	return nil
}

// describeIdentity returns a presentation-only description of the desire's
// logical Identity for logs and errors. It is not a storage key, Redis key
// encoding, or reusable persistence format.
func describeIdentity(id desire.Identity) string {
	return fmt.Sprintf(
		"managementCluster=%q type=%q group=%q resource=%q namespace=%q name=%q",
		id.ManagementCluster, id.Type, id.Group, id.Resource, id.Namespace, id.Name,
	)
}
