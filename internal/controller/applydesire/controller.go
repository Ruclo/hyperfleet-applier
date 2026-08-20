// Package applydesire reconciles ApplyDesires against the local kube-apiserver.
// A Reconciler is bound to one management cluster and applies listed desires
// via SSA, recording the result as status.
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

// specLister is the read-only SpecStore surface the reconciler needs.
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

// NewReconciler builds a Reconciler for one management cluster.
// mapper resolves GVKs for SSA; discovery cache ownership stays with the host.
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
// Ordinary apply failures are recorded in status and excluded from the returned
// error; only non-conflict status-write failures are joined. Context
// cancellation aborts immediately and is not written as status.
func (r *Reconciler) ReconcileAll(ctx context.Context) error {
	desires, err := r.spec.ListApplyDesires(ctx, r.managementCluster)
	if err != nil {
		return fmt.Errorf("apply: list apply desires for management cluster %q: %w", r.managementCluster, err)
	}

	var errs []error
	for _, d := range desires {
		// Do not record shutdown as a per-desire failure.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("apply: reconcile aborted for management cluster %q: %w", r.managementCluster, ctxErr)
		}
		if reconcileErr := r.reconcileOne(ctx, d); reconcileErr != nil {
			if errors.Is(reconcileErr, context.Canceled) || errors.Is(reconcileErr, context.DeadlineExceeded) {
				return fmt.Errorf(
					"apply: reconcile desire %s: %w",
					describeIdentity(d.Identity), reconcileErr,
				)
			}
			// WithResourceID carries a display-only identity string.
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

// reconcileOne parses d, resolves its target, applies it via SSA, and writes
// the resulting status. It never mutates the SpecStore.
func (r *Reconciler) reconcileOne(ctx context.Context, d desire.ApplyDesire) error {
	newStatus, err := r.applyToCluster(ctx, d)
	if err != nil {
		// Cancellation aborts the pass instead of writing status.
		return err
	}

	if conditions.Equal(newStatus, d.Status) {
		return nil
	}

	if _, err := r.status.UpdateApplyDesireStatus(ctx, d.Identity, newStatus, d.Version); err != nil {
		if errors.Is(err, desire.ErrVersionConflict) {
			// Benign race; the next poll retries with the fresh version.
			slog.DebugContext(ctx, "apply: status write lost a version race, will retry next pass",
				"identity", d.Identity,
			)
			return nil
		}
		return fmt.Errorf("apply: write status for %s: %w", describeIdentity(d.Identity), err)
	}
	return nil
}

// applyToCluster returns the status to persist and, separately, an error.
// Only context cancellation returns a non-nil error; all other outcomes are
// encoded as status conditions.
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
			// Abort instead of recording shutdown as KubeAPIError.
			return desire.Status{}, fmt.Errorf("apply %s: %w", describeIdentity(d.Identity), ctxErr)
		}
		return applyFailed(d.Status, err), nil
	}
	return applied(d.Status), nil
}

// checkApplyTarget rejects manifests that target a different object than id.
// An omitted manifest namespace is allowed; apply uses Identity.Namespace.
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
		// Require one identity per cluster-scoped object.
		return fmt.Errorf(
			"apply: identity namespace %q must be empty for cluster-scoped resource %q", id.Namespace, id.Name,
		)
	}
	return nil
}

// describeIdentity formats an Identity for logs and errors.
func describeIdentity(id desire.Identity) string {
	return fmt.Sprintf(
		"managementCluster=%q type=%q group=%q resource=%q namespace=%q name=%q",
		id.ManagementCluster, id.Type, id.Group, id.Resource, id.Namespace, id.Name,
	)
}
