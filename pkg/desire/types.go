package desire

import (
	"encoding/json"
	"net/url"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DesireType enumerates the kinds of desire a store can hold.
type DesireType string

const (
	TypeApply  DesireType = "apply"
	TypeDelete DesireType = "delete"
	TypeRead   DesireType = "read"
)

// Identity uniquely identifies a desire: management-cluster partition, desire
// type, and the Kubernetes resource coordinates it targets.
type Identity struct {
	// ManagementCluster is the management cluster the desire is scoped to.
	// Required.
	ManagementCluster string `json:"managementCluster"`
	// Type is the kind of desire (apply, delete, or read).
	Type DesireType `json:"type"`
	// Group is the API group of the resource. May be empty for core resources.
	Group string `json:"group"`
	// Resource is the resource type (e.g., "configmaps", "secrets").
	// Required.
	Resource string `json:"resource"`
	// Namespace is the target namespace.
	Namespace string `json:"namespace"`
	// Name is the target resource name.
	Name string `json:"name"`
}

// ResourceKey is a resource-level key without Type, representing a single
// physical resource record across all desire types.
type ResourceKey struct {
	ManagementCluster string
	Group             string
	Resource          string
	Namespace         string
	Name              string
}

// String encodes this ResourceKey into a flat string shared by backends that
// key storage on ResourceKey. Each field is path-escaped so embedded
// separators cannot collide distinct keys.
func (rk ResourceKey) String() string {
	return "desire/" + strings.Join([]string{
		url.PathEscape(rk.ManagementCluster),
		url.PathEscape(rk.Group),
		url.PathEscape(rk.Resource),
		url.PathEscape(rk.Namespace),
		url.PathEscape(rk.Name),
	}, "/")
}

// ManagementClusterKeyPrefix is the Redis/SCAN key prefix for a management cluster.
// PathEscape keeps glob metacharacters out of the SCAN MATCH pattern.
func ManagementClusterKeyPrefix(managementCluster string) string {
	return "desire/" + url.PathEscape(managementCluster) + "/"
}

// ResourceKey returns the resource-level key from this Identity, projecting
// away the Type field.
func (id Identity) ResourceKey() ResourceKey {
	return ResourceKey{
		ManagementCluster: id.ManagementCluster,
		Group:             id.Group,
		Resource:          id.Resource,
		Namespace:         id.Namespace,
		Name:              id.Name,
	}
}

// Identity reconstructs an Identity from this ResourceKey for the given
// desire type, the inverse of ResourceKey().
func (rk ResourceKey) Identity(t DesireType) Identity {
	return Identity{
		ManagementCluster: rk.ManagementCluster,
		Type:              t,
		Group:             rk.Group,
		Resource:          rk.Resource,
		Namespace:         rk.Namespace,
		Name:              rk.Name,
	}
}

// Conditions follow the Kubernetes metav1.Condition convention.
//
// Every desire carries a single summary condition, Successful, with positive
// polarity: Status=True means the desired state is currently achieved. The
// specific outcome is carried by the machine-readable Reason. This mirrors the
// rest of the HyperFleet status contract, which is uniformly one binary
// condition plus a reason (e.g. the API's per-adapter "<Adapter>Successful"
// condition and the aggregate "Reconciled"), and follows the Kubernetes API
// convention of a summarizing top-level condition for simple consumers.
//
// "In progress" is not a condition status: like the rest of the system, a
// not-yet-achieved desire is Successful=False with a non-error reason
// (WaitingForDeletion), distinguished from failure by the reason, not by a
// separate Progressing/Degraded condition.
//
// Condition semantics:
//
//	Successful=True  (desired state achieved)
//	  ReasonApplied - ApplyDesire reconciled to the cluster
//	  ReasonDeleted - DeleteDesire confirmed the object is gone past finalizers
//	  ReasonSynced  - ReadDesire mirrored live object state
//
//	Successful=False, in progress (not an error)
//	  ReasonWaitingForDeletion - delete issued; waiting on finalizers / graceful termination
//
//	Successful=False, error
//	  ReasonKubeAPIError   - the kube-apiserver call failed
//	  ReasonPreCheckFailed - the call could not be executed at all (e.g. unparseable manifest)
const (
	// TypeSuccessful is the single summary condition every desire carries.
	TypeSuccessful = "Successful"

	// Success reasons (Successful=True).
	ReasonApplied = "Applied"
	ReasonDeleted = "Deleted"
	ReasonSynced  = "Synced"

	// In-progress reason (Successful=False, not an error).
	ReasonWaitingForDeletion = "WaitingForDeletion"

	// Failure reasons (Successful=False).
	ReasonKubeAPIError   = "KubeAPIError"
	ReasonPreCheckFailed = "PreCheckFailed"
)

// Status is the uniform condition contract across all desire types.
type Status struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ApplyDesire is the intent to make a Kubernetes resource exist with specific
// content, via server-side apply.
type ApplyDesire struct {
	Identity Identity `json:"identity"`
	Owner    string   `json:"owner"`
	// ResourceID correlates this desire with the originating API resource
	// (provenance); it is content, not identity.
	ResourceID string    `json:"resourceId,omitempty"`
	Spec       ApplySpec `json:"spec"`
	Status     Status    `json:"status"`
	Version    int64     `json:"version"`
}

// ApplySpec describes the resource desired to exist.
type ApplySpec struct {
	// KubeContent is the raw resource manifest (JSON) to apply.
	KubeContent json.RawMessage `json:"kubeContent"`
}

// DeleteDesire is the intent to remove a Kubernetes resource.
type DeleteDesire struct {
	Identity Identity `json:"identity"`
	Owner    string   `json:"owner"`
	// ResourceID is provenance: the originating API resource. Content, not identity.
	ResourceID string `json:"resourceId,omitempty"`
	Status     Status `json:"status"`
	Version    int64  `json:"version"`
}

// ReadDesire is the intent to read a Kubernetes resource.
type ReadDesire struct {
	Identity Identity `json:"identity"`
	Owner    string   `json:"owner"`
	// ResourceID is provenance: the originating API resource. Content, not identity.
	ResourceID string     `json:"resourceId,omitempty"`
	Status     ReadStatus `json:"status"`
	Version    int64      `json:"version"`
}

// ReadStatus extends Status with KubeContent to mirror the current state
// of the Kubernetes resource.
type ReadStatus struct {
	Status
	KubeContent json.RawMessage `json:"kubeContent,omitempty"`
}
