package desire

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DesireType enumerates the kinds of desire a store can hold.
type DesireType string

const (
	TypeApply  DesireType = "apply"
	TypeDelete DesireType = "delete"
	TypeRead   DesireType = "read"
)

var allTypes = []DesireType{TypeApply, TypeDelete, TypeRead}

// AllTypes returns a copy of all desire types.
func AllTypes() []DesireType {
	return append([]DesireType{}, allTypes...)
}

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

// Conditions follow the Kubernetes metav1.Condition convention.
// Every desire carries one summary condition, Successful. Status=True means the
// desired state is achieved; Status=False uses Reason to distinguish
// in-progress work from failure.
//
//	Successful=True:  Applied, Deleted, Synced
//	Successful=False: WaitingForDeletion, KubeAPIError, PreCheckFailed
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
	// OriginID identifies the originating HyperFleet API resource.
	OriginID string    `json:"originId,omitempty"`
	Spec     ApplySpec `json:"spec"`
	Status   Status    `json:"status"`
	Version  int64     `json:"version"`
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
	// OriginID identifies the originating HyperFleet API resource.
	OriginID string `json:"originId,omitempty"`
	Status   Status `json:"status"`
	Version  int64  `json:"version"`
}

// ReadDesire is the intent to read a Kubernetes resource.
type ReadDesire struct {
	Identity Identity `json:"identity"`
	Owner    string   `json:"owner"`
	// OriginID identifies the originating HyperFleet API resource.
	OriginID string     `json:"originId,omitempty"`
	Status   ReadStatus `json:"status"`
	Version  int64      `json:"version"`
}

// ReadStatus extends Status with KubeContent to mirror the current state
// of the Kubernetes resource.
type ReadStatus struct {
	Status
	KubeContent json.RawMessage `json:"kubeContent,omitempty"`
}
