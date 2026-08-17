package desire

import (
	"context"
	"errors"
	"strings"
)

// Sentinel errors returned by SpecStore and StatusStore operations.
var (
	// ErrNotFound is returned when a desire doesn't exist.
	ErrNotFound = errors.New("desire: not found")

	// ErrAlreadyExists is returned when attempting to create a desire that already exists.
	ErrAlreadyExists = errors.New("desire: already exists")

	// ErrVersionConflict is returned when a write or delete includes a stale version.
	ErrVersionConflict = errors.New("desire: version conflict")

	// ErrAborted is returned when a concurrent mutation prevented the write; refresh and retry.
	ErrAborted = errors.New("desire: aborted")

	// ErrOwnerConflict is returned when a write or delete is attempted by a different owner.
	ErrOwnerConflict = errors.New("desire: owner conflict")

	// ErrDeletePending is returned when attempting to create an ApplyDesire
	// while a DeleteDesire is active for the same resource.
	ErrDeletePending = errors.New("desire: apply rejected, delete is pending for this resource")
)

// PrefixSelector specifies which desires to match in a prefix-based delete operation.
// Type, Namespace, and Name are matched exactly; Group and Resource are matched
// by prefix. Empty fields match any value.
type PrefixSelector struct {
	// Type is the desire type to delete (TypeApply, TypeDelete, or TypeRead).
	// If empty (unset), all types are matched.
	Type DesireType
	// Group is matched as a prefix of the API group; empty matches any group.
	Group string
	// Resource is matched as a prefix of the resource type (e.g., "configmaps"); empty matches any resource.
	Resource string
	// Namespace is matched exactly; empty matches any namespace.
	Namespace string
	// Name is matched exactly; empty matches any name.
	Name string
}

// Matches reports whether id matches this selector.
func (sel PrefixSelector) Matches(id Identity) bool {
	if sel.Type != "" && id.Type != sel.Type {
		return false
	}
	if sel.Group != "" && !strings.HasPrefix(id.Group, sel.Group) {
		return false
	}
	if sel.Resource != "" && !strings.HasPrefix(id.Resource, sel.Resource) {
		return false
	}
	if sel.Namespace != "" && id.Namespace != sel.Namespace {
		return false
	}
	if sel.Name != "" && id.Name != sel.Name {
		return false
	}
	return true
}

// SpecStore manages the specification side of desires (creates, updates, deletes, reads).
// Each Create/Update method atomically increments the resource-level version counter.
type SpecStore interface {
	// CreateApplyDesire atomically creates a new ApplyDesire for a resource.
	// When a resource record already exists, ownership is checked first:
	// Returns ErrOwnerConflict if the resource has a different owner.
	// Returns ErrDeletePending if a DeleteDesire is currently active for that resource.
	// Returns ErrAlreadyExists if an ApplyDesire already exists for that resource.
	CreateApplyDesire(ctx context.Context, d ApplyDesire) (ApplyDesire, error)

	// GetApplyDesire retrieves a previously created ApplyDesire.
	// Returns ErrNotFound if the desire doesn't exist.
	GetApplyDesire(ctx context.Context, id Identity) (ApplyDesire, error)

	// UpdateApplyDesireSpec updates the Spec and Owner of an ApplyDesire.
	// Requires exact Version match; returns ErrVersionConflict if stale.
	// Returns ErrOwnerConflict if called by a different owner.
	UpdateApplyDesireSpec(
		ctx context.Context, id Identity, spec ApplySpec, owner string, version int64,
	) (ApplyDesire, error)

	// DeleteApplyDesire removes an ApplyDesire.
	// Requires exact Version match and owner.
	DeleteApplyDesire(ctx context.Context, id Identity, owner string, version int64) error

	// CreateDeleteDesire atomically creates a new DeleteDesire for a resource.
	// If an ApplyDesire exists, it is superseded (cleared).
	// When a resource record already exists, ownership is checked first:
	// Returns ErrOwnerConflict if the resource has a different owner.
	// Returns ErrAlreadyExists if a DeleteDesire already exists for that resource.
	CreateDeleteDesire(ctx context.Context, d DeleteDesire) (DeleteDesire, error)

	// GetDeleteDesire retrieves a previously created DeleteDesire.
	// Returns ErrNotFound if the desire doesn't exist.
	GetDeleteDesire(ctx context.Context, id Identity) (DeleteDesire, error)

	// DeleteDeleteDesire removes a DeleteDesire.
	// Requires exact Version match and owner.
	DeleteDeleteDesire(ctx context.Context, id Identity, owner string, version int64) error

	// CreateReadDesire atomically creates a new ReadDesire for a resource.
	// When a resource record already exists, ownership is checked first:
	// Returns ErrOwnerConflict if the resource has a different owner.
	// Returns ErrAlreadyExists if a ReadDesire already exists for that resource.
	CreateReadDesire(ctx context.Context, d ReadDesire) (ReadDesire, error)

	// GetReadDesire retrieves a previously created ReadDesire.
	// Returns ErrNotFound if the desire doesn't exist.
	GetReadDesire(ctx context.Context, id Identity) (ReadDesire, error)

	// DeleteReadDesire removes a ReadDesire.
	// Requires exact Version match and owner.
	DeleteReadDesire(ctx context.Context, id Identity, owner string, version int64) error

	// ListApplyDesires returns all ApplyDesires for a given management cluster.
	ListApplyDesires(ctx context.Context, managementCluster string) ([]ApplyDesire, error)

	// ListDeleteDesires returns all DeleteDesires for a given management cluster.
	ListDeleteDesires(ctx context.Context, managementCluster string) ([]DeleteDesire, error)

	// ListReadDesires returns all ReadDesires for a given management cluster.
	ListReadDesires(ctx context.Context, managementCluster string) ([]ReadDesire, error)

	// DeleteByPrefix removes desires matching a PrefixSelector within a management cluster.
	DeleteByPrefix(ctx context.Context, managementCluster string, sel PrefixSelector) error
}

// StatusStore manages the status side of desires (status-only updates and reads).
// Updates do not check ownership; they only require Version match.
type StatusStore interface {
	// GetApplyDesire retrieves an ApplyDesire including its status.
	// Returns ErrNotFound if the desire doesn't exist.
	GetApplyDesire(ctx context.Context, id Identity) (ApplyDesire, error)

	// UpdateApplyDesireStatus updates only the Status field of an ApplyDesire.
	// Requires exact Version match; returns ErrVersionConflict if stale.
	// Does not check ownership.
	UpdateApplyDesireStatus(ctx context.Context, id Identity, status Status, version int64) (ApplyDesire, error)

	// GetDeleteDesire retrieves a DeleteDesire including its status.
	// Returns ErrNotFound if the desire doesn't exist.
	GetDeleteDesire(ctx context.Context, id Identity) (DeleteDesire, error)

	// UpdateDeleteDesireStatus updates only the Status field of a DeleteDesire.
	// Requires exact Version match; returns ErrVersionConflict if stale.
	// Does not check ownership.
	UpdateDeleteDesireStatus(ctx context.Context, id Identity, status Status, version int64) (DeleteDesire, error)

	// GetReadDesire retrieves a ReadDesire including its status.
	// Returns ErrNotFound if the desire doesn't exist.
	GetReadDesire(ctx context.Context, id Identity) (ReadDesire, error)

	// UpdateReadDesireStatus updates the Status field of a ReadDesire,
	// optionally including KubeContent.
	// Requires exact Version match; returns ErrVersionConflict if stale.
	// Does not check ownership.
	UpdateReadDesireStatus(ctx context.Context, id Identity, status ReadStatus, version int64) (ReadDesire, error)
}
