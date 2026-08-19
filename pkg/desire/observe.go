package desire

import (
	"context"
	"log/slog"
	"sync/atomic"

	hflog "github.com/openshift-hyperfleet/hyperfleet-logger"
)

// ownerConflictTotal counts owner-conflict rejections in this process.
var ownerConflictTotal atomic.Uint64

// OwnerConflictTotal returns owner-conflict rejections since process start.
func OwnerConflictTotal() uint64 {
	return ownerConflictTotal.Load()
}

// ReportOwnerConflict increments the counter and logs a WARN.
func ReportOwnerConflict(ctx context.Context, id Identity, existingOwner, attemptedOwner string) {
	ownerConflictTotal.Add(1)
	ctx = hflog.WithResourceType(ctx, "desire")
	slog.WarnContext(ctx, "desire: owner conflict",
		"identity", id,
		"existing_owner", existingOwner,
		"attempted_owner", attemptedOwner,
	)
}

// CheckOwner reports and returns ErrOwnerConflict on owner mismatch.
func CheckOwner(ctx context.Context, id Identity, existingOwner, attemptedOwner string) error {
	if existingOwner == attemptedOwner {
		return nil
	}
	ReportOwnerConflict(ctx, id, existingOwner, attemptedOwner)
	return ErrOwnerConflict
}
