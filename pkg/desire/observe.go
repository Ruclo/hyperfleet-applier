package desire

import (
	"context"
	"log/slog"
	"sync/atomic"

	hflog "github.com/openshift-hyperfleet/hyperfleet-logger"
)

// ownerConflictTotal is the store-side owner-conflict counter (single-writer
// reject path). It is a process-local stand-in; scrapeable Prometheus/OTel
// metrics belong in the hosting binary, often incremented from this value or
// from ErrOwnerConflict at the SpecStore call site.
var ownerConflictTotal atomic.Uint64

// OwnerConflictTotal returns owner-conflict rejections since process start.
func OwnerConflictTotal() uint64 {
	return ownerConflictTotal.Load()
}

// ReportOwnerConflict records a single-writer ownership collision: increments
// the owner-conflict counter and logs at WARN.
func ReportOwnerConflict(ctx context.Context, rk ResourceKey, existingOwner, attemptedOwner string) {
	ownerConflictTotal.Add(1)
	ctx = hflog.WithResourceType(ctx, "desire")
	ctx = hflog.WithResourceID(ctx, rk.String())
	slog.WarnContext(ctx, "desire: owner conflict",
		"management_cluster", rk.ManagementCluster,
		"group", rk.Group,
		"resource", rk.Resource,
		"namespace", rk.Namespace,
		"name", rk.Name,
		"existing_owner", existingOwner,
		"attempted_owner", attemptedOwner,
	)
}

// CheckOwner returns ErrOwnerConflict when existingOwner and attemptedOwner
// differ, after reporting the collision. Matching owners yield nil.
func CheckOwner(ctx context.Context, rk ResourceKey, existingOwner, attemptedOwner string) error {
	if existingOwner == attemptedOwner {
		return nil
	}
	ReportOwnerConflict(ctx, rk, existingOwner, attemptedOwner)
	return ErrOwnerConflict
}
