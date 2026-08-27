// Package reconciler defines the lifecycle shared by the applier's
// long-running reconcilers and controllers.
package reconciler

import "context"

// Runnable owns a reconciliation loop that runs until ctx is canceled.
// A caller-driven stop is successful and must return nil.
type Runnable interface {
	Start(context.Context) error
}
