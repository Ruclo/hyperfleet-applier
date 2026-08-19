// Package conditions holds helpers for setting and comparing desire.Status
// conditions, shared across the reconcile controllers.
package conditions

import (
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// WithCondition returns a copy of status with cond set, leaving the input
// untouched. The Conditions slice is cloned so callers and stores never share
// backing storage.
func WithCondition(status desire.Status, cond metav1.Condition) desire.Status {
	out := desire.Status{Conditions: slices.Clone(status.Conditions)}
	meta.SetStatusCondition(&out.Conditions, cond)
	return out
}

// Equal reports whether a and b carry the same set of conditions, comparing by
// condition Type and ignoring LastTransitionTime.
//
// Kubernetes conditions are logically keyed by Type, not by slice position, so
// the comparison matches on Type rather than index. LastTransitionTime is
// ignored because meta.SetStatusCondition stamps it (when Status changes, or
// when a condition is first added), which is not a meaningful difference when
// deciding whether a status write would be a no-op.
func Equal(a, b desire.Status) bool {
	if len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Conditions {
		ac := a.Conditions[i]
		bc := meta.FindStatusCondition(b.Conditions, ac.Type)
		if bc == nil {
			return false
		}
		acCopy, bcCopy := ac, *bc
		acCopy.LastTransitionTime = metav1.Time{}
		bcCopy.LastTransitionTime = metav1.Time{}
		if acCopy != bcCopy {
			return false
		}
	}
	return true
}
