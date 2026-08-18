package applydesire

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controller/conditions"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// applied returns a copy of status with the Successful condition set to
// True, reason Applied: the kube-apiserver accepted the server-side apply
// (not a post-apply drift check).
func applied(status desire.Status) desire.Status {
	return conditions.WithCondition(status, metav1.Condition{
		Type:   desire.TypeSuccessful,
		Status: metav1.ConditionTrue,
		Reason: desire.ReasonApplied,
	})
}

func applyFailed(status desire.Status, err error) desire.Status {
	return conditions.WithCondition(status, metav1.Condition{
		Type:    desire.TypeSuccessful,
		Status:  metav1.ConditionFalse,
		Reason:  desire.ReasonKubeAPIError,
		Message: err.Error(),
	})
}

// preCheckFailed returns a copy of status with the Successful condition set
// to False, reason PreCheckFailed: validation failed before any kube-apiserver
// call (e.g. bad manifest or manifest/identity target mismatch).
func preCheckFailed(status desire.Status, msg string) desire.Status {
	return conditions.WithCondition(status, metav1.Condition{
		Type:    desire.TypeSuccessful,
		Status:  metav1.ConditionFalse,
		Reason:  desire.ReasonPreCheckFailed,
		Message: msg,
	})
}
