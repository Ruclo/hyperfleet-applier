package deletedesire

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/util"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// deleted returns a copy of status with the Successful condition set to
// True, reason Deleted: the resource is confirmed absent from the cluster.
func deleted(status desire.Status) desire.Status {
	return util.WithCondition(status, metav1.Condition{
		Type:   desire.TypeSuccessful,
		Status: metav1.ConditionTrue,
		Reason: desire.ReasonDeleted,
	})
}

// waitingForDeletion returns a copy of status with the Successful condition
// set to False, reason WaitingForDeletion: the delete was issued but the
// resource still exists (finalizers, graceful termination).
func waitingForDeletion(status desire.Status, msg string) desire.Status {
	return util.WithCondition(status, metav1.Condition{
		Type:    desire.TypeSuccessful,
		Status:  metav1.ConditionFalse,
		Reason:  desire.ReasonWaitingForDeletion,
		Message: msg,
	})
}

// deleteFailed returns a copy of status with the Successful condition set to
// False, reason KubeAPIError: a Kubernetes API call (GET or DELETE) failed.
func deleteFailed(status desire.Status, err error) desire.Status {
	return util.WithCondition(status, metav1.Condition{
		Type:    desire.TypeSuccessful,
		Status:  metav1.ConditionFalse,
		Reason:  desire.ReasonKubeAPIError,
		Message: err.Error(),
	})
}

// preCheckFailed returns a copy of status with the Successful condition set
// to False, reason PreCheckFailed: validation failed before any kube-apiserver
// call (e.g. invalid GVR mapping).
func preCheckFailed(status desire.Status, msg string) desire.Status {
	return util.WithCondition(status, metav1.Condition{
		Type:    desire.TypeSuccessful,
		Status:  metav1.ConditionFalse,
		Reason:  desire.ReasonPreCheckFailed,
		Message: msg,
	})
}
