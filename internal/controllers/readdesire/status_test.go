package readdesire

import (
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

func TestSynced_SetsSyncedReasonAndKubeContent(t *testing.T) {
	content := []byte(`{"kind":"ConfigMap"}`)
	got := synced(desire.ReadStatus{}, content)

	cond := findCondition(got.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != desire.ReasonSynced {
		t.Errorf("condition = %+v, want Status=True Reason=%q", cond, desire.ReasonSynced)
	}
	if string(got.KubeContent) != string(content) {
		t.Errorf("KubeContent = %s, want %s", got.KubeContent, content)
	}
}

func TestNotFound_ClearsKubeContentAndSetsNotFoundReason(t *testing.T) {
	current := desire.ReadStatus{KubeContent: []byte(`{"stale":"content"}`)}
	got := notFound(current)

	cond := findCondition(got.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonNotFound {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonNotFound)
	}
	if got.KubeContent != nil {
		t.Errorf("KubeContent = %s, want nil: a not-found result must not carry over stale content", got.KubeContent)
	}
}

func TestKubeAPIError_PreservesPriorKubeContent(t *testing.T) {
	priorContent := []byte(`{"last":"known-good"}`)
	current := desire.ReadStatus{KubeContent: priorContent}
	readErr := errors.New("transient list error")

	got := kubeAPIError(current, readErr)

	cond := findCondition(got.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonKubeAPIError {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonKubeAPIError)
	}
	if cond != nil && cond.Message != readErr.Error() {
		t.Errorf("condition.Message = %q, want %q", cond.Message, readErr.Error())
	}
	if string(got.KubeContent) != string(priorContent) {
		t.Errorf("KubeContent = %s, want %s: a transient read failure must not erase the last known mirror",
			got.KubeContent, priorContent)
	}
}

// TestReadStatusEqual_DetectsKubeContentOnlyDifference proves readStatusEqual
// catches a KubeContent-only change even when conditions are identical -
// conditions.Equal alone would incorrectly treat this as unchanged.
func TestReadStatusEqual_DetectsKubeContentOnlyDifference(t *testing.T) {
	cond := metav1.Condition{Type: desire.TypeSuccessful, Status: metav1.ConditionTrue, Reason: desire.ReasonSynced}
	a := desire.ReadStatus{Status: desire.Status{Conditions: []metav1.Condition{cond}}, KubeContent: []byte(`{"v":1}`)}
	b := desire.ReadStatus{Status: desire.Status{Conditions: []metav1.Condition{cond}}, KubeContent: []byte(`{"v":2}`)}

	if readStatusEqual(a, b) {
		t.Error("readStatusEqual(a, b) = true, want false: KubeContent differs even though conditions match")
	}
	if !readStatusEqual(a, a) {
		t.Error("readStatusEqual(a, a) = false, want true: identical values must compare equal")
	}
}
