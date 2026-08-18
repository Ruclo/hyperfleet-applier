package conditions_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controller/conditions"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

func successful(reason string) metav1.Condition {
	return metav1.Condition{
		Type:   desire.TypeSuccessful,
		Status: metav1.ConditionTrue,
		Reason: reason,
	}
}

func TestWithCondition_DoesNotMutateInput(t *testing.T) {
	in := desire.Status{}

	out := conditions.WithCondition(in, successful(desire.ReasonApplied))

	if in.Conditions != nil {
		t.Errorf("WithCondition mutated the input status, got conditions %+v", in.Conditions)
	}
	if len(out.Conditions) != 1 || out.Conditions[0].Reason != desire.ReasonApplied {
		t.Errorf("WithCondition did not set the condition, got %+v", out.Conditions)
	}
}

func TestWithCondition_ReplacesSameType(t *testing.T) {
	first := conditions.WithCondition(desire.Status{}, successful(desire.ReasonApplied))

	out := conditions.WithCondition(first, metav1.Condition{
		Type:   desire.TypeSuccessful,
		Status: metav1.ConditionFalse,
		Reason: desire.ReasonKubeAPIError,
	})

	if len(out.Conditions) != 1 {
		t.Fatalf(
			"WithCondition over an existing type produced %d conditions, want 1: %+v",
			len(out.Conditions), out.Conditions,
		)
	}
	if out.Conditions[0].Reason != desire.ReasonKubeAPIError {
		t.Errorf("WithCondition did not replace the same-type condition, got %+v", out.Conditions[0])
	}
}

func TestEqual_IgnoresLastTransitionTime(t *testing.T) {
	a := conditions.WithCondition(desire.Status{}, successful(desire.ReasonApplied))

	b := desire.Status{Conditions: append([]metav1.Condition(nil), a.Conditions...)}
	for i := range b.Conditions {
		b.Conditions[i].LastTransitionTime = metav1.NewTime(time.Now().Add(time.Hour))
	}

	if !conditions.Equal(a, b) {
		t.Errorf("Equal(a, b) = false, want true when only LastTransitionTime differs\na=%+v\nb=%+v", a, b)
	}
}

func TestEqual_DetectsReasonChange(t *testing.T) {
	a := conditions.WithCondition(desire.Status{}, successful(desire.ReasonApplied))
	b := conditions.WithCondition(desire.Status{}, successful(desire.ReasonSynced))

	if conditions.Equal(a, b) {
		t.Errorf("Equal(a, b) = true, want false when Reason differs\na=%+v\nb=%+v", a, b)
	}
}

func TestEqual_DetectsLengthChange(t *testing.T) {
	a := desire.Status{}
	b := conditions.WithCondition(desire.Status{}, successful(desire.ReasonApplied))

	if conditions.Equal(a, b) {
		t.Errorf("Equal(a, b) = true, want false when Conditions length differs\na=%+v\nb=%+v", a, b)
	}
}
