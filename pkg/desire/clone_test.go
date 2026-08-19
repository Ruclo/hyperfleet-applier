package desire_test

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

func TestCloneApplySpec_IsolatesAndPreservesNil(t *testing.T) {
	src := desire.ApplySpec{KubeContent: json.RawMessage(`{"v":1}`)}
	cloned := desire.CloneApplySpec(src)
	cloned.KubeContent[2] = '9'
	if string(src.KubeContent) != `{"v":1}` {
		t.Fatalf("CloneApplySpec must not share KubeContent backing array")
	}
	if desire.CloneApplySpec(desire.ApplySpec{}).KubeContent != nil {
		t.Fatalf("CloneApplySpec must preserve nil KubeContent")
	}
}

func TestCloneStatus_IsolatesAndPreservesNil(t *testing.T) {
	src := desire.Status{Conditions: []metav1.Condition{{Type: desire.TypeSuccessful, Reason: desire.ReasonApplied}}}
	cloned := desire.CloneStatus(src)
	cloned.Conditions[0].Reason = "mutated"
	if src.Conditions[0].Reason != desire.ReasonApplied {
		t.Fatalf("CloneStatus must not share Conditions backing array")
	}
	if desire.CloneStatus(desire.Status{}).Conditions != nil {
		t.Fatalf("CloneStatus must preserve nil Conditions")
	}
}

func TestCloneReadStatus_IsolatesAndPreservesNil(t *testing.T) {
	src := desire.ReadStatus{
		Status: desire.Status{
			Conditions: []metav1.Condition{{
				Type: string(desire.TypeSuccessful), Reason: desire.ReasonSynced,
			}},
		},
		KubeContent: json.RawMessage(`{"v":1}`),
	}
	cloned := desire.CloneReadStatus(src)
	cloned.Conditions[0].Reason = "mutated"
	cloned.KubeContent[2] = '9'
	if src.Conditions[0].Reason != desire.ReasonSynced {
		t.Fatalf("CloneReadStatus must not share Conditions backing array")
	}
	if string(src.KubeContent) != `{"v":1}` {
		t.Fatalf("CloneReadStatus must not share KubeContent backing array")
	}

	empty := desire.CloneReadStatus(desire.ReadStatus{})
	if empty.Conditions != nil {
		t.Fatalf("CloneReadStatus must preserve nil Conditions")
	}
	if empty.KubeContent != nil {
		t.Fatalf("CloneReadStatus must preserve nil KubeContent")
	}
}
