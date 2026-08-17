package memory

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

func TestCloneApplySpec_IsolatesAndPreservesNil(t *testing.T) {
	src := desire.ApplySpec{KubeContent: json.RawMessage(`{"v":1}`)}
	cloned := cloneApplySpec(src)
	cloned.KubeContent[2] = '9'
	if string(src.KubeContent) != `{"v":1}` {
		t.Fatalf("cloneApplySpec must not share KubeContent backing array")
	}
	if cloneApplySpec(desire.ApplySpec{}).KubeContent != nil {
		t.Fatalf("cloneApplySpec must preserve nil KubeContent")
	}
}

func TestCloneStatus_IsolatesAndPreservesNil(t *testing.T) {
	src := desire.Status{Conditions: []metav1.Condition{{Type: "Ready", Reason: "Applied"}}}
	cloned := cloneStatus(src)
	cloned.Conditions[0].Reason = "mutated"
	if src.Conditions[0].Reason != "Applied" {
		t.Fatalf("cloneStatus must not share Conditions backing array")
	}
	if cloneStatus(desire.Status{}).Conditions != nil {
		t.Fatalf("cloneStatus must preserve nil Conditions")
	}
}
