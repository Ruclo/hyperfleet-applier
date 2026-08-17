package desire_test

import (
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

const fieldManagementCluster = "managementCluster"

func TestApplyDesire_IdentityNestedUnderIdentityKey(t *testing.T) {
	id := validIdentity()
	d := desire.ApplyDesire{
		Identity: id,
		Owner:    "controller-1",
		Version:  1,
		Spec: desire.ApplySpec{
			KubeContent: json.RawMessage(`{"kind":"Deployment"}`),
		},
	}

	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var obj map[string]any
	if err := json.Unmarshal(b, &obj); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	identity, ok := obj["identity"].(map[string]any)
	if !ok {
		t.Fatalf("expected top-level %q object, got keys: %v", "identity", obj)
	}

	keys := []string{fieldManagementCluster, "type", "group", fieldResource, "namespace", "name"}
	for _, key := range keys {
		if _, ok := identity[key]; !ok {
			t.Errorf("expected identity key %q, got keys: %v", key, identity)
		}
	}
}

func TestIdentity_ResourceKey(t *testing.T) {
	id := validIdentity()
	rk := id.ResourceKey()

	if rk.ManagementCluster != id.ManagementCluster ||
		rk.Group != id.Group ||
		rk.Resource != id.Resource ||
		rk.Namespace != id.Namespace ||
		rk.Name != id.Name {
		t.Errorf("ResourceKey did not project all fields correctly: %+v vs %+v", id, rk)
	}
}

func TestResourceKey_String_DistinctWithEmbeddedSeparators(t *testing.T) {
	// Same slash layout, different field boundaries - must not collide.
	a := desire.ResourceKey{
		ManagementCluster: "a/b",
		Group:             "c",
		Resource:          testResourceDeployments,
		Namespace:         testNamespace,
		Name:              testName,
	}
	b := desire.ResourceKey{
		ManagementCluster: "a",
		Group:             "b/c",
		Resource:          testResourceDeployments,
		Namespace:         testNamespace,
		Name:              testName,
	}
	if a.String() == b.String() {
		t.Fatalf("distinct ResourceKeys collided as %q", a.String())
	}
	if !strings.Contains(a.String(), "%2F") {
		t.Fatalf("expected path-escaped slash in %q", a.String())
	}
	if !strings.Contains(b.String(), "%2F") {
		t.Fatalf("expected path-escaped slash in %q", b.String())
	}
}

func TestResourceKey_String_ValidLabelsUnchanged(t *testing.T) {
	rk := desire.ResourceKey{
		ManagementCluster: testManagementCluster,
		Group:             testGroup,
		Resource:          testResourceDeployments,
		Namespace:         testNamespace,
		Name:              testName,
	}
	want := "desire/" + strings.Join([]string{
		testManagementCluster, testGroup, testResourceDeployments, testNamespace, testName,
	}, "/")
	if got := rk.String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestReadDesire_StatusJSON(t *testing.T) {
	d := desire.ReadDesire{
		Identity: validIdentity(),
		Owner:    "controller-1",
		Version:  1,
		Status: desire.ReadStatus{
			Status: desire.Status{
				Conditions: []metav1.Condition{{
					Type:   desire.TypeAvailable,
					Status: metav1.ConditionTrue,
					Reason: desire.ReasonApplied,
				}},
			},
			KubeContent: json.RawMessage(`{"kind":"ConfigMap"}`),
		},
	}

	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var obj map[string]any
	if err := json.Unmarshal(b, &obj); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	status, ok := obj["status"].(map[string]any)
	if !ok {
		t.Fatalf("expected top-level status object, got keys: %v", obj)
	}
	if _, ok := status["conditions"]; !ok {
		t.Fatalf("expected status.conditions (flattened), got keys: %v", status)
	}
	if _, ok := status["status"]; ok {
		t.Fatalf("unexpected nested status.status; got keys: %v", status)
	}
	if _, ok := status["kubeContent"]; !ok {
		t.Fatalf("expected status.kubeContent, got keys: %v", status)
	}
}
