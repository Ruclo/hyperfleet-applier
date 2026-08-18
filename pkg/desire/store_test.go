package desire_test

import (
	"testing"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

func TestPrefixSelector_Matches(t *testing.T) {
	id := desire.Identity{
		Type:      desire.TypeApply,
		Group:     "apps.example.com",
		Namespace: "default",
		Resource:  "configmaps",
		Name:      "cfg-1",
	}
	cases := []struct {
		name string
		sel  desire.PrefixSelector
		want bool
	}{
		{"group prefix match", desire.PrefixSelector{Group: "apps."}, true},
		{"group prefix mismatch", desire.PrefixSelector{Group: "batch."}, false},
		{"namespace exact match", desire.PrefixSelector{Namespace: "default"}, true},
		{"namespace exact mismatch", desire.PrefixSelector{Namespace: "other"}, false},
		{"resource prefix match", desire.PrefixSelector{Resource: "config"}, true},
		{"name exact mismatch", desire.PrefixSelector{Name: "other"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.sel.Matches(id); got != tc.want {
				t.Errorf("Matches() = %v, want %v", got, tc.want)
			}
		})
	}
}
