package desire

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestIdentity_LogValue(t *testing.T) {
	id := Identity{
		ManagementCluster: "mc-1",
		Type:              TypeApply,
		Group:             "apps",
		Resource:          "deployments",
		Namespace:         "ns-1",
		Name:              "dep-1",
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.Info("test", "identity", id)

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log line: %v\nraw: %s", err, buf.String())
	}
	identity, ok := got["identity"].(map[string]any)
	if !ok {
		t.Fatalf("identity attribute = %#v, want nested object", got["identity"])
	}
	want := map[string]string{
		"management_cluster": "mc-1",
		"type":               "apply",
		"group":              "apps",
		"resource":           "deployments",
		"namespace":          "ns-1",
		"name":               "dep-1",
	}
	for k, v := range want {
		if gotVal, _ := identity[k].(string); gotVal != v {
			t.Errorf("identity.%s = %q, want %q", k, gotVal, v)
		}
	}
}
