//go:build bench

package benchmark

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const configMapPathPrefix = "/api/v1/namespaces/" + defaultNamespace + "/configmaps/"

// fakeAPI is an HTTP stand-in for the kube-apiserver that answers ConfigMap
// GET, DELETE, and server-side-apply PATCH. When slow is set, every request
// sleeps for a seeded lognormal draw before the response.
type fakeAPI struct {
	mu      sync.Mutex
	objects map[string][]byte
	slow    *latencySleep
}

func newFakeAPI(slow *latencySleep) *fakeAPI {
	return &fakeAPI{objects: make(map[string][]byte), slow: slow}
}

func (f *fakeAPI) seed(name string, body []byte) {
	f.mu.Lock()
	f.objects[name] = append([]byte(nil), body...)
	f.mu.Unlock()
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.slow != nil {
		f.slow.sleep()
	}
	name, ok := configMapName(r.URL.Path)
	if !ok {
		writeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound, "no such path")
		return
	}
	switch r.Method {
	case http.MethodGet:
		f.get(w, name)
	case http.MethodDelete:
		f.del(w, name)
	case http.MethodPatch:
		f.patch(w, name)
	default:
		writeStatus(w, http.StatusMethodNotAllowed, metav1.StatusReasonMethodNotAllowed, "method not allowed")
	}
}

func (f *fakeAPI) get(w http.ResponseWriter, name string) {
	f.mu.Lock()
	body, ok := f.objects[name]
	f.mu.Unlock()
	if !ok {
		writeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound, "configmap not found")
		return
	}
	writeJSON(w, http.StatusOK, body)
}

func (f *fakeAPI) del(w http.ResponseWriter, name string) {
	f.mu.Lock()
	body, ok := f.objects[name]
	if ok {
		delete(f.objects, name)
	}
	f.mu.Unlock()
	if !ok {
		writeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound, "configmap not found")
		return
	}
	writeJSON(w, http.StatusOK, body)
}

func (f *fakeAPI) patch(w http.ResponseWriter, name string) {
	body, err := configMapBody(name)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError, err.Error())
		return
	}
	f.mu.Lock()
	f.objects[name] = body
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, body)
}

func configMapName(path string) (string, bool) {
	if !strings.HasPrefix(path, configMapPathPrefix) {
		return "", false
	}
	name := strings.TrimPrefix(path, configMapPathPrefix)
	if name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

func writeJSON(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func writeStatus(w http.ResponseWriter, code int, reason metav1.StatusReason, message string) {
	raw, err := json.Marshal(metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusFailure,
		Message:  message,
		Reason:   reason,
		Code:     int32(code),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, code, raw)
}

// configMapManifest is the desire payload. It omits server-owned metadata
// such as uid; envtest rejects an SSA create that sets one.
func configMapManifest(name string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      name,
			"namespace": defaultNamespace,
		},
		"data": map[string]string{"k": "v"},
	})
}

func configMapBody(name string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       defaultNamespace,
			"uid":             name,
			"resourceVersion": "1",
		},
		"data": map[string]string{"k": "v"},
	})
}

func configMapMapper() meta.RESTMapper {
	dm := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Group: "", Version: "v1"}})
	dm.AddSpecific(
		schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
		schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"},
		schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmap"},
		meta.RESTScopeNamespace,
	)
	return dm
}

// newMockBackend serves a mock apiserver. degraded selects the lognormal sleep.
// seedKey fixes the sleep sequence for a cell so a rerun matches.
func newMockBackend(t *testing.T, degraded bool, seedKey string, rec *httpLog) (dynamic.Interface, meta.RESTMapper, *fakeAPI) {
	t.Helper()
	var slow *latencySleep
	if degraded {
		slow = newLatencySleep(seedKey)
	}
	api := newFakeAPI(slow)
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)

	cfg := &rest.Config{
		Host:  srv.URL,
		QPS:   clientQPS,
		Burst: clientBurst,
	}
	cfg.WrapTransport = rec.wrap
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("mock dynamic client: %v", err)
	}
	return dyn, configMapMapper(), api
}
