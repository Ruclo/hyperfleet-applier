//go:build bench

package benchmark

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	discomemory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var configMapGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

// newEnvtestBackend starts a real kube-apiserver and etcd. The dynamic client
// and the discovery client share rec, so response times include discovery that
// happens during the pass. t.Cleanup stops the environment. KUBEBUILDER_ASSETS
// must already be set.
func newEnvtestBackend(t *testing.T, rec *httpLog) (dynamic.Interface, meta.RESTMapper) {
	t.Helper()
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest: start: %v", err)
	}
	t.Cleanup(func() {
		if stopErr := env.Stop(); stopErr != nil {
			t.Errorf("envtest: stop: %v", stopErr)
		}
	})
	cfg.QPS = clientQPS
	cfg.Burst = clientBurst
	cfg.WrapTransport = rec.wrap

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("envtest: dynamic client: %v", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatalf("envtest: discovery client: %v", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discomemory.NewMemCacheClient(discoveryClient))
	return dyn, mapper
}

func createConfigMap(ctx context.Context, dyn dynamic.Interface, name string) error {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      name,
			"namespace": defaultNamespace,
		},
		"data": map[string]string{"k": "v"},
	}}
	_, err := dyn.Resource(configMapGVR).Namespace(defaultNamespace).Create(ctx, obj, metav1.CreateOptions{})
	return err
}
