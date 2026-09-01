//go:build envtest

// Package integration is the single envtest suite for applydesire,
// deletedesire, and readdesire: one shared kube-apiserver (TestMain), one
// shared set of fixture/assertion helpers (helpers_test.go), and one file per
// controller plus a cross-controller lifecycle file. Every test here uses
// memory.New() as its desire store - only the kube-apiserver side is real.
// Pure Go control flow already covered by each controller's unit tests is
// deliberately not repeated here.
package integration

import (
	"fmt"
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	discomemory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	testManagementCluster = "test-cluster"
	testOwner             = "owner-1"
	defaultNamespace      = "default"
	testTargetVersion     = "v1"
	rbacGroup             = "rbac.authorization.k8s.io"
)

var (
	envTestEnvironment *envtest.Environment
	envRESTConfig      *rest.Config
	envK8sClient       client.Client
	envDynamicClient   dynamic.Interface
	envRESTMapper      *restmapper.DeferredDiscoveryRESTMapper

	configMapGVR   = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	clusterRoleGVR = schema.GroupVersionResource{Group: rbacGroup, Version: "v1", Resource: "clusterroles"}
	podGVR         = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
)

// TestMain boots one real envtest apiserver, shared across every test in this
// package.
func TestMain(m *testing.M) {
	envTestEnvironment = &envtest.Environment{}

	cfg, err := envTestEnvironment.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest: failed to start test environment: %v\n", err)
		os.Exit(1)
	}
	envRESTConfig = cfg

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		if stopErr := envTestEnvironment.Stop(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "envtest: failed to stop test environment: %v\n", stopErr)
		}
		fmt.Fprintf(os.Stderr, "envtest: failed to build controller-runtime client: %v\n", err)
		os.Exit(1)
	}
	envK8sClient = k8sClient

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		if stopErr := envTestEnvironment.Stop(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "envtest: failed to stop test environment: %v\n", stopErr)
		}
		fmt.Fprintf(os.Stderr, "envtest: failed to build dynamic client: %v\n", err)
		os.Exit(1)
	}
	envDynamicClient = dyn

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		if stopErr := envTestEnvironment.Stop(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "envtest: failed to stop test environment: %v\n", stopErr)
		}
		fmt.Fprintf(os.Stderr, "envtest: failed to build discovery client: %v\n", err)
		os.Exit(1)
	}
	envRESTMapper = restmapper.NewDeferredDiscoveryRESTMapper(discomemory.NewMemCacheClient(discoveryClient))

	code := m.Run()

	if err := envTestEnvironment.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "envtest: failed to stop test environment: %v\n", err)
		if code == 0 {
			// A teardown failure (leaked apiserver/etcd processes) must not be
			// masked by passing tests.
			code = 1
		}
	}
	os.Exit(code)
}
