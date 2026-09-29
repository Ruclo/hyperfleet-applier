//go:build bench

package benchmark

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

const (
	defaultNamespace  = "default"
	managementCluster = "bench-cluster"
	benchOwner        = "bench-owner"
	clientQPS         = 20
	clientBurst       = 40

	// csvNote is the first line of a new results file. Production leaves client
	// QPS unset, so client-go uses 5; these rows are the latency-bound comparison.
	csvNote = "# Production applier QPS is 5; a cycle at that limit is about http_count/5 seconds, independent of these latency-bound timings."
)

var csvHeader = []string{
	"backend",
	"mode",
	"n",
	"wall_seconds",
	"apply_pass_seconds",
	"delete_pass_seconds",
	"delete_finished_seconds",
	"http_p50_ms",
	"http_p99_ms",
	"http_count",
}

var (
	benchBackend string
	benchMode    string
	benchN       int
	benchCSV     string
)

func init() {
	flag.StringVar(&benchBackend, "backend", "", "envtest, mock, or degraded")
	flag.StringVar(&benchMode, "mode", "", "parallel or serial")
	flag.IntVar(&benchN, "n", 0, "partition size, even and at least 2")
	flag.StringVar(&benchCSV, "csv", "results.csv", "results CSV path")
}

// TestOrdering times one apply pass and one delete pass for the flags
// -backend, -mode, -n, and appends one CSV row to -csv.
func TestOrdering(t *testing.T) {
	if benchBackend == "" || benchMode == "" || benchN == 0 || benchCSV == "" {
		t.Fatal("TestOrdering requires -backend, -mode, -n, and -csv")
	}
	if benchN < 2 || benchN%2 != 0 {
		t.Fatalf("n must be an even integer >= 2, got %d", benchN)
	}
	switch benchBackend {
	case "envtest", "mock", "degraded":
	default:
		t.Fatalf("backend must be envtest, mock, or degraded, got %q", benchBackend)
	}
	switch benchMode {
	case "parallel", "serial":
	default:
		t.Fatalf("mode must be parallel or serial, got %q", benchMode)
	}

	ctx := context.Background()
	rec := &httpLog{}
	half := benchN / 2

	var dyn dynamic.Interface
	var mapper apimeta.RESTMapper
	var api *fakeAPI
	switch benchBackend {
	case "envtest":
		dyn, mapper = newEnvtestBackend(t, rec)
	default:
		dyn, mapper, api = newMockBackend(t, benchBackend == "degraded", seedKey(), rec)
	}

	store := memory.New()
	applyIDs := make([]desire.Identity, 0, half)
	deleteIDs := make([]desire.Identity, 0, half)
	for i := range half {
		name := fmt.Sprintf("cm-apply-%d", i)
		id := configMapIdentity(desire.TypeApply, name)
		content, err := configMapManifest(name)
		if err != nil {
			t.Fatalf("marshal apply manifest %s: %v", name, err)
		}
		if _, err := store.CreateApplyDesire(ctx, desire.ApplyDesire{
			Identity: id,
			Owner:    benchOwner,
			Spec:     desire.ApplySpec{KubeContent: content},
		}); err != nil {
			t.Fatalf("CreateApplyDesire %s: %v", name, err)
		}
		applyIDs = append(applyIDs, id)
	}
	for i := range half {
		name := fmt.Sprintf("cm-delete-%d", i)
		if api != nil {
			body, err := configMapBody(name)
			if err != nil {
				t.Fatalf("marshal delete object %s: %v", name, err)
			}
			api.seed(name, body)
		} else if err := createConfigMap(ctx, dyn, name); err != nil {
			t.Fatalf("create configmap %s: %v", name, err)
		}
		id := configMapIdentity(desire.TypeDelete, name)
		if _, err := store.CreateDeleteDesire(ctx, desire.DeleteDesire{
			Identity: id,
			Owner:    benchOwner,
		}); err != nil {
			t.Fatalf("CreateDeleteDesire %s: %v", name, err)
		}
		deleteIDs = append(deleteIDs, id)
	}

	applyR := applydesire.New(store, store, dyn, mapper, managementCluster, time.Hour)
	deleteR := deletedesire.New(store, store, dyn, mapper, managementCluster, time.Hour)

	// Seeding used the same client. Response time covers the pass only.
	rec.reset()

	var measured passTimes
	var applyErr, deleteErr error
	switch benchMode {
	case "parallel":
		measured, applyErr, deleteErr = runParallel(ctx, applyR, deleteR)
	default:
		measured, applyErr, deleteErr = runSerial(ctx, applyR, deleteR)
	}
	if applyErr != nil {
		t.Fatalf("apply pass: %v", applyErr)
	}
	if deleteErr != nil {
		t.Fatalf("delete pass: %v", deleteErr)
	}
	requireApplied(t, store, applyIDs)
	requireDeleted(t, store, deleteIDs)

	durs := rec.snapshot()
	p50, p99 := percentiles(durs)
	if err := appendCSV(benchCSV, []string{
		benchBackend,
		benchMode,
		strconv.Itoa(benchN),
		seconds(measured.wall),
		seconds(measured.applyPass),
		seconds(measured.deletePass),
		seconds(measured.deleteFinished),
		millis(p50),
		millis(p99),
		strconv.Itoa(len(durs)),
	}); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	t.Logf("backend=%s mode=%s n=%d wall=%s delete_finished=%s http_p50=%s http_p99=%s http_count=%d",
		benchBackend, benchMode, benchN, measured.wall, measured.deleteFinished, p50, p99, len(durs))
}

type passTimes struct {
	wall           time.Duration
	applyPass      time.Duration
	deletePass     time.Duration
	deleteFinished time.Duration
}

func runParallel(
	ctx context.Context, applyR *applydesire.ApplyReconciler, deleteR *deletedesire.DeleteReconciler,
) (passTimes, error, error) {
	var applyPass, deletePass, deleteFinished time.Duration
	var applyErr, deleteErr error
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		t0 := time.Now()
		applyErr = applyR.ReconcileOnce(ctx)
		applyPass = time.Since(t0)
	}()
	go func() {
		defer wg.Done()
		t0 := time.Now()
		deleteErr = deleteR.ReconcileOnce(ctx)
		deletePass = time.Since(t0)
		deleteFinished = time.Since(start)
	}()
	wg.Wait()
	return passTimes{
		wall:           time.Since(start),
		applyPass:      applyPass,
		deletePass:     deletePass,
		deleteFinished: deleteFinished,
	}, applyErr, deleteErr
}

func runSerial(
	ctx context.Context, applyR *applydesire.ApplyReconciler, deleteR *deletedesire.DeleteReconciler,
) (passTimes, error, error) {
	start := time.Now()
	t0 := time.Now()
	applyErr := applyR.ReconcileOnce(ctx)
	applyPass := time.Since(t0)
	t0 = time.Now()
	deleteErr := deleteR.ReconcileOnce(ctx)
	deletePass := time.Since(t0)
	elapsed := time.Since(start)
	return passTimes{
		wall:           elapsed,
		applyPass:      applyPass,
		deletePass:     deletePass,
		deleteFinished: elapsed,
	}, applyErr, deleteErr
}

func seedKey() string {
	return fmt.Sprintf("%s\x00%s\x00%d", benchBackend, benchMode, benchN)
}

func configMapIdentity(dtype desire.DesireType, name string) desire.Identity {
	return desire.Identity{
		ManagementCluster: managementCluster,
		Type:              dtype,
		Group:             "",
		Resource:          "configmaps",
		Namespace:         defaultNamespace,
		Name:              name,
	}
}

func requireApplied(t *testing.T, store *memory.Store, ids []desire.Identity) {
	t.Helper()
	failed := 0
	var sample string
	for _, id := range ids {
		got, err := store.GetApplyDesire(context.Background(), id)
		if err != nil {
			failed++
			if sample == "" {
				sample = err.Error()
			}
			continue
		}
		cond := apimeta.FindStatusCondition(got.Status.Conditions, desire.TypeSuccessful)
		if cond == nil || cond.Reason != desire.ReasonApplied || cond.Status != metav1.ConditionTrue {
			failed++
			if sample == "" {
				sample = fmt.Sprintf("%s status=%+v", id.Name, got.Status.Conditions)
			}
		}
	}
	if failed > 0 {
		t.Fatalf("%d/%d apply desires are not ReasonApplied; first: %s", failed, len(ids), sample)
	}
}

func requireDeleted(t *testing.T, store *memory.Store, ids []desire.Identity) {
	t.Helper()
	failed := 0
	var sample string
	for _, id := range ids {
		got, err := store.GetDeleteDesire(context.Background(), id)
		if err != nil {
			failed++
			if sample == "" {
				sample = err.Error()
			}
			continue
		}
		if !desire.IsDeleted(got.Status) {
			failed++
			if sample == "" {
				sample = fmt.Sprintf("%s status=%+v", id.Name, got.Status.Conditions)
			}
		}
	}
	if failed > 0 {
		t.Fatalf("%d/%d delete desires are not ReasonDeleted; first: %s", failed, len(ids), sample)
	}
}

func appendCSV(path string, row []string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	needHeader := false
	st, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		needHeader = true
	} else if st.Size() == 0 {
		needHeader = true
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if needHeader {
		if _, err := fmt.Fprintln(f, csvNote); err != nil {
			return err
		}
	}
	w := csv.NewWriter(f)
	if needHeader {
		if err := w.Write(csvHeader); err != nil {
			return err
		}
	}
	if err := w.Write(row); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func seconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', 6, 64)
}

func millis(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds()*1000, 'f', 3, 64)
}
