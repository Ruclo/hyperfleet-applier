package desire_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

func TestReportOwnerConflict_LogsWarn(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	before := desire.OwnerConflictTotal()
	desire.ReportOwnerConflict(context.Background(), validIdentity(), "owner-a", "owner-b")
	if desire.OwnerConflictTotal() != before+1 {
		t.Fatalf("OwnerConflictTotal: before=%d after=%d, want +1", before, desire.OwnerConflictTotal())
	}
	if !strings.Contains(buf.String(), "desire: owner conflict") {
		t.Fatalf("expected owner conflict warn log, got %q", buf.String())
	}
}

func TestCheckOwner_Mismatch(t *testing.T) {
	err := desire.CheckOwner(context.Background(), validIdentity(), "owner-a", "owner-b")
	if !errors.Is(err, desire.ErrOwnerConflict) {
		t.Fatalf("got %v, want ErrOwnerConflict", err)
	}
}

func TestCheckOwner_Match(t *testing.T) {
	if err := desire.CheckOwner(context.Background(), validIdentity(), "owner-a", "owner-a"); err != nil {
		t.Fatalf("got %v, want nil", err)
	}
}
