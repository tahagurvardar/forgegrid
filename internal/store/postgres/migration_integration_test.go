//go:build integration

package postgres

import (
	"testing"
	"time"

	"forgegrid/db/migrations"
	"forgegrid/internal/domain"
	"forgegrid/internal/testutil"
)

func TestUpgradeAndIdempotentCancellationMigration(t *testing.T) {
	pool, ctx := testutil.Database(t)
	if _, err := pool.Exec(ctx, migrations.Initial); err != nil {
		t.Fatal(err)
	}
	s := &Store{Pool: pool, Lease: 10 * time.Second, Offline: 6 * time.Second}
	id, err := s.Submit(ctx, domain.Spec{Image: "alpine:3.22", Command: []string{"true"}, TimeoutSeconds: 10, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, s, ctx)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if snapshot(t, s, ctx) != before {
		t.Fatal("reapplying migrations changed persisted data")
	}
	if _, err := s.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	j, err := s.GetJob(ctx, id)
	if err != nil || j.State != "CANCELLED" {
		t.Fatalf("upgraded cancellation: %+v %v", j, err)
	}
	var tables int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_type='BASE TABLE'`).Scan(&tables); err != nil || tables != 7 {
		t.Fatalf("table count=%d %v", tables, err)
	}
}
