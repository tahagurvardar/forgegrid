//go:build integration

package postgres

import (
	"testing"
	"time"

	"forgegrid/db/migrations"
	"forgegrid/internal/testutil"
	"github.com/google/uuid"
)

func TestUpgradeAndIdempotentCancellationMigration(t *testing.T) {
	pool, ctx := testutil.Database(t)
	if _, err := pool.Exec(ctx, migrations.Initial); err != nil {
		t.Fatal(err)
	}
	s := &Store{Pool: pool, Lease: 10 * time.Second, Offline: 6 * time.Second}
	// Seed using the original schema, not the current binary's post-migration API.
	id := uuid.NewString()
	_, err := pool.Exec(ctx, `INSERT INTO jobs(id,state,image,command,timeout_seconds,max_attempts) VALUES($1,'QUEUED','alpine:3.22','["true"]',10,2)`, id)
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
