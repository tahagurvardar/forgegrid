-- Optional diagnostic context only; never used to validate ownership/readiness.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS trace_parent text NOT NULL DEFAULT '';
ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS trace_parent text NOT NULL DEFAULT '';
-- Diagnostic timestamps do not determine readiness or authority.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_queued_at timestamptz;
ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS queue_wait_seconds double precision;
