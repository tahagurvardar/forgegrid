CREATE TABLE IF NOT EXISTS workers (
 id text PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 128),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS worker_sessions (
 id uuid PRIMARY KEY,
 worker_id text NOT NULL REFERENCES workers(id),
 state text NOT NULL CHECK (state IN ('ONLINE','OFFLINE','SUPERSEDED')),
 connected boolean NOT NULL DEFAULT true,
 capacity_slots integer NOT NULL CHECK (capacity_slots = 1),
 active_slots integer NOT NULL DEFAULT 0 CHECK (active_slots BETWEEN 0 AND capacity_slots),
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_assignment_at timestamptz,
 disconnected_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS one_online_session ON worker_sessions(worker_id) WHERE state = 'ONLINE';
CREATE TABLE IF NOT EXISTS jobs (
 id uuid PRIMARY KEY,
 state text NOT NULL CHECK (state IN ('QUEUED','DISPATCHED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED')),
 image text NOT NULL,
 command jsonb NOT NULL CHECK (jsonb_typeof(command) = 'array'),
 timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 86400),
 max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
 attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND max_attempts),
 current_attempt_id uuid,
 fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
 retry_available_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz
);
CREATE TABLE IF NOT EXISTS job_attempts (
 id uuid PRIMARY KEY,
 job_id uuid NOT NULL REFERENCES jobs(id),
 attempt_number integer NOT NULL CHECK (attempt_number > 0),
 fencing_token bigint NOT NULL CHECK (fencing_token > 0),
 worker_session_id uuid NOT NULL REFERENCES worker_sessions(id),
 state text NOT NULL CHECK (state IN ('ASSIGNED','RUNNING','SUCCEEDED','FAILED','TIMED_OUT','LOST')),
 lease_expires_at timestamptz NOT NULL,
 accepted_at timestamptz,
 assigned_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 started_at timestamptz,
 finished_at timestamptz,
 exit_code integer,
 failure_kind text NOT NULL DEFAULT '',
 failure_detail text NOT NULL DEFAULT '' CHECK (octet_length(failure_detail) <= 2048),
 UNIQUE(job_id, attempt_number),
 UNIQUE(job_id, fencing_token),
 UNIQUE(job_id, id)
);
-- Also prevents a job pointer from referencing an attempt belonging to another job.
DO $$ BEGIN
 ALTER TABLE jobs ADD CONSTRAINT current_attempt_fk FOREIGN KEY (id, current_attempt_id) REFERENCES job_attempts(job_id, id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS one_active_attempt ON job_attempts(job_id) WHERE state IN ('ASSIGNED','RUNNING');
CREATE INDEX IF NOT EXISTS runnable_jobs ON jobs(created_at) WHERE state IN ('QUEUED','RETRY_WAIT');
CREATE INDEX IF NOT EXISTS expiring_attempts ON job_attempts(lease_expires_at) WHERE state IN ('ASSIGNED','RUNNING');
CREATE TABLE IF NOT EXISTS job_log_chunks (
 attempt_id uuid NOT NULL REFERENCES job_attempts(id),
 sequence bigint NOT NULL CHECK (sequence > 0),
 stream text NOT NULL CHECK (stream IN ('STDOUT','STDERR')),
 payload bytea NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 16384),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(attempt_id, sequence)
);
