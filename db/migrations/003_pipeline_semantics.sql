CREATE TABLE IF NOT EXISTS pipelines (
 id uuid PRIMARY KEY,
 state text NOT NULL CHECK (state IN ('RUNNING','CANCELLING','SUCCEEDED','FAILED','CANCELLED')),
 cancel_requested_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz
);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS pipeline_id uuid REFERENCES pipelines(id);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS job_key text;
DO $$ BEGIN
 IF position('BLOCKED' in (SELECT pg_get_constraintdef(oid) FROM pg_constraint
     WHERE conrelid='jobs'::regclass AND conname='jobs_state_check')) = 0 THEN
  ALTER TABLE jobs DROP CONSTRAINT jobs_state_check;
  ALTER TABLE jobs ADD CONSTRAINT jobs_state_check CHECK
   (state IN ('BLOCKED','QUEUED','DISPATCHED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLING','CANCELLED','SKIPPED'));
 END IF;
END $$;
DO $$ BEGIN
 ALTER TABLE jobs ADD CONSTRAINT pipeline_key_pair CHECK
  ((pipeline_id IS NULL AND job_key IS NULL) OR (pipeline_id IS NOT NULL AND job_key IS NOT NULL AND length(job_key) BETWEEN 1 AND 128));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS pipeline_job_key ON jobs(pipeline_id,job_key);
CREATE UNIQUE INDEX IF NOT EXISTS pipeline_job_identity ON jobs(pipeline_id,id);
CREATE TABLE IF NOT EXISTS job_dependencies (
 pipeline_id uuid NOT NULL REFERENCES pipelines(id),
 job_id uuid NOT NULL,
 depends_on_job_id uuid NOT NULL,
 PRIMARY KEY(job_id,depends_on_job_id),
 CHECK (job_id<>depends_on_job_id),
 FOREIGN KEY(pipeline_id,job_id) REFERENCES jobs(pipeline_id,id),
 FOREIGN KEY(pipeline_id,depends_on_job_id) REFERENCES jobs(pipeline_id,id)
);
CREATE INDEX IF NOT EXISTS dependency_parents ON job_dependencies(depends_on_job_id);
ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS execution_deadline_at timestamptz;
