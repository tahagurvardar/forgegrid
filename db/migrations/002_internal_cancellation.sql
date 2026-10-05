-- Gate C internal cancellation. No HTTP cancellation endpoint is introduced.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cancel_requested_at timestamptz;
DO $$ BEGIN
 IF position('CANCELLING' in (SELECT pg_get_constraintdef(oid) FROM pg_constraint
     WHERE conrelid='jobs'::regclass AND conname='jobs_state_check')) = 0 THEN
  ALTER TABLE jobs DROP CONSTRAINT jobs_state_check;
  ALTER TABLE jobs ADD CONSTRAINT jobs_state_check CHECK
   (state IN ('QUEUED','DISPATCHED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLING','CANCELLED'));
 END IF;
 IF position('CANCELLED' in (SELECT pg_get_constraintdef(oid) FROM pg_constraint
     WHERE conrelid='job_attempts'::regclass AND conname='job_attempts_state_check')) = 0 THEN
  ALTER TABLE job_attempts DROP CONSTRAINT job_attempts_state_check;
  ALTER TABLE job_attempts ADD CONSTRAINT job_attempts_state_check CHECK
   (state IN ('ASSIGNED','RUNNING','SUCCEEDED','FAILED','TIMED_OUT','LOST','CANCELLED'));
 END IF;
END $$;
