export interface Attempt {
  attempt_id: string;
  job_id: string;
  attempt_number: number;
  worker_id: string;
  worker_session_id: string;
  fencing_token: number;
  state: string;
  lease_expires_at: string;
  execution_deadline_at?: string;
  assigned_at: string;
  started_at: string | null;
  finished_at: string | null;
  exit_code: number | null;
  failure_kind: string;
  failure_detail: string;
  trace_id?: string;
}
export interface Job {
  id: string;
  pipeline_id?: string;
  key?: string;
  dependencies?: string[];
  image: string;
  command: string[];
  timeout_seconds: number;
  max_attempts: number;
  state: string;
  current_attempt_id: string | null;
  fencing_token: number;
  attempt_count: number;
  attempts: Attempt[];
  created_at: string;
  finished_at: string | null;
  retry_available_at: string | null;
}
export interface Pipeline {
  id: string;
  state: string;
  jobs: Job[];
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}
export interface PipelineSummary extends Omit<Pipeline, "jobs"> {
  job_count: number;
  terminal_count: number;
  attempt_count: number;
}
export interface Worker {
  worker_id: string;
  worker_session_id: string;
  state: string;
  current: boolean;
  connected: boolean;
  active_slots: number;
  capacity_slots: number;
  last_seen_at: string;
  started_at: string;
  disconnected_at: string | null;
}
export interface Overview {
  server_time: string;
  jobs: Record<string, number>;
  recent_activity: Attempt[];
  active_attempts: Attempt[];
}
export interface Page<T> {
  items: T[];
  next_offset: number | null;
}
export interface LogChunk {
  sequence: number;
  stream: "STDOUT" | "STDERR";
  payload: string;
}
export const terminalJob = (state: string) =>
  ["SUCCEEDED", "FAILED", "SKIPPED", "CANCELLED"].includes(state);
