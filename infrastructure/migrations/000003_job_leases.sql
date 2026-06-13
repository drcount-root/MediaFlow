-- Job leases (Milestone 5.2).
-- A worker claims a job by stamping its id and a lease expiry; it heartbeats to
-- extend the lease while FFmpeg runs. A reaper finds jobs whose lease has
-- expired (the worker died without releasing it) and either re-enqueues them
-- (below max attempts) or marks them failed.

ALTER TABLE video_jobs
  ADD COLUMN IF NOT EXISTS claimed_by TEXT,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

-- Reaper scan: processing jobs ordered by when their lease runs out.
CREATE INDEX IF NOT EXISTS idx_video_jobs_lease
  ON video_jobs(lease_expires_at)
  WHERE status = 'processing';
