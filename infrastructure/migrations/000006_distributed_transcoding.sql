-- Distributed transcoding job hierarchy (Milestone 7).
-- The monolithic transcode job is replaced by a fan-out/aggregate pipeline:
--   plan  -> N renditions -> finalize
-- A `plan` job (the one ingest enqueues) probes the source, makes the thumbnail,
-- and fans out one `rendition` job per target quality. Each rendition transcodes
-- exactly one quality and atomically decrements the plan's pending-rendition
-- counter; whoever drives it to zero enqueues the `finalize` job, which writes
-- master.m3u8 and marks the video ready.
--
-- job_type is a free-text column (no enum), so the new 'plan' | 'rendition' |
-- 'finalize' values need no DDL beyond the new columns below. Historical rows use
-- 'transcode'; nothing reads that value, so they are left untouched.

ALTER TABLE video_jobs
  -- Renditions and the finalize job point back at their plan job. ON DELETE
  -- CASCADE so removing a video (which cascades to its jobs) stays consistent.
  ADD COLUMN IF NOT EXISTS parent_job_id UUID REFERENCES video_jobs(id) ON DELETE CASCADE,
  -- Aggregation counter, set on the plan job at fan-out time. Each finishing
  -- rendition decrements it; the one that hits 0 triggers finalize.
  ADD COLUMN IF NOT EXISTS pending_renditions INT,
  -- The single quality a rendition job must produce (quality, dims, bitrate,
  -- codec). Stored so the reaper can rebuild the queue message on requeue without
  -- re-running the planner.
  ADD COLUMN IF NOT EXISTS rendition_spec JSONB;

-- Reaper/aggregation lookups by parent.
CREATE INDEX IF NOT EXISTS idx_video_jobs_parent
  ON video_jobs(parent_job_id)
  WHERE parent_job_id IS NOT NULL;
