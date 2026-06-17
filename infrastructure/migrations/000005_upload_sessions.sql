-- Milestone 6: scalable ingest. Uploads go directly browser -> object storage
-- via presigned multipart; the API only tracks the session so it can issue part
-- URLs, report progress for resume, and validate/enqueue on completion.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'upload_session_status') THEN
    CREATE TYPE upload_session_status AS ENUM (
      'pending',
      'uploading',
      'completed',
      'aborted',
      'expired'
    );
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS upload_sessions (
  id UUID PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT,
  object_key TEXT NOT NULL,
  upload_id TEXT NOT NULL,
  part_size BIGINT NOT NULL,
  total_size BIGINT NOT NULL,
  part_count INT NOT NULL,
  content_type TEXT NOT NULL,
  original_filename TEXT NOT NULL,
  checksum_sha256 TEXT,
  status upload_session_status NOT NULL DEFAULT 'pending',
  video_id UUID REFERENCES videos(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_upload_sessions_status ON upload_sessions(status);
CREATE INDEX IF NOT EXISTS idx_upload_sessions_expires_at ON upload_sessions(expires_at);
