-- Idempotency keys for upload (Milestone 5.4).
-- A client may send an `Idempotency-Key` header; replaying the same key returns
-- the original video instead of creating a duplicate. The key is stored on the
-- video row, with a partial unique index so concurrent replays collide on the DB
-- (the loser falls back to returning the winner's row).

ALTER TABLE videos
  ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_videos_idempotency_key
  ON videos(idempotency_key)
  WHERE idempotency_key IS NOT NULL;
