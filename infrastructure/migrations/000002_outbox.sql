-- Transactional outbox (Milestone 5.1).
-- Upload writes video + job + outbox row in one transaction and never talks to
-- RabbitMQ directly; a relay loop in the API publishes unsent rows with
-- publisher confirms. Delivery is at-least-once, so consumers must be idempotent.

CREATE TABLE IF NOT EXISTS outbox_messages (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  exchange TEXT NOT NULL,
  routing_key TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

-- Hot path for the relay: only the not-yet-published rows, oldest first.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
  ON outbox_messages(created_at)
  WHERE published_at IS NULL;
