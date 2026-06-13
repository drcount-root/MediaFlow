// Package outbox implements the transactional-outbox relay (Milestone 5.1): it
// polls the outbox_messages table for unpublished rows and publishes them to the
// broker with publisher confirms, marking each row sent only once acked.
//
// Delivery is at-least-once: a row is published inside the same transaction that
// marks it sent, but a crash between the broker ack and the commit will replay
// the message on a later tick. Consumers must be idempotent.
package outbox

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

// Publisher delivers a single outbox message and returns only once the broker
// has confirmed it.
type Publisher interface {
	Publish(ctx context.Context, exchange, routingKey string, body []byte) error
}

type Relay struct {
	db        *sql.DB
	publisher Publisher
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

func NewRelay(db *sql.DB, publisher Publisher, logger *slog.Logger, interval time.Duration, batchSize int) *Relay {
	if interval <= 0 {
		interval = time.Second
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	return &Relay{
		db:        db,
		publisher: publisher,
		logger:    logger,
		interval:  interval,
		batchSize: batchSize,
	}
}

// Run drains the outbox on a ticker until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.Info("outbox relay started", "interval", r.interval.String(), "batchSize", r.batchSize)
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("outbox relay stopped")
			return
		case <-ticker.C:
			if _, err := r.drain(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("outbox drain failed", "error", err)
			}
		}
	}
}

// drain relays full batches until the outbox is empty (or an error stops it),
// so a backlog clears within one tick rather than one row per tick.
func (r *Relay) drain(ctx context.Context) (int, error) {
	total := 0
	for {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n, err := r.relayBatch(ctx)
		total += n
		if err != nil {
			return total, err
		}
		if n < r.batchSize {
			return total, nil
		}
	}
}

// relayBatch claims up to batchSize unpublished rows with FOR UPDATE SKIP LOCKED
// (so concurrent relays never grab the same row), publishes each, and marks the
// successfully published rows sent — all in one transaction. A publish failure
// rolls back the batch, leaving the rows for the next tick.
func (r *Relay) relayBatch(ctx context.Context) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, exchange, routing_key, payload_json
		FROM outbox_messages
		WHERE published_at IS NULL
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	`, r.batchSize)
	if err != nil {
		return 0, err
	}

	type message struct {
		id         string
		exchange   string
		routingKey string
		payload    []byte
	}
	var batch []message
	for rows.Next() {
		var m message
		if err := rows.Scan(&m.id, &m.exchange, &m.routingKey, &m.payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, m)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	if len(batch) == 0 {
		return 0, nil
	}

	published := 0
	for _, m := range batch {
		if err := r.publisher.Publish(ctx, m.exchange, m.routingKey, m.payload); err != nil {
			// Stop here: roll back so any rows already marked in this batch are
			// re-sent next tick (at-least-once) rather than lost.
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE outbox_messages SET published_at = now() WHERE id = $1
		`, m.id); err != nil {
			return 0, err
		}
		published++
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return published, nil
}
