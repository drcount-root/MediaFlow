//go:build integration

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// startTestRelay runs a minimal outbox relay for the duration of a test. In
// production the API owns the relay; the worker only writes outbox rows (planner
// fan-out, rendition→finalize hand-off, reaper requeues). These tests run the
// worker in isolation, so they need a relay to publish those rows to RabbitMQ.
// It mirrors apps/api/internal/outbox: claim unpublished rows with FOR UPDATE
// SKIP LOCKED, publish with confirms, mark them sent.
func startTestRelay(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	conn, err := amqp.Dial(infra.rabbitURL)
	if err != nil {
		t.Fatalf("relay amqp dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatalf("relay amqp channel: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		conn.Close()
		t.Fatalf("relay confirm mode: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(); _ = conn.Close() })

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := drainOutbox(ctx, db, ch); err != nil && ctx.Err() == nil {
					t.Logf("test relay drain error: %v", err)
				}
			}
		}
	}()
}

func drainOutbox(ctx context.Context, db *sql.DB, ch *amqp.Channel) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, exchange, routing_key, payload_json
		FROM outbox_messages
		WHERE published_at IS NULL
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 50
	`)
	if err != nil {
		return err
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
			return err
		}
		batch = append(batch, m)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, m := range batch {
		confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, m.exchange, m.routingKey, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         m.payload,
		})
		if err != nil {
			return err
		}
		if acked, err := confirm.WaitContext(ctx); err != nil || !acked {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET published_at = now() WHERE id = $1`, m.id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
