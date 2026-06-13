//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/api/internal/outbox"
	"mediaflow/apps/api/internal/queue"
	"mediaflow/apps/api/internal/videos"
)

// TestRelayDeliversOutboxToQueue inserts an unpublished outbox row, runs the
// relay against real RabbitMQ, and asserts the message is delivered to the
// transcode queue with publisher confirms and the row is marked published.
func TestRelayDeliversOutboxToQueue(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	drainTranscodeQueue(t)
	ctx := context.Background()

	job := videos.TranscodeJob{
		JobID:        uuid.NewString(),
		VideoID:      uuid.NewString(),
		RawBucket:    rawBucket,
		RawObjectKey: "raw-videos/x/original.mp4",
		RequestedAt:  time.Now().UTC().Truncate(time.Second),
	}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}

	var outboxID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO outbox_messages (exchange, routing_key, payload_json)
		VALUES ($1, $2, $3)
		RETURNING id
	`, videos.VideoExchange, videos.TranscodeRoutingKey, payload).Scan(&outboxID); err != nil {
		t.Fatalf("insert outbox row: %v", err)
	}

	publisher, err := queue.NewRabbitPublisher(infra.rabbitURL)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	relay := outbox.NewRelay(db, publisher, slog.New(slog.NewTextHandler(io.Discard, nil)), 50*time.Millisecond, 10)
	relayCtx, cancel := context.WithCancel(ctx)
	relayDone := make(chan struct{})
	go func() {
		relay.Run(relayCtx)
		close(relayDone)
	}()
	// Stop the relay and wait for it to exit before the test returns, so it can
	// never act on a later test's rows (the package shares one database).
	defer func() {
		cancel()
		<-relayDone
	}()

	// The relay should publish to the queue.
	got := consumeOneTranscodeJob(t, 10*time.Second)
	if got.JobID != job.JobID || got.VideoID != job.VideoID || got.RawObjectKey != job.RawObjectKey {
		t.Fatalf("delivered job mismatch: inserted %#v, consumed %#v", job, got)
	}

	// ...and mark the row published.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var published *string
		if err := db.QueryRowContext(ctx,
			`SELECT published_at::text FROM outbox_messages WHERE id = $1`, outboxID).Scan(&published); err != nil {
			t.Fatalf("read outbox row: %v", err)
		}
		if published != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox row was not marked published within 5s")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func rabbitChannel(t *testing.T) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(infra.rabbitURL)
	if err != nil {
		t.Fatalf("amqp dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("amqp channel: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })

	// Ensure the exchange/queue exist for a consumer-only run (e.g. draining
	// before the relay has declared them).
	if err := ch.ExchangeDeclare(videos.VideoExchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := ch.QueueDeclare(videos.TranscodeRoutingKey, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := ch.QueueBind(videos.TranscodeRoutingKey, videos.TranscodeRoutingKey, videos.VideoExchange, false, nil); err != nil {
		t.Fatalf("bind queue: %v", err)
	}
	return ch
}

func drainTranscodeQueue(t *testing.T) {
	t.Helper()
	ch := rabbitChannel(t)
	if _, err := ch.QueuePurge(videos.TranscodeRoutingKey, false); err != nil {
		t.Fatalf("purge queue: %v", err)
	}
}

func consumeOneTranscodeJob(t *testing.T, timeout time.Duration) videos.TranscodeJob {
	t.Helper()
	ch := rabbitChannel(t)

	deliveries, err := ch.Consume(videos.TranscodeRoutingKey, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	select {
	case d := <-deliveries:
		var job videos.TranscodeJob
		if err := json.Unmarshal(d.Body, &job); err != nil {
			t.Fatalf("unmarshal job: %v", err)
		}
		return job
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for a transcode message")
		return videos.TranscodeJob{}
	}
}
