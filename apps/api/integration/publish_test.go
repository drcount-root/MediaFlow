//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/api/internal/queue"
	"mediaflow/apps/api/internal/videos"
)

// TestPublishConsumeRoundTrip publishes a TranscodeJob through the production
// publisher and reads it back off the queue, verifying the wire contract.
func TestPublishConsumeRoundTrip(t *testing.T) {
	publisher, err := queue.NewRabbitPublisher(infra.rabbitURL)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	// Drain any leftover messages so we read the one we publish.
	drainTranscodeQueue(t)

	job := videos.TranscodeJob{
		JobID:        uuid.NewString(),
		VideoID:      uuid.NewString(),
		RawBucket:    rawBucket,
		RawObjectKey: "raw-videos/x/original.mp4",
		RequestedAt:  time.Now().UTC().Truncate(time.Second),
	}
	if err := publisher.PublishTranscode(context.Background(), job); err != nil {
		t.Fatalf("publish: %v", err)
	}

	got := consumeOneTranscodeJob(t, 10*time.Second)
	if got.JobID != job.JobID || got.VideoID != job.VideoID {
		t.Fatalf("round-trip mismatch: published %#v, consumed %#v", job, got)
	}
	if got.RawObjectKey != job.RawObjectKey || got.RawBucket != job.RawBucket {
		t.Fatalf("payload fields not preserved: %#v", got)
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

	// The publisher declares the exchange/queue, but a consumer-only test run
	// (or a drain before the first publish) needs them to exist too.
	if err := ch.ExchangeDeclare("mediaflow.video", amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := ch.QueueDeclare("video.transcode", true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := ch.QueueBind("video.transcode", "video.transcode", "mediaflow.video", false, nil); err != nil {
		t.Fatalf("bind queue: %v", err)
	}
	return ch
}

func drainTranscodeQueue(t *testing.T) {
	t.Helper()
	ch := rabbitChannel(t)
	if _, err := ch.QueuePurge("video.transcode", false); err != nil {
		t.Fatalf("purge queue: %v", err)
	}
}

func consumeOneTranscodeJob(t *testing.T, timeout time.Duration) videos.TranscodeJob {
	t.Helper()
	ch := rabbitChannel(t)

	deliveries, err := ch.Consume("video.transcode", "", true, false, false, false, nil)
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
