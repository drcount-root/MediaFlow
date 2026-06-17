package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/api/internal/videos"
)

var errNotConfirmed = errors.New("publish not confirmed by broker")

// RabbitPublisher publishes outbox messages to RabbitMQ with publisher confirms.
// It is driven by the outbox relay — the request path never publishes directly.
//
// The connection is re-established lazily: if the broker goes away and comes
// back, the next Publish redials and re-declares the topology before sending.
// This is what lets the outbox drain automatically after a broker restart rather
// than leaving videos stuck in `queued` until the API is bounced.
type RabbitPublisher struct {
	url string

	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewRabbitPublisher dials the broker once up front so a misconfigured URL fails
// fast at startup. After that, connection liveness is managed lazily per Publish.
func NewRabbitPublisher(url string) (*RabbitPublisher, error) {
	p := &RabbitPublisher{url: url}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

// connect dials the broker, opens a confirm-mode channel, and declares the
// exchange/queue topology. Callers must hold no expectation about prior state —
// connect replaces conn/channel wholesale. Not safe for concurrent use; callers
// guard with p.mu.
func (p *RabbitPublisher) connect() error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	if err := channel.ExchangeDeclare(videos.VideoExchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		conn.Close()
		return err
	}

	if _, err := channel.QueueDeclare(videos.TranscodeRoutingKey, true, false, false, false, nil); err != nil {
		conn.Close()
		return err
	}

	if err := channel.QueueBind(videos.TranscodeRoutingKey, videos.TranscodeRoutingKey, videos.VideoExchange, false, nil); err != nil {
		conn.Close()
		return err
	}

	// Publisher confirms: the relay only marks an outbox row sent once the broker
	// has acked it, so a publish that silently drops is retried next tick.
	if err := channel.Confirm(false); err != nil {
		conn.Close()
		return err
	}

	p.conn = conn
	p.channel = channel
	return nil
}

// ensureChannel returns a live channel, reconnecting if the current connection
// or channel has been closed (e.g. the broker restarted). Caller holds p.mu.
func (p *RabbitPublisher) ensureChannel() (*amqp.Channel, error) {
	if p.conn != nil && !p.conn.IsClosed() && p.channel != nil && !p.channel.IsClosed() {
		return p.channel, nil
	}
	// Drop whatever we had and redial.
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
		p.channel = nil
	}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p.channel, nil
}

// Publish sends body to the given exchange/routing key and blocks until the
// broker confirms it (or ctx is cancelled). The payload is already-marshalled
// JSON carried verbatim from the outbox row.
//
// A failure here (including a dead connection) is surfaced to the relay, which
// rolls back the batch and retries on its next tick; by then ensureChannel will
// have redialed. The publisher's own channel is invalidated on error so the next
// call is forced to reconnect.
func (p *RabbitPublisher) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	channel, err := p.ensureChannel()
	if err != nil {
		return err
	}

	confirm, err := channel.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
	if err != nil {
		p.invalidate()
		return err
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		p.invalidate()
		return err
	}
	if !acked {
		return errNotConfirmed
	}
	return nil
}

// invalidate tears down the current connection so the next ensureChannel
// redials. Caller holds p.mu.
func (p *RabbitPublisher) invalidate() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn = nil
	p.channel = nil
}

func (p *RabbitPublisher) Close() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		err := p.conn.Close()
		p.conn = nil
		p.channel = nil
		return err
	}

	return nil
}
