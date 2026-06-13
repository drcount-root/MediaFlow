package queue

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/api/internal/videos"
)

var errNotConfirmed = errors.New("publish not confirmed by broker")

// RabbitPublisher publishes outbox messages to RabbitMQ with publisher confirms.
// It is driven by the outbox relay — the request path never publishes directly.
type RabbitPublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewRabbitPublisher(url string) (*RabbitPublisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	if err := channel.ExchangeDeclare(videos.VideoExchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	if _, err := channel.QueueDeclare(videos.TranscodeRoutingKey, true, false, false, false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	if err := channel.QueueBind(videos.TranscodeRoutingKey, videos.TranscodeRoutingKey, videos.VideoExchange, false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	// Publisher confirms: the relay only marks an outbox row sent once the broker
	// has acked it, so a publish that silently drops is retried next tick.
	if err := channel.Confirm(false); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	return &RabbitPublisher{conn: conn, channel: channel}, nil
}

// Publish sends body to the given exchange/routing key and blocks until the
// broker confirms it (or ctx is cancelled). The payload is already-marshalled
// JSON carried verbatim from the outbox row.
func (p *RabbitPublisher) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	confirm, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
	if err != nil {
		return err
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errNotConfirmed
	}
	return nil
}

func (p *RabbitPublisher) Close() error {
	if p == nil {
		return nil
	}

	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}

	return nil
}
