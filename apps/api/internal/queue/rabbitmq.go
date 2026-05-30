package queue

import (
	"context"
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/api/internal/videos"
)

const (
	videoExchange       = "mediaflow.video"
	transcodeRoutingKey = "video.transcode"
	transcodeQueue      = "video.transcode"
)

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

	if err := channel.ExchangeDeclare(videoExchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	if _, err := channel.QueueDeclare(transcodeQueue, true, false, false, false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	if err := channel.QueueBind(transcodeQueue, transcodeRoutingKey, videoExchange, false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	return &RabbitPublisher{conn: conn, channel: channel}, nil
}

func (p *RabbitPublisher) PublishTranscode(ctx context.Context, job videos.TranscodeJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return p.channel.PublishWithContext(ctx, videoExchange, transcodeRoutingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
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
