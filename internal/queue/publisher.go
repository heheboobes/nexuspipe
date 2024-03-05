package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublishMode int

const (
	PublishModeNormal PublishMode = iota
	PublishModeConfirm
	PublishModeMandatory
)

type PublishOption func(*publishConfig)

type publishConfig struct {
	mode        PublishMode
	exchange    string
	headers     amqp.Table
	contentType string
	priority    uint8
	expiration  string
	timestamp   time.Time
	appID       string
	userID      string
	messageID   string
}

func defaultPublishConfig() publishConfig {
	return publishConfig{
		mode:        PublishModeNormal,
		contentType: "application/json",
		timestamp:   time.Now(),
	}
}

func WithConfirmMode() PublishOption {
	return func(c *publishConfig) { c.mode = PublishModeConfirm }
}

func WithMandatoryMode() PublishOption {
	return func(c *publishConfig) { c.mode = PublishModeMandatory }
}

func WithHeaders(h amqp.Table) PublishOption {
	return func(c *publishConfig) { c.headers = h }
}

func WithContentType(ct string) PublishOption {
	return func(c *publishConfig) { c.contentType = ct }
}

func WithPriority(p uint8) PublishOption {
	return func(c *publishConfig) { c.priority = p }
}

func WithExpiration(exp string) PublishOption {
	return func(c *publishConfig) { c.expiration = exp }
}

func WithAppID(appID string) PublishOption {
	return func(c *publishConfig) { c.appID = appID }
}

func WithMessageID(msgID string) PublishOption {
	return func(c *publishConfig) { c.messageID = msgID }
}

type Publisher struct {
	rmq      *RabbitMQ
	exchange string
	mu       sync.Mutex
	confirm  bool
}

func NewPublisher(rmq *RabbitMQ, exchange string) *Publisher {
	return &Publisher{
		rmq:      rmq,
		exchange: exchange,
	}
}

func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte, opts ...PublishOption) error {
	cfg := defaultPublishConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.exchange = p.exchange

	ch, err := p.rmq.AcquireChannel()
	if err != nil {
		return fmt.Errorf("acquire channel: %w", err)
	}
	defer p.rmq.ReleaseChannel(ch)

	if cfg.mode == PublishModeConfirm {
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("confirm mode: %w", err)
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		defer ch.Close()

		msg := p.buildPublishing(body, cfg)
		if err := ch.PublishWithContext(ctx, cfg.exchange, routingKey, cfg.mode == PublishModeMandatory, false, msg); err != nil {
			return fmt.Errorf("publish: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case confirm, ok := <-confirms:
			if !ok {
				return errors.New("confirm channel closed unexpectedly")
			}
			if !confirm.Ack {
				return fmt.Errorf("publish not confirmed (delivery tag: %d)", confirm.DeliveryTag)
			}
		}
		return nil
	}

	msg := p.buildPublishing(body, cfg)
	if err := ch.PublishWithContext(ctx, cfg.exchange, routingKey, cfg.mode == PublishModeMandatory, false, msg); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

func (p *Publisher) PublishBatch(ctx context.Context, routingKey string, bodies [][]byte, opts ...PublishOption) error {
	if len(bodies) == 0 {
		return nil
	}

	cfg := defaultPublishConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.exchange = p.exchange

	ch, err := p.rmq.AcquireChannel()
	if err != nil {
		return fmt.Errorf("acquire channel: %w", err)
	}
	defer p.rmq.ReleaseChannel(ch)

	if cfg.mode == PublishModeConfirm {
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("confirm mode: %w", err)
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, len(bodies)))
		defer ch.Close()

		for i, body := range bodies {
			msg := p.buildPublishing(body, cfg)
			if err := ch.PublishWithContext(ctx, cfg.exchange, routingKey, cfg.mode == PublishModeMandatory, false, msg); err != nil {
				return fmt.Errorf("publish batch item %d: %w", i, err)
			}
		}

		confirmed := 0
		for confirmed < len(bodies) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case confirm, ok := <-confirms:
				if !ok {
					return fmt.Errorf("confirm channel closed after %d/%d confirms", confirmed, len(bodies))
				}
				if !confirm.Ack {
					return fmt.Errorf("batch item delivery tag %d not confirmed", confirm.DeliveryTag)
				}
				confirmed++
			}
		}
		return nil
	}

	for i, body := range bodies {
		msg := p.buildPublishing(body, cfg)
		if err := ch.PublishWithContext(ctx, cfg.exchange, routingKey, cfg.mode == PublishModeMandatory, false, msg); err != nil {
			return fmt.Errorf("publish batch item %d: %w", i, err)
		}
	}

	return nil
}

func (p *Publisher) PublishJSON(ctx context.Context, routingKey string, v interface{}, opts ...PublishOption) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}
	return p.Publish(ctx, routingKey, body, opts...)
}

func (p *Publisher) PublishEnvelope(ctx context.Context, routingKey string, env *Envelope, opts ...PublishOption) error {
	data, err := env.Marshal()
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	opts = append(opts, WithHeaders(amqp.Table(env.Headers)))
	return p.Publish(ctx, routingKey, data, opts...)
}

func (p *Publisher) buildPublishing(body []byte, cfg publishConfig) amqp.Publishing {
	msg := amqp.Publishing{
		Body:         body,
		ContentType:  cfg.contentType,
		Timestamp:    cfg.timestamp,
		Priority:     cfg.priority,
		AppId:        cfg.appID,
		MessageId:    cfg.messageID,
		Headers:      cfg.headers,
		DeliveryMode: amqp.Persistent,
	}

	if cfg.expiration != "" {
		msg.Expiration = cfg.expiration
	}

	return msg
}

func (p *Publisher) PublishWithRetry(ctx context.Context, routingKey string, body []byte, maxRetries int, opts ...PublishOption) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt*100) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		if err := p.Publish(ctx, routingKey, body, opts...); err != nil {
			lastErr = err
			log.Printf("publish attempt %d/%d failed: %v", attempt+1, maxRetries+1, err)
			continue
		}
		return nil
	}
	return fmt.Errorf("publish failed after %d retries: %w", maxRetries, lastErr)
}
