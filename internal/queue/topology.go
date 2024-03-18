package queue

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeType string

const (
	ExchangeDirect  ExchangeType = "direct"
	ExchangeTopic   ExchangeType = "topic"
	ExchangeFanout  ExchangeType = "fanout"
	ExchangeHeaders ExchangeType = "headers"
	ExchangeDelayed ExchangeType = "x-delayed-message"
)

type ExchangeConfig struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type QueueConfig struct {
	Name           string
	Durable        bool
	AutoDelete     bool
	Exclusive      bool
	NoWait         bool
	Args           amqp.Table
	DLX            string
	DLQ            string
	DLXRouting     string
	MessageTTL     int
	MaxLength      int
	MaxLengthBytes int
	MaxPriority    int
}

type BindingConfig struct {
	Queue    string
	Exchange string
	Routing  string
	NoWait   bool
	Args     amqp.Table
}

func defaultExchangeArgs(exchange string) amqp.Table {
	return amqp.Table{
		"x-delayed-type": "direct",
	}
}

func defaultQueueArgs(qc QueueConfig) amqp.Table {
	args := amqp.Table{}

	if qc.DLX != "" {
		args["x-dead-letter-exchange"] = qc.DLX
	}
	if qc.DLQ != "" {
		args["x-dead-letter-routing-key"] = qc.DLXRouting
	}
	if qc.MessageTTL > 0 {
		args["x-message-ttl"] = qc.MessageTTL
	}
	if qc.MaxLength > 0 {
		args["x-max-length"] = qc.MaxLength
	}
	if qc.MaxLengthBytes > 0 {
		args["x-max-length-bytes"] = qc.MaxLengthBytes
	}
	if qc.MaxPriority > 0 {
		args["x-max-priority"] = qc.MaxPriority
	}

	return args
}

type TopologyBuilder struct {
	exchanges []ExchangeConfig
	queues    []QueueConfig
	bindings  []BindingConfig
}

func NewTopologyBuilder() *TopologyBuilder {
	return &TopologyBuilder{}
}

func (tb *TopologyBuilder) AddExchange(cfg ExchangeConfig) *TopologyBuilder {
	tb.exchanges = append(tb.exchanges, cfg)
	return tb
}

func (tb *TopologyBuilder) AddQueue(cfg QueueConfig) *TopologyBuilder {
	tb.queues = append(tb.queues, cfg)
	return tb
}

func (tb *TopologyBuilder) AddBinding(cfg BindingConfig) *TopologyBuilder {
	tb.bindings = append(tb.bindings, cfg)
	return tb
}

func (tb *TopologyBuilder) DeclareAll(ctx context.Context, ch *amqp.Channel) error {
	for _, exc := range tb.exchanges {
		if err := DeclareExchange(ch, exc); err != nil {
			return fmt.Errorf("declare exchange %s: %w", exc.Name, err)
		}
	}

	for _, q := range tb.queues {
		if _, err := DeclareQueue(ch, q); err != nil {
			return fmt.Errorf("declare queue %s: %w", q.Name, err)
		}
	}

	for _, b := range tb.bindings {
		if err := BindQueue(ch, b); err != nil {
			return fmt.Errorf("bind queue %s to %s: %w", b.Queue, b.Exchange, err)
		}
	}

	return nil
}

func DeclareExchange(ch *amqp.Channel, cfg ExchangeConfig) error {
	return ch.ExchangeDeclare(
		cfg.Name,
		string(cfg.Type),
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Internal,
		cfg.NoWait,
		cfg.Args,
	)
}

func DeclareQueue(ch *amqp.Channel, cfg QueueConfig) (*amqp.Queue, error) {
	args := defaultQueueArgs(cfg)
	if cfg.Args != nil {
		for k, v := range cfg.Args {
			args[k] = v
		}
	}

	q, err := ch.QueueDeclare(
		cfg.Name,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Exclusive,
		cfg.NoWait,
		args,
	)
	if err != nil {
		return nil, err
	}

	return &q, nil
}

func BindQueue(ch *amqp.Channel, cfg BindingConfig) error {
	return ch.QueueBind(
		cfg.Queue,
		cfg.Routing,
		cfg.Exchange,
		cfg.NoWait,
		cfg.Args,
	)
}

func DeclareTopology(ctx context.Context, rmq *RabbitMQ) error {
	ch, err := rmq.AcquireChannel()
	if err != nil {
		return fmt.Errorf("acquire channel for topology: %w", err)
	}
	defer rmq.ReleaseChannel(ch)

	builder := NewTopologyBuilder()

	dlx := ExchangeConfig{
		Name:    "nexuspipe.dlx",
		Type:    ExchangeDirect,
		Durable: true,
	}
	builder.AddExchange(dlx)

	dlq := QueueConfig{
		Name:    "nexuspipe.dlq",
		Durable: true,
		Args: amqp.Table{
			"x-message-ttl":          60000,
			"x-dead-letter-exchange": "nexuspipe.direct",
		},
	}
	builder.AddQueue(dlq)

	builder.AddBinding(BindingConfig{
		Queue:    "nexuspipe.dlq",
		Exchange: "nexuspipe.dlx",
		Routing:  "dead-letter",
	})

	directExchange := ExchangeConfig{
		Name:    "nexuspipe.direct",
		Type:    ExchangeDirect,
		Durable: true,
	}
	builder.AddExchange(directExchange)

	topicExchange := ExchangeConfig{
		Name:    "nexuspipe.topic",
		Type:    ExchangeTopic,
		Durable: true,
	}
	builder.AddExchange(topicExchange)

	fanoutExchange := ExchangeConfig{
		Name:    "nexuspipe.fanout",
		Type:    ExchangeFanout,
		Durable: true,
	}
	builder.AddExchange(fanoutExchange)

	eventsQueue := QueueConfig{
		Name:       "nexuspipe.events",
		Durable:    true,
		DLX:        "nexuspipe.dlx",
		DLXRouting: "dead-letter",
	}
	builder.AddQueue(eventsQueue)

	notificationsQueue := QueueConfig{
		Name:       "nexuspipe.notifications",
		Durable:    true,
		DLX:        "nexuspipe.dlx",
		DLXRouting: "dead-letter",
	}
	builder.AddQueue(notificationsQueue)

	builder.AddBinding(BindingConfig{
		Queue:    "nexuspipe.events",
		Exchange: "nexuspipe.topic",
		Routing:  "event.#",
	})

	builder.AddBinding(BindingConfig{
		Queue:    "nexuspipe.notifications",
		Exchange: "nexuspipe.topic",
		Routing:  "notification.#",
	})

	if err := builder.DeclareAll(ctx, ch); err != nil {
		return fmt.Errorf("declare all topology: %w", err)
	}

	log.Println("topology declared successfully")
	return nil
}

func PurgeQueue(ctx context.Context, ch *amqp.Channel, queue string) (int, error) {
	count, err := ch.QueuePurge(queue, false)
	if err != nil {
		return 0, fmt.Errorf("purge queue %s: %w", queue, err)
	}
	return count, nil
}

func QueueLength(ctx context.Context, ch *amqp.Channel, queue string) (int, error) {
	q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	if err != nil {
		return 0, fmt.Errorf("inspect queue %s: %w", queue, err)
	}
	return q.Messages, nil
}
