package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type HandlerFunc func(ctx context.Context, msg amqp.Delivery) error

type ConsumerConfig struct {
	Queue           string
	ConsumerTag     string
	AutoAck         bool
	Exclusive       bool
	NoLocal         bool
	NoWait          bool
	PrefetchCount   int
	PrefetchGlobal  bool
	Concurrency     int
	RequeueOnFail   bool
	MaxRetry        int
	RetryDelay      time.Duration
	ShutdownTimeout time.Duration
	Args            amqp.Table
}

func DefaultConsumerConfig(queue string) ConsumerConfig {
	return ConsumerConfig{
		Queue:           queue,
		ConsumerTag:     fmt.Sprintf("consumer-%s-%d", queue, time.Now().UnixNano()),
		AutoAck:         false,
		PrefetchCount:   10,
		Concurrency:     1,
		RequeueOnFail:   true,
		MaxRetry:        3,
		RetryDelay:      time.Second,
		ShutdownTimeout: 10 * time.Second,
	}
}

type Consumer struct {
	rmq        *RabbitMQ
	cfg        ConsumerConfig
	handler    HandlerFunc
	cancel     context.CancelFunc
	deliveries <-chan amqp.Delivery
	workers    sync.WaitGroup
	mu         sync.Mutex
	running    bool
	done       chan struct{}
}

func NewConsumer(rmq *RabbitMQ, cfg ConsumerConfig, handler HandlerFunc) *Consumer {
	return &Consumer{
		rmq:     rmq,
		cfg:     cfg,
		handler: handler,
		done:    make(chan struct{}),
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("consumer already running")
	}
	c.running = true
	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	ch, err := c.rmq.AcquireChannel()
	if err != nil {
		return fmt.Errorf("acquire channel: %w", err)
	}

	if err := ch.Qos(c.cfg.PrefetchCount, 0, c.cfg.PrefetchGlobal); err != nil {
		c.rmq.ReleaseChannel(ch)
		return fmt.Errorf("set qos: %w", err)
	}

	deliveries, err := ch.Consume(
		c.cfg.Queue,
		c.cfg.ConsumerTag,
		c.cfg.AutoAck,
		c.cfg.Exclusive,
		c.cfg.NoLocal,
		c.cfg.NoWait,
		c.cfg.Args,
	)
	if err != nil {
		c.rmq.ReleaseChannel(ch)
		return fmt.Errorf("consume: %w", err)
	}

	c.deliveries = deliveries

	for i := 0; i < c.cfg.Concurrency; i++ {
		c.workers.Add(1)
		workerID := i
		go c.workerLoop(ctx, workerID)
	}

	go func() {
		<-ctx.Done()
		c.rmq.ReleaseChannel(ch)
	}()

	log.Printf("consumer started: tag=%s, queue=%s, concurrency=%d",
		c.cfg.ConsumerTag, c.cfg.Queue, c.cfg.Concurrency)

	return nil
}

func (c *Consumer) workerLoop(ctx context.Context, workerID int) {
	defer c.workers.Done()

	log.Printf("worker %d started for queue %s", workerID, c.cfg.Queue)

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d shutting down: %v", workerID, ctx.Err())
			return
		case msg, ok := <-c.deliveries:
			if !ok {
				log.Printf("worker %d: delivery channel closed", workerID)
				return
			}
			c.processMessage(ctx, workerID, msg)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, workerID int, msg amqp.Delivery) {
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetry; attempt++ {
		select {
		case <-ctx.Done():
			c.nack(msg, false)
			return
		default:
		}

		if attempt > 0 {
			delay := c.cfg.RetryDelay * time.Duration(1<<uint(attempt-1))
			log.Printf("worker %d: retrying message (attempt %d/%d) after %v",
				workerID, attempt+1, c.cfg.MaxRetry+1, delay)

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				c.nack(msg, false)
				return
			case <-timer.C:
			}
			timer.Stop()
		}

		if err := c.handler(ctx, msg); err != nil {
			lastErr = err
			log.Printf("worker %d: handler error (attempt %d/%d): %v",
				workerID, attempt+1, c.cfg.MaxRetry+1, err)
			continue
		}

		if !c.cfg.AutoAck {
			if err := msg.Ack(false); err != nil {
				log.Printf("worker %d: ack failed: %v", workerID, err)
			}
		}
		return
	}

	log.Printf("worker %d: message failed after %d attempts, last error: %v",
		workerID, c.cfg.MaxRetry+1, lastErr)

	c.nack(msg, c.cfg.RequeueOnFail)
}

func (c *Consumer) nack(msg amqp.Delivery, requeue bool) {
	if c.cfg.AutoAck {
		return
	}
	if err := msg.Nack(false, requeue); err != nil {
		log.Printf("nack failed (requeue=%v): %v", requeue, err)
	}
}

func (c *Consumer) Cancel() error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	done := make(chan struct{})
	go func() {
		c.workers.Wait()
		close(done)
	}()

	timer := time.NewTimer(c.cfg.ShutdownTimeout)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		log.Printf("consumer shutdown timeout after %v", c.cfg.ShutdownTimeout)
	}

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	log.Printf("consumer stopped: tag=%s, queue=%s", c.cfg.ConsumerTag, c.cfg.Queue)
	return nil
}

func (c *Consumer) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

type ConsumerPool struct {
	consumers []*Consumer
	mu        sync.Mutex
}

func NewConsumerPool() *ConsumerPool {
	return &ConsumerPool{}
}

func (p *ConsumerPool) Add(consumer *Consumer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.consumers = append(p.consumers, consumer)
}

func (p *ConsumerPool) StartAll(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.consumers {
		if err := c.Start(ctx); err != nil {
			return fmt.Errorf("start consumer %s: %w", c.cfg.ConsumerTag, err)
		}
	}
	return nil
}

func (p *ConsumerPool) StopAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.consumers {
		if err := c.Cancel(); err != nil {
			log.Printf("error stopping consumer %s: %v", c.cfg.ConsumerTag, err)
		}
	}
}
