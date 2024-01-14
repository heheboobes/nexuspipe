package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/config"
	"github.com/heheboobes/nexuspipe/internal/logger"
)

var cfgFile string

type Worker struct {
	cfg     *config.Config
	log     *zap.SugaredLogger
	pgPool  *pgxpool.Pool
	rmqConn *amqp091.Connection
	rmqChan *amqp091.Channel
	rdb     *redis.Client
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "nexuspipe-worker",
		Short: "NexusPipe background worker",
		Long:  `Background worker that consumes and processes events and tasks from RabbitMQ queues.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runWorker()
		},
	}

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "config.yaml", "path to config file")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func runWorker() error {
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	logCfg := logger.Config{
		Level:     cfg.Log.Level,
		Format:    cfg.Log.Format,
		AddSource: cfg.Log.AddSource,
	}

	zapLog, err := logger.NewLogger(logCfg)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer zapLog.Sync()

	sugar := zapLog.Sugar()
	sugar.Infow("starting NexusPipe worker",
		"concurrency", cfg.Worker.Concurrency,
		"queues", cfg.Worker.QueueNames,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := &Worker{
		cfg:    cfg,
		log:    sugar,
		ctx:    ctx,
		cancel: cancel,
	}

	if err := worker.connectDatabase(); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	defer worker.pgPool.Close()

	if err := worker.connectRabbitMQ(); err != nil {
		return fmt.Errorf("rabbitmq connection failed: %w", err)
	}
	defer worker.rmqConn.Close()
	defer worker.rmqChan.Close()

	if err := worker.connectRedis(); err != nil {
		return fmt.Errorf("redis connection failed: %w", err)
	}
	defer worker.rdb.Close()

	worker.startConsumers()

	sugar.Info("worker is running. waiting for signals...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	sugar.Infow("shutting down worker...")
	cancel()
	worker.wg.Wait()

	sugar.Infow("worker shut down gracefully")
	return nil
}

func (w *Worker) connectDatabase() error {
	dsn := w.cfg.Database.DSN
	if dsn == "" {
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			w.cfg.Database.User,
			w.cfg.Database.Password,
			w.cfg.Database.Host,
			w.cfg.Database.Port,
			w.cfg.Database.DBName,
			w.cfg.Database.SSLMode,
		)
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse database config: %w", err)
	}

	poolCfg.MaxConns = int32(w.cfg.Database.MaxOpenConns)
	poolCfg.MinConns = int32(w.cfg.Database.MaxIdleConns)
	poolCfg.MaxConnLifetime = w.cfg.Database.ConnMaxLifetime
	poolCfg.MaxConnIdleTime = w.cfg.Database.ConnMaxIdleTime

	pool, err := pgxpool.NewWithConfig(w.ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(w.ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	w.pgPool = pool
	w.log.Infow("connected to database", "pool_size", poolCfg.MaxConns)
	return nil
}

func (w *Worker) connectRabbitMQ() error {
	url := w.cfg.RabbitMQ.URL
	if url == "" {
		url = fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
			w.cfg.RabbitMQ.User,
			w.cfg.RabbitMQ.Password,
			w.cfg.RabbitMQ.Host,
			w.cfg.RabbitMQ.Port,
			w.cfg.RabbitMQ.VHost,
		)
	}

	conn, err := amqp091.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open rabbitmq channel: %w", err)
	}

	if err := ch.Qos(w.cfg.RabbitMQ.PrefetchCount, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to set qos: %w", err)
	}

	w.rmqConn = conn
	w.rmqChan = ch

	go w.handleRabbitMQReconnect()

	w.log.Infow("connected to rabbitmq",
		"host", w.cfg.RabbitMQ.Host,
		"prefetch", w.cfg.RabbitMQ.PrefetchCount,
	)
	return nil
}

func (w *Worker) handleRabbitMQReconnect() {
	notify := w.rmqConn.NotifyClose(make(chan *amqp091.Error))
	err, ok := <-notify
	if !ok {
		return
	}

	w.log.Warnw("rabbitmq connection lost", "reason", err.Reason, "code", err.Code)

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(w.cfg.RabbitMQ.ReconnectInterval):
			w.log.Infow("attempting to reconnect to rabbitmq...")
			if connErr := w.connectRabbitMQ(); connErr == nil {
				w.log.Infow("reconnected to rabbitmq")
				w.startConsumers()
				return
			}
			w.log.Warnw("reconnect failed, retrying...")
		}
	}
}

func (w *Worker) connectRedis() error {
	opts := &redis.Options{
		Addr:         fmt.Sprintf("%s:%d", w.cfg.Redis.Host, w.cfg.Redis.Port),
		Password:     w.cfg.Redis.Password,
		DB:           w.cfg.Redis.DB,
		PoolSize:     w.cfg.Redis.PoolSize,
		MinIdleConns: w.cfg.Redis.MinIdleConns,
		DialTimeout:  w.cfg.Redis.DialTimeout,
		ReadTimeout:  w.cfg.Redis.ReadTimeout,
		WriteTimeout: w.cfg.Redis.WriteTimeout,
	}

	rdb := redis.NewClient(opts)
	if err := rdb.Ping(w.ctx).Err(); err != nil {
		return fmt.Errorf("redis ping failed: %w", err)
	}

	w.rdb = rdb
	w.log.Infow("connected to redis", "addr", opts.Addr)
	return nil
}

func (w *Worker) startConsumers() {
	for _, queueName := range w.cfg.Worker.QueueNames {
		for i := 0; i < w.cfg.Worker.Concurrency; i++ {
			w.wg.Add(1)
			go w.consumeQueue(queueName, i)
		}
		w.log.Infow("started consumers for queue",
			"queue", queueName,
			"concurrency", w.cfg.Worker.Concurrency,
		)
	}
}

func (w *Worker) consumeQueue(queueName string, consumerID int) {
	defer w.wg.Done()

	msgs, err := w.rmqChan.Consume(
		queueName,
		fmt.Sprintf("worker-%d-%d", consumerID, time.Now().Unix()),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		w.log.Errorw("failed to register consumer",
			"queue", queueName,
			"consumer_id", consumerID,
			"error", err,
		)
		return
	}

	w.log.Infow("consumer registered",
		"queue", queueName,
		"consumer_id", consumerID,
	)

	for {
		select {
		case <-w.ctx.Done():
			w.log.Infow("consumer shutting down",
				"queue", queueName,
				"consumer_id", consumerID,
			)
			return
		case msg, ok := <-msgs:
			if !ok {
				w.log.Warnw("consumer channel closed",
					"queue", queueName,
					"consumer_id", consumerID,
				)
				return
			}
			w.processMessage(msg)
		}
	}
}

func (w *Worker) processMessage(msg amqp091.Delivery) {
	startTime := time.Now()
	w.log.Infow("processing message",
		"routing_key", msg.RoutingKey,
		"message_id", msg.MessageId,
		"delivery_tag", msg.DeliveryTag,
		"size", len(msg.Body),
	)

	if err := w.handleDelivery(msg); err != nil {
		w.log.Errorw("message processing failed",
			"error", err,
			"delivery_tag", msg.DeliveryTag,
		)

		if err := msg.Nack(false, msg.Redelivered); err != nil {
			w.log.Errorw("failed to nack message", "error", err)
		}
		return
	}

	if err := msg.Ack(false); err != nil {
		w.log.Errorw("failed to ack message", "error", err)
	}

	w.log.Infow("message processed successfully",
		"delivery_tag", msg.DeliveryTag,
		"duration", time.Since(startTime),
	)
}

func (w *Worker) handleDelivery(msg amqp091.Delivery) error {
	_ = w.ctx
	_ = w.pgPool

	switch msg.RoutingKey {
	case "pipeline.execute":
		return w.handlePipelineExecute(msg.Body)
	case "task.process":
		return w.handleTaskProcess(msg.Body)
	case "event.ingest":
		return w.handleEventIngest(msg.Body)
	default:
		return fmt.Errorf("unknown routing key: %s", msg.RoutingKey)
	}
}

func (w *Worker) handlePipelineExecute(body []byte) error {
	w.log.Infow("handling pipeline execution", "payload_size", len(body))
	return nil
}

func (w *Worker) handleTaskProcess(body []byte) error {
	w.log.Infow("handling task processing", "payload_size", len(body))
	return nil
}

func (w *Worker) handleEventIngest(body []byte) error {
	w.log.Infow("handling event ingestion", "payload_size", len(body))
	return nil
}
