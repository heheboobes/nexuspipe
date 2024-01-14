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
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/config"
	"github.com/heheboobes/nexuspipe/internal/logger"
)

var cfgFile string

type Scheduler struct {
	cfg     *config.Config
	log     *zap.SugaredLogger
	pgPool  *pgxpool.Pool
	rmqChan *amqp091.Channel
	cron    *cron.Cron
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	jobs    map[string]cron.EntryID
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "nexuspipe-scheduler",
		Short: "NexusPipe cron scheduler",
		Long:  `Cron-based scheduler that dispatches periodic pipeline executions and maintenance tasks.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runScheduler()
		},
	}

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "config.yaml", "path to config file")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func runScheduler() error {
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
	sugar.Infow("starting NexusPipe scheduler",
		"timezone", cfg.Scheduler.Timezone,
		"cron_jobs", len(cfg.Scheduler.CronExpressions),
	)

	if !cfg.Scheduler.Enabled {
		sugar.Infow("scheduler is disabled in config, exiting")
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched := &Scheduler{
		cfg:    cfg,
		log:    sugar,
		ctx:    ctx,
		cancel: cancel,
		jobs:   make(map[string]cron.EntryID),
	}

	if err := sched.connectDatabase(); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	defer sched.pgPool.Close()

	if err := sched.connectRabbitMQ(); err != nil {
		return fmt.Errorf("rabbitmq connection failed: %w", err)
	}
	defer sched.rmqChan.Close()

	loc, err := time.LoadLocation(cfg.Scheduler.Timezone)
	if err != nil {
		sugar.Warnw("invalid timezone, falling back to UTC",
			"timezone", cfg.Scheduler.Timezone,
			"error", err,
		)
		loc = time.UTC
	}

	sched.cron = cron.New(
		cron.WithLocation(loc),
		cron.WithSeconds(),
		cron.WithLogger(cron.PrintfLogger(log.New(os.Stdout, "cron: ", log.LstdFlags))),
	)

	sched.registerBuiltinJobs()
	sched.registerConfigJobs()

	sched.cron.Start()
	sugar.Infow("scheduler started", "job_count", len(sched.cron.Entries()))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	sugar.Infow("shutting down scheduler...")

	stopCtx := sched.cron.Stop()
	select {
	case <-stopCtx.Done():
		sugar.Infow("all cron jobs stopped")
	case <-time.After(30 * time.Second):
		sugar.Warnw("timeout waiting for cron jobs to stop")
	}

	cancel()
	sched.wg.Wait()

	sugar.Infow("scheduler shut down gracefully")
	return nil
}

func (s *Scheduler) connectDatabase() error {
	dsn := s.cfg.Database.DSN
	if dsn == "" {
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			s.cfg.Database.User,
			s.cfg.Database.Password,
			s.cfg.Database.Host,
			s.cfg.Database.Port,
			s.cfg.Database.DBName,
			s.cfg.Database.SSLMode,
		)
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse database config: %w", err)
	}

	poolCfg.MaxConns = 5
	poolCfg.MinConns = 1

	pool, err := pgxpool.NewWithConfig(s.ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(s.ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	s.pgPool = pool
	s.log.Infow("connected to database")
	return nil
}

func (s *Scheduler) connectRabbitMQ() error {
	url := s.cfg.RabbitMQ.URL
	if url == "" {
		url = fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
			s.cfg.RabbitMQ.User,
			s.cfg.RabbitMQ.Password,
			s.cfg.RabbitMQ.Host,
			s.cfg.RabbitMQ.Port,
			s.cfg.RabbitMQ.VHost,
		)
	}

	conn, err := amqp091.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		s.cfg.RabbitMQ.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	s.rmqChan = ch
	s.log.Infow("connected to rabbitmq", "exchange", s.cfg.RabbitMQ.Exchange)
	return nil
}

func (s *Scheduler) registerBuiltinJobs() {
	jobs := map[string]struct {
		schedule string
		fn       func()
		desc     string
	}{
		"cleanup_stale_tasks": {
			schedule: "0 */5 * * * *",
			fn:       s.cleanupStaleTasks,
			desc:     "Clean up stale tasks every 5 minutes",
		},
		"retry_failed_events": {
			schedule: "0 */1 * * * *",
			fn:       s.retryFailedEvents,
			desc:     "Retry failed events every minute",
		},
		"heartbeat": {
			schedule: "0 */30 * * * *",
			fn:       s.sendHeartbeat,
			desc:     "Send system heartbeat every 30 minutes",
		},
		"metrics_snapshot": {
			schedule: "0 */15 * * * *",
			fn:       s.takeMetricsSnapshot,
			desc:     "Take metrics snapshot every 15 minutes",
		},
		"purge_old_events": {
			schedule: "0 0 3 * * *",
			fn:       s.purgeOldEvents,
			desc:     "Purge events older than 90 days at 3 AM daily",
		},
	}

	for name, job := range jobs {
		id, err := s.cron.AddFunc(job.schedule, job.fn)
		if err != nil {
			s.log.Errorw("failed to register builtin job",
				"job", name,
				"error", err,
			)
			continue
		}
		s.jobs[name] = id
		s.log.Infow("registered builtin cron job",
			"job", name,
			"schedule", job.schedule,
			"entry_id", id,
		)
	}
}

func (s *Scheduler) registerConfigJobs() {
	for _, job := range s.cfg.Scheduler.CronExpressions {
		jobFn := s.createConfigJob(job)
		id, err := s.cron.AddFunc(job.Schedule, jobFn)
		if err != nil {
			s.log.Errorw("failed to register config job",
				"job", job.Name,
				"schedule", job.Schedule,
				"error", err,
			)
			continue
		}
		s.jobs[job.Name] = id
		s.log.Infow("registered config cron job",
			"job", job.Name,
			"schedule", job.Schedule,
			"type", job.Type,
			"entry_id", id,
		)
	}
}

func (s *Scheduler) createConfigJob(job config.CronJob) func() {
	return func() {
		s.log.Infow("executing config cron job",
			"job", job.Name,
			"type", job.Type,
			"payload", job.Payload,
		)

		message := amqp091.Publishing{
			ContentType:  "application/json",
			Body:         []byte(job.Payload),
			Timestamp:    time.Now(),
			MessageId:    fmt.Sprintf("%s-%d", job.Name, time.Now().UnixNano()),
			DeliveryMode: amqp091.Persistent,
		}

		routingKey := fmt.Sprintf("scheduler.%s", job.Type)
		if err := s.rmqChan.Publish(
			s.cfg.RabbitMQ.Exchange,
			routingKey,
			true,
			false,
			message,
		); err != nil {
			s.log.Errorw("failed to publish scheduled job",
				"job", job.Name,
				"error", err,
			)
			return
		}

		s.log.Infow("scheduled job dispatched",
			"job", job.Name,
			"routing_key", routingKey,
		)
	}
}

func (s *Scheduler) cleanupStaleTasks() {
	s.log.Infow("running stale task cleanup")
	_, err := s.pgPool.Exec(s.ctx,
		`UPDATE tasks SET status = 'failed', updated_at = NOW()
		 WHERE status = 'running' AND updated_at < NOW() - INTERVAL '1 hour'`)
	if err != nil {
		s.log.Errorw("stale task cleanup failed", "error", err)
	}
}

func (s *Scheduler) retryFailedEvents() {
	s.log.Infow("running failed event retry")
	_, err := s.pgPool.Exec(s.ctx,
		`UPDATE events SET status = 'pending', retry_count = retry_count + 1, updated_at = NOW()
		 WHERE status = 'failed' AND retry_count < 5 AND updated_at < NOW() - INTERVAL '5 minutes'`)
	if err != nil {
		s.log.Errorw("failed event retry failed", "error", err)
	}
}

func (s *Scheduler) sendHeartbeat() {
	s.log.Infow("sending scheduler heartbeat")
	message := amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"type":"scheduler_heartbeat","timestamp":"` + time.Now().UTC().Format(time.RFC3339) + `"}`),
		Timestamp:   time.Now(),
		MessageId:   fmt.Sprintf("hb-%d", time.Now().Unix()),
	}
	if err := s.rmqChan.Publish(
		s.cfg.RabbitMQ.Exchange,
		"system.heartbeat",
		false,
		false,
		message,
	); err != nil {
		s.log.Errorw("heartbeat publish failed", "error", err)
	}
}

func (s *Scheduler) takeMetricsSnapshot() {
	s.log.Infow("taking metrics snapshot")
	var taskCount, eventCount, pipelineCount int

	if err := s.pgPool.QueryRow(s.ctx, `SELECT COUNT(*) FROM tasks`).Scan(&taskCount); err != nil {
		s.log.Errorw("failed to count tasks", "error", err)
	}
	if err := s.pgPool.QueryRow(s.ctx, `SELECT COUNT(*) FROM events`).Scan(&eventCount); err != nil {
		s.log.Errorw("failed to count events", "error", err)
	}
	if err := s.pgPool.QueryRow(s.ctx, `SELECT COUNT(*) FROM pipelines`).Scan(&pipelineCount); err != nil {
		s.log.Errorw("failed to count pipelines", "error", err)
	}

	s.log.Infow("metrics snapshot",
		"pipelines", pipelineCount,
		"events", eventCount,
		"tasks", taskCount,
	)
}

func (s *Scheduler) purgeOldEvents() {
	s.log.Infow("purging events older than 90 days")
	result, err := s.pgPool.Exec(s.ctx,
		`DELETE FROM events WHERE created_at < NOW() - INTERVAL '90 days'`)
	if err != nil {
		s.log.Errorw("event purge failed", "error", err)
		return
	}
	s.log.Infow("events purged", "rows_affected", result.RowsAffected())
}
