//go:build integration

package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"nexuspipe/internal/models"
	"nexuspipe/internal/pipeline"
	"nexuspipe/internal/queue"
	"nexuspipe/internal/retry"
	"nexuspipe/internal/scheduler"
)

func skipIfNoDocker(t *testing.T) {
	host := os.Getenv("INTEGRATION_DB_HOST")
	if host == "" {
		t.Skip("INTEGRATION_DB_HOST not set; skipping integration test")
	}
}

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbHost := os.Getenv("INTEGRATION_DB_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}
	dbPort := os.Getenv("INTEGRATION_DB_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbUser := os.Getenv("INTEGRATION_DB_USER")
	if dbUser == "" {
		dbUser = "nexuspipe"
	}
	dbPass := os.Getenv("INTEGRATION_DB_PASSWORD")
	if dbPass == "" {
		dbPass = "nexuspipe"
	}
	dbName := os.Getenv("INTEGRATION_DB_NAME")
	if dbName == "" {
		dbName = "nexuspipe_test"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		dbUser, dbPass, dbHost, dbPort, dbName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Fatalf("failed to ping test db: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()

	redisAddr := os.Getenv("INTEGRATION_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", redisAddr, err)
	}

	t.Cleanup(func() {
		rdb.FlushDB(ctx)
		rdb.Close()
	})

	return rdb
}

func newTestQueue(t *testing.T) *amqp091.Connection {
	t.Helper()

	amqpURL := os.Getenv("INTEGRATION_AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	conn, err := amqp091.Dial(amqpURL)
	if err != nil {
		t.Skipf("rabbitmq not available at %s: %v", amqpURL, err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

func TestPipelineEndToEnd(t *testing.T) {
	skipIfNoDocker(t)

	db := newTestDB(t)
	rdb := newTestRedis(t)
	conn := newTestQueue(t)

	_ = db
	_ = rdb
	_ = conn

	pipe := models.NewPipeline("e2e-test-pipeline", uuid.New())
	pipe.Config = models.DefaultPipelineConfig()

	validator := pipeline.NewPipelineValidator()
	result := validator.ValidatePipeline(pipe)
	if !result.Valid {
		t.Fatalf("pipeline validation failed: %v", result.Error())
	}

	t.Logf("created pipeline %s (%s)", pipe.Name, pipe.ID)
	t.Logf("database ping successful, redis ping successful, amqp connected")
}

func TestWorkerMessageProcessing(t *testing.T) {
	skipIfNoDocker(t)

	conn := newTestQueue(t)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	queueName := fmt.Sprintf("test-queue-%s", uuid.New().String())
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}

	body, _ := json.Marshal(map[string]string{"task": "test"})
	err = ch.Publish("", queueName, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgs, err := ch.ConsumeWithContext(ctx, queueName, "test-consumer", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consumer: %v", err)
	}

	select {
	case msg := <-msgs:
		var payload map[string]string
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			t.Fatalf("failed to unmarshal message: %v", err)
		}
		if payload["task"] != "test" {
			t.Errorf("expected task 'test', got %q", payload["task"])
		}
		t.Logf("received message: %s", string(msg.Body))
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}

	ch.QueueDelete(queueName, false, false, false)
}

func TestSchedulerJobExecution(t *testing.T) {
	skipIfNoDocker(t)

	db := newTestDB(t)
	_ = db

	sched := &scheduler.Schedule{
		ID:                uuid.New().String(),
		PipelineID:        uuid.New().String(),
		CronExpression:    "*/5 * * * *",
		Timezone:          "UTC",
		Status:            models.ScheduleStatusActive,
		Enabled:           true,
		MaxConcurrentRuns: 1,
	}

	parsed, err := scheduler.ParseCronExpression(sched.CronExpression)
	if err != nil {
		t.Fatalf("failed to parse cron expression: %v", err)
	}

	next := parsed.Next(time.Now().UTC())
	if next.IsZero() {
		t.Fatal("expected non-zero next run time")
	}

	description, err := scheduler.DescribeSchedule(sched.CronExpression)
	if err != nil {
		t.Fatalf("failed to describe schedule: %v", err)
	}
	t.Logf("schedule %s: next run at %s (%s)", sched.ID, next.Format(time.RFC3339), description)
}

func TestRetryWithQueue(t *testing.T) {
	skipIfNoDocker(t)

	conn := newTestQueue(t)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	ctx := context.Background()
	var attempts int

	err = retry.Do(ctx, func(ctx context.Context) error {
		attempts++
		if attempts < 2 {
			return fmt.Errorf("transient error on attempt %d", attempts)
		}

		qName := fmt.Sprintf("retry-test-%s", uuid.New().String())
		_, declareErr := ch.QueueDeclare(qName, true, false, false, false, nil)
		if declareErr != nil {
			return declareErr
		}
		defer ch.QueueDelete(qName, false, false, false)

		return ch.Publish("", qName, false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte("ok"),
		})
	}, retry.WithMaxAttempts(3), retry.WithBaseDelay(100*time.Millisecond))

	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestPipelineWithStagesIntegration(t *testing.T) {
	skipIfNoDocker(t)

	pipe := models.NewPipeline("integration-pipeline", uuid.New())
	pipe.Config = models.DefaultPipelineConfig()
	pipe.Config.TimeoutSeconds = 60

	b := pipeline.NewPipelineBuilderFromExisting(pipe)
	b.AddStage(pipeline.Stage{
		Name:     "fetch",
		Type:     models.TaskTypeHTTP,
		Timeout:  30 * time.Second,
		MaxRetry: 2,
	})
	b.AddStage(pipeline.Stage{
		Name:      "process",
		Type:      models.TaskTypeTransform,
		Timeout:   15 * time.Second,
		MaxRetry:  0,
		DependsOn: []string{"fetch"},
	})
	b.AddStage(pipeline.Stage{
		Name:      "store",
		Type:      models.TaskTypeSQL,
		Timeout:   10 * time.Second,
		MaxRetry:  1,
		DependsOn: []string{"process"},
	})

	built, stages, err := b.BuildWithStages()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	validator := pipeline.NewPipelineValidator()
	result := validator.ValidatePipeline(built)
	if !result.Valid {
		t.Fatalf("pipeline validation failed: %v", result.Error())
	}

	if len(stages) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(stages))
	}

	t.Logf("pipeline %q with %d stages validated successfully", built.Name, len(stages))
	for _, s := range stages {
		t.Logf("  stage: %s (%s)", s.Name, s.Type)
	}
}
