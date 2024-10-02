package config

import (
	"testing"
	"time"
)

func TestLoadConfigFromString(t *testing.T) {
	yaml := `
app:
  name: test-app
  port: 9090
database:
  host: db.example.com
  port: 5432
  user: testuser
  dbname: testdb
rabbitmq:
  host: mq.example.com
  port: 5672
`
	cfg, err := LoadConfigFromString(yaml)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.App.Name != "test-app" {
		t.Errorf("expected App.Name 'test-app', got %q", cfg.App.Name)
	}
	if cfg.App.Port != 9090 {
		t.Errorf("expected App.Port 9090, got %d", cfg.App.Port)
	}
	if cfg.Database.Host != "db.example.com" {
		t.Errorf("expected Database.Host 'db.example.com', got %q", cfg.Database.Host)
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("expected Database.Port 5432, got %d", cfg.Database.Port)
	}
	if cfg.Database.User != "testuser" {
		t.Errorf("expected Database.User 'testuser', got %q", cfg.Database.User)
	}
	if cfg.Database.DBName != "testdb" {
		t.Errorf("expected Database.DBName 'testdb', got %q", cfg.Database.DBName)
	}
	if cfg.RabbitMQ.Host != "mq.example.com" {
		t.Errorf("expected RabbitMQ.Host 'mq.example.com', got %q", cfg.RabbitMQ.Host)
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	_, err := LoadConfigFromString(`invalid: yaml: [bad`)
	if err == nil {
		t.Fatal("expected error loading invalid YAML, got nil")
	}
}

func TestLoadConfigEmptyString(t *testing.T) {
	_, err := LoadConfigFromString("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateConfigMissingDB(t *testing.T) {
	cfg := &Config{
		App: AppConfig{Port: 8080},
		RabbitMQ: RabbitMQConfig{
			URL: "amqp://localhost",
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for missing database config")
	}
}

func TestValidateConfigMissingRabbitMQ(t *testing.T) {
	cfg := &Config{
		Database: DatabaseConfig{
			Host: "localhost",
			Port: 5432,
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for missing rabbitmq config")
	}
}

func TestValidateConfigInvalidPortLow(t *testing.T) {
	cfg := &Config{
		App: AppConfig{Port: -1},
		Database: DatabaseConfig{
			DSN: "postgres://localhost",
		},
		RabbitMQ: RabbitMQConfig{
			URL: "amqp://localhost",
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for invalid port")
	}
}

func TestValidateConfigInvalidPortHigh(t *testing.T) {
	cfg := &Config{
		App: AppConfig{Port: 65536},
		Database: DatabaseConfig{
			DSN: "postgres://localhost",
		},
		RabbitMQ: RabbitMQConfig{
			URL: "amqp://localhost",
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for invalid port")
	}
}

func TestValidateConfigValid(t *testing.T) {
	cfg := &Config{
		App: AppConfig{Port: 8080},
		Database: DatabaseConfig{
			DSN: "postgres://localhost",
		},
		RabbitMQ: RabbitMQConfig{
			URL: "amqp://localhost",
		},
	}
	err := cfg.Validate()
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestDefaultsApplied(t *testing.T) {
	yaml := `
app:
  name: test
database:
  dsn: postgres://localhost
rabbitmq:
  url: amqp://localhost
`
	cfg, err := LoadConfigFromString(yaml)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.App.Environment != "development" {
		t.Errorf("expected default environment 'development', got %q", cfg.App.Environment)
	}
	if cfg.App.Version != "0.1.0" {
		t.Errorf("expected default version '0.1.0', got %q", cfg.App.Version)
	}
	if cfg.App.Host != "0.0.0.0" {
		t.Errorf("expected default host '0.0.0.0', got %q", cfg.App.Host)
	}
	if cfg.Database.Driver != "postgres" {
		t.Errorf("expected default driver 'postgres', got %q", cfg.Database.Driver)
	}
	if cfg.Database.SSLMode != "disable" {
		t.Errorf("expected default sslmode 'disable', got %q", cfg.Database.SSLMode)
	}
	if cfg.RabbitMQ.User != "guest" {
		t.Errorf("expected default rabbitmq user 'guest', got %q", cfg.RabbitMQ.User)
	}
	if cfg.RabbitMQ.VHost != "/" {
		t.Errorf("expected default vhost '/', got %q", cfg.RabbitMQ.VHost)
	}
	if cfg.Redis.Host != "localhost" {
		t.Errorf("expected default redis host 'localhost', got %q", cfg.Redis.Host)
	}
	if cfg.Redis.Port != 6379 {
		t.Errorf("expected default redis port 6379, got %d", cfg.Redis.Port)
	}
	if cfg.Metrics.Enabled != true {
		t.Errorf("expected metrics enabled by default")
	}
	if cfg.Worker.Concurrency != 10 {
		t.Errorf("expected default worker concurrency 10, got %d", cfg.Worker.Concurrency)
	}
	if cfg.Scheduler.Enabled != true {
		t.Errorf("expected scheduler enabled by default")
	}
	if cfg.PipelineEngine.MaxConcurrency != 50 {
		t.Errorf("expected default max concurrency 50, got %d", cfg.PipelineEngine.MaxConcurrency)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("expected default log level 'info', got %q", cfg.Log.Level)
	}
}

func TestDefaultsOverridden(t *testing.T) {
	yaml := `
app:
  environment: staging
  version: 2.0.0
  read_timeout: 60s
database:
  dsn: postgres://prod
  max_open_conns: 100
rabbitmq:
  url: amqp://prod
  prefetch_count: 50
log:
  level: debug
`
	cfg, err := LoadConfigFromString(yaml)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.App.Environment != "staging" {
		t.Errorf("expected environment 'staging', got %q", cfg.App.Environment)
	}
	if cfg.App.Version != "2.0.0" {
		t.Errorf("expected version '2.0.0', got %q", cfg.App.Version)
	}
	if cfg.App.ReadTimeout != 60*time.Second {
		t.Errorf("expected read_timeout 60s, got %v", cfg.App.ReadTimeout)
	}
	if cfg.Database.MaxOpenConns != 100 {
		t.Errorf("expected max_open_conns 100, got %d", cfg.Database.MaxOpenConns)
	}
	if cfg.RabbitMQ.PrefetchCount != 50 {
		t.Errorf("expected prefetch_count 50, got %d", cfg.RabbitMQ.PrefetchCount)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("expected log level 'debug', got %q", cfg.Log.Level)
	}
}
