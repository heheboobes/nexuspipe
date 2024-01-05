package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	App       AppConfig       `mapstructure:"app" yaml:"app"`
	Database  DatabaseConfig  `mapstructure:"database" yaml:"database"`
	RabbitMQ  RabbitMQConfig  `mapstructure:"rabbitmq" yaml:"rabbitmq"`
	Redis     RedisConfig     `mapstructure:"redis" yaml:"redis"`
	Auth      AuthConfig      `mapstructure:"auth" yaml:"auth"`
	Metrics   MetricsConfig   `mapstructure:"metrics" yaml:"metrics"`
	Worker    WorkerConfig    `mapstructure:"worker" yaml:"worker"`
	Scheduler SchedulerConfig `mapstructure:"scheduler" yaml:"scheduler"`
	Log       LogConfig       `mapstructure:"log" yaml:"log"`
}

type AppConfig struct {
	Name                string        `mapstructure:"name" yaml:"name"`
	Environment         string        `mapstructure:"environment" yaml:"environment"`
	Version             string        `mapstructure:"version" yaml:"version"`
	Host                string        `mapstructure:"host" yaml:"host"`
	Port                int           `mapstructure:"port" yaml:"port"`
	ReadTimeout         time.Duration `mapstructure:"read_timeout" yaml:"read_timeout"`
	WriteTimeout        time.Duration `mapstructure:"write_timeout" yaml:"write_timeout"`
	ShutdownGracePeriod time.Duration `mapstructure:"shutdown_grace_period" yaml:"shutdown_grace_period"`
	CORSOrigins         []string      `mapstructure:"cors_origins" yaml:"cors_origins"`
}

type DatabaseConfig struct {
	Driver          string        `mapstructure:"driver" yaml:"driver"`
	DSN             string        `mapstructure:"dsn" yaml:"dsn"`
	Host            string        `mapstructure:"host" yaml:"host"`
	Port            int           `mapstructure:"port" yaml:"port"`
	User            string        `mapstructure:"user" yaml:"user"`
	Password        string        `mapstructure:"password" yaml:"password"`
	DBName          string        `mapstructure:"dbname" yaml:"dbname"`
	SSLMode         string        `mapstructure:"sslmode" yaml:"sslmode"`
	MaxOpenConns    int           `mapstructure:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime" yaml:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time" yaml:"conn_max_idle_time"`
	MigrationsPath  string        `mapstructure:"migrations_path" yaml:"migrations_path"`
}

type RabbitMQConfig struct {
	URL               string        `mapstructure:"url" yaml:"url"`
	Host              string        `mapstructure:"host" yaml:"host"`
	Port              int           `mapstructure:"port" yaml:"port"`
	User              string        `mapstructure:"user" yaml:"user"`
	Password          string        `mapstructure:"password" yaml:"password"`
	VHost             string        `mapstructure:"vhost" yaml:"vhost"`
	Exchange          string        `mapstructure:"exchange" yaml:"exchange"`
	QueuePrefix       string        `mapstructure:"queue_prefix" yaml:"queue_prefix"`
	PrefetchCount     int           `mapstructure:"prefetch_count" yaml:"prefetch_count"`
	ReconnectInterval time.Duration `mapstructure:"reconnect_interval" yaml:"reconnect_interval"`
	MaxRetries        int           `mapstructure:"max_retries" yaml:"max_retries"`
}

type RedisConfig struct {
	URL          string        `mapstructure:"url" yaml:"url"`
	Host         string        `mapstructure:"host" yaml:"host"`
	Port         int           `mapstructure:"port" yaml:"port"`
	Password     string        `mapstructure:"password" yaml:"password"`
	DB           int           `mapstructure:"db" yaml:"db"`
	PoolSize     int           `mapstructure:"pool_size" yaml:"pool_size"`
	MinIdleConns int           `mapstructure:"min_idle_conns" yaml:"min_idle_conns"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout" yaml:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout" yaml:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout" yaml:"write_timeout"`
}

type AuthConfig struct {
	JWTSecret       string        `mapstructure:"jwt_secret" yaml:"jwt_secret"`
	JWTTTL          time.Duration `mapstructure:"jwt_ttl" yaml:"jwt_ttl"`
	RefreshTTL      time.Duration `mapstructure:"refresh_ttl" yaml:"refresh_ttl"`
	Issuer          string        `mapstructure:"issuer" yaml:"issuer"`
	BcryptCost      int           `mapstructure:"bcrypt_cost" yaml:"bcrypt_cost"`
	RateLimitPerMin int           `mapstructure:"rate_limit_per_min" yaml:"rate_limit_per_min"`
}

type MetricsConfig struct {
	Enabled   bool      `mapstructure:"enabled" yaml:"enabled"`
	Path      string    `mapstructure:"path" yaml:"path"`
	Namespace string    `mapstructure:"namespace" yaml:"namespace"`
	Subsystem string    `mapstructure:"subsystem" yaml:"subsystem"`
	Buckets   []float64 `mapstructure:"buckets" yaml:"buckets"`
}

type WorkerConfig struct {
	Concurrency       int           `mapstructure:"concurrency" yaml:"concurrency"`
	PollInterval      time.Duration `mapstructure:"poll_interval" yaml:"poll_interval"`
	MaxRetries        int           `mapstructure:"max_retries" yaml:"max_retries"`
	RetryBackoff      time.Duration `mapstructure:"retry_backoff" yaml:"retry_backoff"`
	QueueNames        []string      `mapstructure:"queue_names" yaml:"queue_names"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval" yaml:"heartbeat_interval"`
}

type SchedulerConfig struct {
	Enabled         bool      `mapstructure:"enabled" yaml:"enabled"`
	Timezone        string    `mapstructure:"timezone" yaml:"timezone"`
	CronExpressions []CronJob `mapstructure:"cron_expressions" yaml:"cron_expressions"`
}

type CronJob struct {
	Name     string `mapstructure:"name" yaml:"name"`
	Schedule string `mapstructure:"schedule" yaml:"schedule"`
	Type     string `mapstructure:"type" yaml:"type"`
	Payload  string `mapstructure:"payload" yaml:"payload"`
}

type LogConfig struct {
	Level      string `mapstructure:"level" yaml:"level"`
	Format     string `mapstructure:"format" yaml:"format"`
	OutputPath string `mapstructure:"output_path" yaml:"output_path"`
	ErrorPath  string `mapstructure:"error_path" yaml:"error_path"`
	AddSource  bool   `mapstructure:"add_source" yaml:"add_source"`
	Sampling   bool   `mapstructure:"sampling" yaml:"sampling"`
	Encoding   string `mapstructure:"encoding" yaml:"encoding"`
}

func LoadConfig(path string) (*Config, error) {
	v := viper.New()

	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	v.SetEnvPrefix("NEXUSPIPE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	setDefaults(v)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

func LoadConfigFromString(raw string) (*Config, error) {
	v := viper.New()

	v.SetConfigType("yaml")
	v.SetEnvPrefix("NEXUSPIPE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadConfig(strings.NewReader(raw)); err != nil {
		return nil, fmt.Errorf("failed to read config from string: %w", err)
	}

	setDefaults(v)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.name", "nexuspipe")
	v.SetDefault("app.environment", "development")
	v.SetDefault("app.version", "0.1.0")
	v.SetDefault("app.host", "0.0.0.0")
	v.SetDefault("app.port", 8080)
	v.SetDefault("app.read_timeout", "30s")
	v.SetDefault("app.write_timeout", "30s")
	v.SetDefault("app.shutdown_grace_period", "15s")
	v.SetDefault("app.cors_origins", []string{"*"})

	v.SetDefault("database.driver", "postgres")
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "nexuspipe")
	v.SetDefault("database.dbname", "nexuspipe")
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("database.max_open_conns", 25)
	v.SetDefault("database.max_idle_conns", 10)
	v.SetDefault("database.conn_max_lifetime", "5m")
	v.SetDefault("database.conn_max_idle_time", "1m")
	v.SetDefault("database.migrations_path", "migrations")

	v.SetDefault("rabbitmq.host", "localhost")
	v.SetDefault("rabbitmq.port", 5672)
	v.SetDefault("rabbitmq.user", "guest")
	v.SetDefault("rabbitmq.password", "guest")
	v.SetDefault("rabbitmq.vhost", "/")
	v.SetDefault("rabbitmq.exchange", "nexuspipe.events")
	v.SetDefault("rabbitmq.queue_prefix", "nexuspipe")
	v.SetDefault("rabbitmq.prefetch_count", 10)
	v.SetDefault("rabbitmq.reconnect_interval", "5s")
	v.SetDefault("rabbitmq.max_retries", 5)

	v.SetDefault("redis.host", "localhost")
	v.SetDefault("redis.port", 6379)
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 10)
	v.SetDefault("redis.min_idle_conns", 3)
	v.SetDefault("redis.dial_timeout", "5s")
	v.SetDefault("redis.read_timeout", "3s")
	v.SetDefault("redis.write_timeout", "3s")

	v.SetDefault("auth.jwt_ttl", "24h")
	v.SetDefault("auth.refresh_ttl", "720h")
	v.SetDefault("auth.issuer", "nexuspipe")
	v.SetDefault("auth.bcrypt_cost", 12)
	v.SetDefault("auth.rate_limit_per_min", 100)

	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("metrics.namespace", "nexuspipe")
	v.SetDefault("metrics.subsystem", "api")
	v.SetDefault("metrics.buckets", []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0})

	v.SetDefault("worker.concurrency", 10)
	v.SetDefault("worker.poll_interval", "1s")
	v.SetDefault("worker.max_retries", 3)
	v.SetDefault("worker.retry_backoff", "5s")
	v.SetDefault("worker.queue_names", []string{"nexuspipe.tasks.default"})
	v.SetDefault("worker.heartbeat_interval", "30s")

	v.SetDefault("scheduler.enabled", true)
	v.SetDefault("scheduler.timezone", "UTC")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("log.output_path", "stdout")
	v.SetDefault("log.error_path", "stderr")
	v.SetDefault("log.add_source", true)
	v.SetDefault("log.sampling", false)
	v.SetDefault("log.encoding", "console")
}

func (c *Config) Validate() error {
	if c.Database.DSN == "" && c.Database.Host == "" {
		return fmt.Errorf("database connection is not configured")
	}
	if c.RabbitMQ.URL == "" && c.RabbitMQ.Host == "" {
		return fmt.Errorf("rabbitmq connection is not configured")
	}
	if c.App.Port <= 0 || c.App.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.App.Port)
	}
	return nil
}
