package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

type Strategy int

const (
	StrategyFixed Strategy = iota
	StrategyLinear
	StrategyExponential
	StrategyExponentialWithJitter
)

type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Strategy    Strategy
	Jitter      float64
}

type Option func(*Config)

func WithMaxAttempts(n int) Option {
	return func(c *Config) {
		c.MaxAttempts = n
	}
}

func WithBaseDelay(d time.Duration) Option {
	return func(c *Config) {
		c.BaseDelay = d
	}
}

func WithMaxDelay(d time.Duration) Option {
	return func(c *Config) {
		c.MaxDelay = d
	}
}

func WithExponentialBackoff() Option {
	return func(c *Config) {
		c.Strategy = StrategyExponential
	}
}

func WithJitter(j float64) Option {
	return func(c *Config) {
		c.Jitter = j
	}
}

func DefaultConfig() *Config {
	return &Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    30 * time.Second,
		Strategy:    StrategyExponential,
		Jitter:      0.2,
	}
}

type RetryableFunc func(context.Context) error

func Do(ctx context.Context, fn RetryableFunc, opts ...Option) error {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			delay := calculateDelay(cfg, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		if err := fn(ctx); err != nil {
			lastErr = fmt.Errorf("attempt %d/%d failed: %w", attempt+1, cfg.MaxAttempts, err)
			continue
		}
		return nil
	}

	return fmt.Errorf("all %d attempts failed: %w", cfg.MaxAttempts, lastErr)
}

func DoWithData[T any](ctx context.Context, fn func(context.Context) (T, error), opts ...Option) (T, error) {
	var zero T
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			delay := calculateDelay(cfg, attempt)
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(delay):
			}
		}

		result, err := fn(ctx)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d/%d failed: %w", attempt+1, cfg.MaxAttempts, err)
			continue
		}
		return result, nil
	}

	return zero, fmt.Errorf("all %d attempts failed: %w", cfg.MaxAttempts, lastErr)
}

func calculateDelay(cfg *Config, attempt int) time.Duration {
	var delay time.Duration

	switch cfg.Strategy {
	case StrategyFixed:
		delay = cfg.BaseDelay
	case StrategyLinear:
		delay = cfg.BaseDelay * time.Duration(attempt)
	case StrategyExponential:
		delay = cfg.BaseDelay * time.Duration(math.Pow(2, float64(attempt-1)))
	case StrategyExponentialWithJitter:
		delay = cfg.BaseDelay * time.Duration(math.Pow(2, float64(attempt-1)))
		if cfg.Jitter > 0 {
			jitter := time.Duration(float64(delay) * cfg.Jitter * (rand.Float64()*2 - 1))
			delay += jitter
			if delay < 0 {
				delay = cfg.BaseDelay
			}
		}
	}

	if cfg.MaxDelay > 0 && delay > cfg.MaxDelay {
		delay = cfg.MaxDelay
	}

	return delay
}

type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent error: %v", e.Err)
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

func IsPermanent(err error) bool {
	_, ok := err.(*PermanentError)
	return ok
}
