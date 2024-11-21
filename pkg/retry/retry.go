package retry

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

type Strategy int

const (
	StrategyLinear      Strategy = 0
	StrategyExponential Strategy = 1
	StrategyFibonacci   Strategy = 2
)

var (
	ErrRetryFailed     = errors.New("all retry attempts failed")
	ErrContextCanceled = errors.New("retry canceled by context")
	ErrCacheMiss       = errors.New("cache miss")
)

type Config struct {
	MaxAttempts  int
	BaseDelay    time.Duration
	MaxDelay     time.Duration
	Strategy     Strategy
	Jitter       time.Duration
	RetryableErr func(error) bool
}

type Result struct {
	Value     interface{}
	Err       error
	Attempts  int
	TotalTime time.Duration
}

type Retryer struct {
	config Config
	cache  *resultCache
}

type cacheEntry struct {
	value     interface{}
	err       error
	expiresAt time.Time
}

type resultCache struct {
	mu       sync.RWMutex
	entries  map[string]*cacheEntry
	ttl      time.Duration
	capacity int
}

func newResultCache(ttl time.Duration, capacity int) *resultCache {
	if capacity <= 0 {
		capacity = 1000
	}
	return &resultCache{
		entries:  make(map[string]*cacheEntry, capacity),
		ttl:      ttl,
		capacity: capacity,
	}
}

func (c *resultCache) Get(key string) (interface{}, error, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil, nil, false
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		delete(c.entries, key)
		return nil, nil, false
	}
	return entry.value, entry.err, true
}

func (c *resultCache) Set(key string, value interface{}, err error, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= c.capacity {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	c.entries[key] = &cacheEntry{
		value:     value,
		err:       err,
		expiresAt: exp,
	}
}

func New(config Config) *Retryer {
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.BaseDelay <= 0 {
		config.BaseDelay = 100 * time.Millisecond
	}
	if config.MaxDelay <= 0 {
		config.MaxDelay = 30 * time.Second
	}
	if config.RetryableErr == nil {
		config.RetryableErr = func(err error) bool { return err != nil }
	}

	return &Retryer{
		config: config,
	}
}

func (r *Retryer) WithCache(ttl time.Duration, capacity int) *Retryer {
	r.cache = newResultCache(ttl, capacity)
	return r
}

func (r *Retryer) Do(ctx context.Context, fn func(context.Context) (interface{}, error)) Result {
	start := time.Now()

	if r.cache != nil {
		key := r.cacheKey(ctx, fn)
		if val, err, ok := r.cache.Get(key); ok {
			return Result{
				Value:     val,
				Err:       err,
				Attempts:  0,
				TotalTime: time.Since(start),
			}
		}
	}

	var lastErr error
	attempts := 0

	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return Result{
				Err:       errors.Join(ErrContextCanceled, ctx.Err()),
				Attempts:  attempts,
				TotalTime: time.Since(start),
			}
		default:
		}

		attempts = attempt
		val, err := fn(ctx)
		if err == nil {
			if r.cache != nil {
				key := r.cacheKey(ctx, fn)
				r.cache.Set(key, val, nil, r.cache.ttl)
			}
			return Result{
				Value:     val,
				Err:       nil,
				Attempts:  attempts,
				TotalTime: time.Since(start),
			}
		}

		lastErr = err

		if !r.config.RetryableErr(err) {
			break
		}

		if attempt < r.config.MaxAttempts {
			delay := r.delay(attempt)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return Result{
					Err:       errors.Join(ErrContextCanceled, ctx.Err()),
					Attempts:  attempts,
					TotalTime: time.Since(start),
				}
			case <-timer.C:
			}
		}
	}

	result := Result{
		Err:       errors.Join(ErrRetryFailed, lastErr),
		Attempts:  attempts,
		TotalTime: time.Since(start),
	}

	if r.cache != nil {
		key := r.cacheKey(ctx, fn)
		r.cache.Set(key, nil, result.Err, r.cache.ttl)
	}

	return result
}

func (r *Retryer) delay(attempt int) time.Duration {
	var d time.Duration

	switch r.config.Strategy {
	case StrategyLinear:
		d = time.Duration(attempt) * r.config.BaseDelay
	case StrategyExponential:
		mult := math.Pow(2, float64(attempt-1))
		d = time.Duration(mult) * r.config.BaseDelay
	case StrategyFibonacci:
		d = fibonacciDelay(attempt, r.config.BaseDelay)
	default:
		d = time.Duration(attempt) * r.config.BaseDelay
	}

	if r.config.MaxDelay > 0 && d > r.config.MaxDelay {
		d = r.config.MaxDelay
	}

	if r.config.Jitter > 0 {
		j := time.Duration(int64(r.config.Jitter) * int64(attempt) % int64(r.config.Jitter+1))
		d += j
	}

	return d
}

func fibonacciDelay(attempt int, base time.Duration) time.Duration {
	a, b := 1, 1
	for i := 2; i < attempt; i++ {
		a, b = b, a+b
	}
	return time.Duration(b) * base
}

func (r *Retryer) cacheKey(ctx context.Context, fn func(context.Context) (interface{}, error)) string {
	return ""
}

func ResetCache(r *Retryer) {
	if r.cache != nil {
		r.cache.mu.Lock()
		r.cache.entries = make(map[string]*cacheEntry, r.cache.capacity)
		r.cache.mu.Unlock()
	}
}
