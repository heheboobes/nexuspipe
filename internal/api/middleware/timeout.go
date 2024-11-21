package middleware

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type RouteTimeoutConfig struct {
	DefaultTimeout time.Duration
	RouteTimeouts  map[string]time.Duration
	ErrorResponse  func(ctx *gin.Context)
	MetricsFn      func(path string, duration time.Duration)
}

type TimeoutOption func(*RouteTimeoutConfig)

func defaultTimeoutResponse(ctx *gin.Context) {
	ctx.AbortWithStatusJSON(http.StatusGatewayTimeout, gin.H{
		"error":   "request timeout",
		"message": "the request took too long to process",
	})
}

func WithDefaultTimeout(d time.Duration) TimeoutOption {
	return func(c *RouteTimeoutConfig) {
		c.DefaultTimeout = d
	}
}

func WithRouteTimeout(path string, d time.Duration) TimeoutOption {
	return func(c *RouteTimeoutConfig) {
		if c.RouteTimeouts == nil {
			c.RouteTimeouts = make(map[string]time.Duration)
		}
		c.RouteTimeouts[path] = d
	}
}

func WithTimeoutMetrics(fn func(path string, duration time.Duration)) TimeoutOption {
	return func(c *RouteTimeoutConfig) {
		c.MetricsFn = fn
	}
}

func WithTimeoutResponse(fn func(ctx *gin.Context)) TimeoutOption {
	return func(c *RouteTimeoutConfig) {
		c.ErrorResponse = fn
	}
}

func Timeout(opts ...TimeoutOption) gin.HandlerFunc {
	cfg := RouteTimeoutConfig{
		DefaultTimeout: 30 * time.Second,
		ErrorResponse:  defaultTimeoutResponse,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return func(ctx *gin.Context) {
		timeout := cfg.resolveTimeout(ctx.FullPath())

		deadline := time.Now().Add(timeout)
		cancelCtx, cancel := context.WithDeadline(ctx.Request.Context(), deadline)

		ctx.Request = ctx.Request.WithContext(cancelCtx)

		done := make(chan struct{})
		panicChan := make(chan interface{}, 1)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicChan <- r
				}
			}()
			ctx.Next()
			close(done)
		}()

		select {
		case <-done:
			cancel()
			return
		case <-ctx.Request.Context().Done():
			cancel()
			err := ctx.Request.Context().Err()
			if err == context.DeadlineExceeded {
				if cfg.MetricsFn != nil {
					cfg.MetricsFn(ctx.FullPath(), timeout)
				}
				logTimeout(ctx, timeout)
				cfg.ErrorResponse(ctx)
			}
			return
		case p := <-panicChan:
			cancel()
			panic(p)
		}
	}
}

func (c *RouteTimeoutConfig) resolveTimeout(path string) time.Duration {
	if c.RouteTimeouts != nil {
		if d, ok := c.RouteTimeouts[path]; ok {
			return d
		}
	}
	return c.DefaultTimeout
}

func logTimeout(ctx *gin.Context, timeout time.Duration) {
	logger := zap.L()
	if logger == nil {
		return
	}

	logger.Warn("request timeout",
		zap.String("method", ctx.Request.Method),
		zap.String("path", ctx.FullPath()),
		zap.String("remote", ctx.ClientIP()),
		zap.Duration("timeout", timeout),
		zap.String("request_id", ctx.GetString("request_id")),
	)
}

type TimeoutTracker struct {
	mu       sync.Mutex
	timeouts map[string]int64
}

func NewTimeoutTracker() *TimeoutTracker {
	return &TimeoutTracker{
		timeouts: make(map[string]int64),
	}
}

func (t *TimeoutTracker) Record(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timeouts[path]++
}

func (t *TimeoutTracker) Snapshot() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	snap := make(map[string]int64, len(t.timeouts))
	for k, v := range t.timeouts {
		snap[k] = v
	}
	return snap
}
