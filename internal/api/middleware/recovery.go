package middleware

import (
	"bytes"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type RecoveryConfig struct {
	EnableStackPrint bool
	StackSkip        int
	ErrorResponse    func(ctx *gin.Context, err interface{})
	LogPanic         func(ctx *gin.Context, err interface{}, stack []byte)
	MetricsCallback  func(ctx *gin.Context, err interface{})
}

type RecoveryOption func(*RecoveryConfig)

func defaultErrorResponse(ctx *gin.Context, err interface{}) {
	ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
		"error":   "internal server error",
		"message": "an unexpected error occurred",
	})
}

func defaultLogPanic(ctx *gin.Context, err interface{}, stack []byte) {
	logger := zap.L()
	if logger == nil {
		return
	}

	fields := []zap.Field{
		zap.String("panic", fmt.Sprintf("%v", err)),
		zap.String("method", ctx.Request.Method),
		zap.String("path", ctx.Request.URL.Path),
		zap.String("query", ctx.Request.URL.RawQuery),
		zap.String("remote", ctx.ClientIP()),
		zap.String("user_agent", ctx.Request.UserAgent()),
		zap.String("request_id", ctx.GetString("request_id")),
		zap.Time("timestamp", time.Now().UTC()),
	}

	if requestID := ctx.Writer.Header().Get("X-Request-ID"); requestID != "" {
		fields = append(fields, zap.String("request_id", requestID))
	}

	logger.Error("panic recovered", fields...)
}

func WithStackPrint(enabled bool) RecoveryOption {
	return func(c *RecoveryConfig) {
		c.EnableStackPrint = enabled
	}
}

func WithErrorResponse(fn func(ctx *gin.Context, err interface{})) RecoveryOption {
	return func(c *RecoveryConfig) {
		c.ErrorResponse = fn
	}
}

func WithMetricsCallback(fn func(ctx *gin.Context, err interface{})) RecoveryOption {
	return func(c *RecoveryConfig) {
		c.MetricsCallback = fn
	}
}

func WithCustomLogger(fn func(ctx *gin.Context, err interface{}, stack []byte)) RecoveryOption {
	return func(c *RecoveryConfig) {
		c.LogPanic = fn
	}
}

func Recovery(opts ...RecoveryOption) gin.HandlerFunc {
	cfg := RecoveryConfig{
		EnableStackPrint: true,
		StackSkip:        4,
		ErrorResponse:    defaultErrorResponse,
		LogPanic:         defaultLogPanic,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return func(ctx *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				var stack []byte
				if cfg.EnableStackPrint {
					buf := make([]byte, 1<<16)
					n := runtime.Stack(buf, false)
					stack = buf[:n]

					lines := bytes.Split(stack, []byte("\n"))
					for i, line := range lines {
						if bytes.Contains(line, []byte("created by")) ||
							bytes.Contains(line, []byte("nexuspipe/internal")) {
							lines = lines[:i+2]
							break
						}
					}
					stack = bytes.Join(lines, []byte("\n"))
				}

				if cfg.LogPanic != nil {
					cfg.LogPanic(ctx, r, stack)
				}

				if cfg.MetricsCallback != nil {
					cfg.MetricsCallback(ctx, r)
				}

				ctx.Header("X-Panic-Recovered", "true")
				cfg.ErrorResponse(ctx, r)
			}
		}()

		ctx.Next()
	}
}

func RecoveryWithSkip(skip int) gin.HandlerFunc {
	return Recovery(WithStackPrint(true), func(c *RecoveryConfig) {
		c.StackSkip = skip
	})
}

func PanicCounter() func(ctx *gin.Context, err interface{}) {
	var count uint64
	return func(ctx *gin.Context, err interface{}) {
		count++
	}
}
