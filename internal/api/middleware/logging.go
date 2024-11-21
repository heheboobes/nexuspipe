package middleware

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggingConfig struct {
	SkipPaths         []string
	RedactHeaders     []string
	RedactQueryParams []string
	MaxBodyLogLength  int
	RequestIDHeader   string
	EnableLatencyLog  bool
	SlowThreshold     time.Duration
	LogLevel          zapcore.Level
}

type LoggingOption func(*LoggingConfig)

type RequestLog struct {
	Level      string        `json:"level"`
	Timestamp  time.Time     `json:"timestamp"`
	Method     string        `json:"method"`
	Path       string        `json:"path"`
	Query      string        `json:"query,omitempty"`
	Status     int           `json:"status"`
	Latency    time.Duration `json:"latency"`
	IP         string        `json:"ip"`
	UserAgent  string        `json:"user_agent"`
	RequestID  string        `json:"request_id"`
	ContentLen int           `json:"content_length"`
	Err        string        `json:"error,omitempty"`
	BodySize   int           `json:"body_size"`
}

var sensitiveHeaders = []string{
	"authorization",
	"cookie",
	"set-cookie",
	"x-api-key",
	"x-auth-token",
	"proxy-authorization",
}

var sensitiveQueryParams = []string{
	"token",
	"api_key",
	"apikey",
	"secret",
	"password",
	"passwd",
	"auth",
	"access_token",
}

func defaultLoggingConfig() *LoggingConfig {
	return &LoggingConfig{
		SkipPaths:         []string{"/health", "/metrics", "/readyz"},
		RedactHeaders:     sensitiveHeaders,
		RedactQueryParams: sensitiveQueryParams,
		MaxBodyLogLength:  1024,
		RequestIDHeader:   "X-Request-ID",
		EnableLatencyLog:  true,
		SlowThreshold:     5 * time.Second,
		LogLevel:          zapcore.InfoLevel,
	}
}

func WithSkipPaths(paths []string) LoggingOption {
	return func(c *LoggingConfig) {
		c.SkipPaths = append(c.SkipPaths, paths...)
	}
}

func WithRedactHeaders(headers []string) LoggingOption {
	return func(c *LoggingConfig) {
		c.RedactHeaders = append(c.RedactHeaders, headers...)
	}
}

func WithRedactQueryParams(params []string) LoggingOption {
	return func(c *LoggingConfig) {
		c.RedactQueryParams = append(c.RedactQueryParams, params...)
	}
}

func WithSlowThreshold(d time.Duration) LoggingOption {
	return func(c *LoggingConfig) {
		c.SlowThreshold = d
	}
}

func WithRequestIDHeader(h string) LoggingOption {
	return func(c *LoggingConfig) {
		c.RequestIDHeader = h
	}
}

func RequestLogging(opts ...LoggingOption) gin.HandlerFunc {
	cfg := defaultLoggingConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return func(ctx *gin.Context) {
		start := time.Now()
		path := ctx.Request.URL.Path
		query := ctx.Request.URL.RawQuery

		for _, skip := range cfg.SkipPaths {
			if path == skip {
				ctx.Next()
				return
			}
		}

		requestID := ctx.GetHeader(cfg.RequestIDHeader)
		if requestID == "" {
			requestID = uuid.New().String()
		}
		ctx.Set("request_id", requestID)
		ctx.Header(cfg.RequestIDHeader, requestID)

		ctx.Next()

		latency := time.Since(start)
		status := ctx.Writer.Status()
		method := ctx.Request.Method
		clientIP := ctx.ClientIP()
		userAgent := ctx.Request.UserAgent()
		contentLen := ctx.Request.ContentLength
		bodySize := ctx.Writer.Size()

		logger := zap.L()
		if logger == nil {
			return
		}

		fields := []zap.Field{
			zap.String("request_id", requestID),
			zap.String("method", method),
			zap.String("path", path),
			zap.String("query", redactQueryParams(query, cfg.RedactQueryParams)),
			zap.Int("status", status),
			zap.Duration("latency", latency),
			zap.String("client_ip", clientIP),
			zap.String("user_agent", userAgent),
			zap.Int64("content_length", contentLen),
			zap.Int("body_size", bodySize),
		}

		for _, h := range cfg.RedactHeaders {
			if v := ctx.GetHeader(h); v != "" {
				fields = append(fields, zap.String("header_"+strings.ReplaceAll(h, "-", "_"), "REDACTED"))
			}
		}

		if len(ctx.Errors) > 0 {
			errStr := ctx.Errors.String()
			fields = append(fields, zap.String("error", errStr))
		}

		level := cfg.LogLevel
		if status >= 500 {
			level = zapcore.ErrorLevel
		} else if status >= 400 {
			level = zapcore.WarnLevel
		} else if latency > cfg.SlowThreshold && cfg.EnableLatencyLog {
			level = zapcore.WarnLevel
			fields = append(fields, zap.Bool("slow", true))
		}

		if ce := logger.Check(level, "request"); ce != nil {
			ce.Write(fields...)
		}
	}
}

func extractHeaders(ctx *gin.Context, redactList []string) []zap.Field {
	var fields []zap.Field
	for _, h := range redactList {
		if v := ctx.GetHeader(h); v != "" {
			fields = append(fields, zap.String(h, "REDACTED"))
		}
	}
	return fields
}

func redactQueryParams(query string, redactList []string) string {
	if query == "" || len(redactList) == 0 {
		return query
	}

	params := strings.Split(query, "&")
	for i, p := range params {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			for _, r := range redactList {
				if strings.EqualFold(kv[0], r) {
					params[i] = kv[0] + "=REDACTED"
					break
				}
			}
		}
	}
	return strings.Join(params, "&")
}
