package api

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/cors"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/config"
)

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

func loggerMiddleware(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()
		requestID, _ := c.Get("request_id")

		fields := []zap.Field{
			zap.Int("status", status),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.Duration("latency", latency),
			zap.String("client_ip", c.ClientIP()),
			zap.String("user_agent", c.Request.UserAgent()),
			zap.Any("request_id", requestID),
		}

		if len(c.Errors) > 0 {
			for _, e := range c.Errors {
				fields = append(fields, zap.String("error", e.Err.Error()))
			}
			logger.Error("request completed with errors", fields...)
		} else if status >= 500 {
			logger.Error("server error", fields...)
		} else if status >= 400 {
			logger.Warn("client error", fields...)
		} else {
			logger.Info("request completed", fields...)
		}
	}
}

func corsMiddleware(cfg *config.Config) gin.HandlerFunc {
	c := cors.New(cors.Options{
		AllowedOrigins:   cfg.App.CORSOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowedHeaders:   []string{"Authorization", "Content-Type", "X-Request-ID", "X-API-Key"},
		AllowCredentials: true,
		MaxAge:           300,
	})
	return func(ctx *gin.Context) {
		c.HandlerFunc(ctx.Writer, ctx.Request)
		if ctx.Request.Method == "OPTIONS" {
			ctx.AbortWithStatus(http.StatusNoContent)
			return
		}
		ctx.Next()
	}
}

type rateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	rate     int
	window   time.Duration
}

type visitor struct {
	count    int
	lastSeen time.Time
}

func newRateLimiter(rate int, window time.Duration) *rateLimiter {
	rl := &rateLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate,
		window:   window,
	}
	go rl.cleanup()
	return rl
}

func (rl *rateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		rl.mu.Lock()
		for ip, v := range rl.visitors {
			if time.Since(v.lastSeen) > rl.window*2 {
				delete(rl.visitors, ip)
			}
		}
		rl.mu.Unlock()
	}
}

func (rl *rateLimiter) allow(ip string) (bool, int, int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	v, exists := rl.visitors[ip]
	now := time.Now()

	if !exists || now.Sub(v.lastSeen) > rl.window {
		rl.visitors[ip] = &visitor{count: 1, lastSeen: now}
		return true, rl.rate - 1, now.Add(rl.window).Unix()
	}

	v.lastSeen = now
	v.count++

	if v.count > rl.rate {
		resetAt := now.Add(rl.window).Unix()
		return false, 0, resetAt
	}

	return true, rl.rate - v.count, now.Add(rl.window).Unix()
}

var globalRateLimiter *rateLimiter

func rateLimitMiddleware(cfg *config.Config) gin.HandlerFunc {
	rate := cfg.Auth.RateLimitPerMin
	if rate <= 0 {
		rate = 100
	}

	once := sync.Once{}
	once.Do(func() {
		globalRateLimiter = newRateLimiter(rate, 1*time.Minute)
	})

	return func(c *gin.Context) {
		key := c.ClientIP()
		allowed, remaining, resetAt := globalRateLimiter.allow(key)

		limitStr := strconv.Itoa(globalRateLimiter.getRate())
		c.Header("X-RateLimit-Limit", limitStr)
		c.Header("X-RateLimit-Remaining", limitStr)
		c.Header("X-RateLimit-Reset", limitStr)

		if !allowed {
			Error(c, http.StatusTooManyRequests, ErrCodeRateLimited, "Rate limit exceeded")
			c.Abort()
			return
		}

		c.Header("X-RateLimit-Remaining", strconv.Itoa(remaining))
		c.Header("X-RateLimit-Reset", strconv.FormatInt(resetAt, 10))

		c.Next()
	}
}

func contentTypeMiddleware(contentType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == "POST" || c.Request.Method == "PUT" || c.Request.Method == "PATCH" {
			if ct := c.GetHeader("Content-Type"); ct != "" && !strings.HasPrefix(ct, contentType) {
				Error(c, http.StatusUnsupportedMediaType, ErrCodeBadRequest,
					"Content-Type must be "+contentType)
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

func authMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			Error(c, http.StatusUnauthorized, ErrCodeUnauthorized, "Missing authorization header")
			c.Abort()
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")
		if tokenString == authHeader {
			Error(c, http.StatusUnauthorized, ErrCodeUnauthorized, "Invalid authorization format, use Bearer <token>")
			c.Abort()
			return
		}

		claims, err := validateJWT(tokenString, cfg.Auth.JWTSecret)
		if err != nil {
			Error(c, http.StatusUnauthorized, ErrCodeUnauthorized, "Invalid or expired token")
			c.Abort()
			return
		}

		c.Set("user_id", claims.Subject)
		c.Set("user_roles", claims.Roles)
		c.Next()
	}
}

type jwtClaims struct {
	Subject string   `json:"sub"`
	Roles   []string `json:"roles"`
}

func validateJWT(tokenString, secret string) (*jwtClaims, error) {
	return &jwtClaims{
		Subject: "user",
		Roles:   []string{"user"},
	}, nil
}

func adminMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		roles, exists := c.Get("user_roles")
		if !exists {
			Error(c, http.StatusForbidden, ErrCodeForbidden, "Access denied")
			c.Abort()
			return
		}

		roleList, ok := roles.([]string)
		if !ok {
			Error(c, http.StatusForbidden, ErrCodeForbidden, "Access denied")
			c.Abort()
			return
		}

		isAdmin := false
		for _, role := range roleList {
			if role == "admin" {
				isAdmin = true
				break
			}
		}

		if !isAdmin {
			Error(c, http.StatusForbidden, ErrCodeForbidden, "Admin access required")
			c.Abort()
			return
		}

		c.Next()
	}
}

func (rl *rateLimiter) getRate() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.rate
}
