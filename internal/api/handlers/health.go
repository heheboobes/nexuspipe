package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type HealthHandler struct {
	db     *pgxpool.Pool
	amqp   *amqp091.Connection
	logger *zap.Logger
}

func NewHealthHandler(db *pgxpool.Pool, amqp *amqp091.Connection, logger *zap.Logger) *HealthHandler {
	return &HealthHandler{
		db:     db,
		amqp:   amqp,
		logger: logger,
	}
}

type healthStatus struct {
	Status    string            `json:"status"`
	Service   string            `json:"service"`
	Version   string            `json:"version"`
	Timestamp string            `json:"timestamp"`
	Uptime    string            `json:"uptime"`
	Checks    map[string]string `json:"checks,omitempty"`
}

type readinessStatus struct {
	Status  string            `json:"status"`
	Checks  map[string]string `json:"checks"`
	Healthy bool              `json:"healthy"`
}

func (h *HealthHandler) Health(c *gin.Context) {
	uptime := time.Since(serverStartTime).Truncate(time.Second).String()

	status := healthStatus{
		Status:    "ok",
		Service:   "nexuspipe-api",
		Version:   "0.1.0",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Uptime:    uptime,
	}

	success(c, status)
}

func (h *HealthHandler) Readiness(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	checks := make(map[string]string)
	healthy := true

	if err := h.db.Ping(ctx); err != nil {
		checks["database"] = "unhealthy: " + err.Error()
		healthy = false
	} else {
		checks["database"] = "healthy"
	}

	if h.amqp != nil && !h.amqp.IsClosed() {
		ch, err := h.amqp.Channel()
		if err != nil {
			checks["queue"] = "unhealthy: " + err.Error()
			healthy = false
		} else {
			checks["queue"] = "healthy"
			ch.Close()
		}
	} else {
		checks["queue"] = "not_configured"
	}

	status := "ready"
	httpStatus := http.StatusOK
	if !healthy {
		status = "not_ready"
		httpStatus = http.StatusServiceUnavailable
	}

	c.JSON(httpStatus, readinessStatus{
		Status:  status,
		Checks:  checks,
		Healthy: healthy,
	})
}

func (h *HealthHandler) Liveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "alive",
		"service": "nexuspipe-api",
	})
}

func (h *HealthHandler) Detailed(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	checks := make(map[string]string)
	overall := "healthy"

	dbErr := h.db.Ping(ctx)
	if dbErr != nil {
		checks["database"] = "unhealthy: " + dbErr.Error()
		overall = "degraded"
	} else {
		checks["database"] = "healthy"
	}

	if h.amqp != nil {
		if h.amqp.IsClosed() {
			checks["rabbitmq"] = "disconnected"
			if overall == "healthy" {
				overall = "degraded"
			}
		} else {
			checks["rabbitmq"] = "connected"
		}
	}

	uptime := time.Since(serverStartTime).Truncate(time.Second).String()

	c.JSON(http.StatusOK, gin.H{
		"status":    overall,
		"service":   "nexuspipe-api",
		"uptime":    uptime,
		"checks":    checks,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
