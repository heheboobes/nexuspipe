package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/queue"
)

type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

type ComponentHealth struct {
	Name      string        `json:"name"`
	Status    HealthStatus  `json:"status"`
	Message   string        `json:"message,omitempty"`
	LastCheck time.Time     `json:"last_check"`
	Latency   time.Duration `json:"latency_ms"`
}

type WorkerHealthReport struct {
	WorkerID      string            `json:"worker_id"`
	Hostname      string            `json:"hostname"`
	Status        HealthStatus      `json:"status"`
	Uptime        time.Duration     `json:"uptime_seconds"`
	StartedAt     time.Time         `json:"started_at"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	Components    []ComponentHealth `json:"components"`
	Metrics       HealthMetrics     `json:"metrics"`
	System        SystemInfo        `json:"system"`
}

type HealthMetrics struct {
	TasksProcessed int64   `json:"tasks_processed"`
	TasksSucceeded int64   `json:"tasks_succeeded"`
	TasksFailed    int64   `json:"tasks_failed"`
	TasksRetried   int64   `json:"tasks_retried"`
	ActiveWorkers  int     `json:"active_workers"`
	IdleWorkers    int     `json:"idle_workers"`
	QueueDepth     int     `json:"queue_depth"`
	QueueCapacity  int     `json:"queue_capacity"`
	AvgProcessTime float64 `json:"avg_process_time_ms"`
	ErrorRate      float64 `json:"error_rate"`
	PanicCount     int64   `json:"panic_count"`
}

type SystemInfo struct {
	GoVersion     string  `json:"go_version"`
	NumCPU        int     `json:"num_cpu"`
	NumGoroutine  int     `json:"num_goroutine"`
	MemoryAllocMB float64 `json:"memory_alloc_mb"`
	MemoryTotalMB float64 `json:"memory_total_mb"`
	MemorySysMB   float64 `json:"memory_sys_mb"`
	GCStats       string  `json:"gc_stats,omitempty"`
}

type HealthReporter struct {
	workerID   string
	hostname   string
	startedAt  time.Time
	pool       *WorkerPool
	rmq        *queue.RabbitMQ
	logger     *zap.Logger
	mu         sync.RWMutex
	lastReport *WorkerHealthReport
	heartbeats int64
}

func NewHealthReporter(
	workerID string,
	hostname string,
	pool *WorkerPool,
	rmq *queue.RabbitMQ,
	logger *zap.Logger,
) *HealthReporter {
	return &HealthReporter{
		workerID:  workerID,
		hostname:  hostname,
		startedAt: time.Now().UTC(),
		pool:      pool,
		rmq:       rmq,
		logger:    logger.With(zap.String("component", "health_reporter")),
	}
}

func (hr *HealthReporter) ReportHealth(ctx context.Context) (*WorkerHealthReport, error) {
	start := time.Now().UTC()

	components := hr.checkComponents(ctx)
	status := hr.aggregateStatus(components)
	metrics := hr.collectMetrics()
	sysInfo := hr.collectSystemInfo()

	report := &WorkerHealthReport{
		WorkerID:      hr.workerID,
		Hostname:      hr.hostname,
		Status:        status,
		Uptime:        time.Since(hr.startedAt),
		StartedAt:     hr.startedAt,
		LastHeartbeat: time.Now().UTC(),
		Components:    components,
		Metrics:       metrics,
		System:        sysInfo,
	}

	atomic.AddInt64(&hr.heartbeats, 1)

	hr.mu.Lock()
	hr.lastReport = report
	hr.mu.Unlock()

	hr.logger.Debug("health report generated",
		zap.String("status", string(status)),
		zap.Int("components", len(components)),
		zap.Duration("took", time.Since(start)),
	)

	return report, nil
}

func (hr *HealthReporter) StartHeartbeat(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	hr.logger.Info("starting heartbeat reporting",
		zap.Duration("interval", interval),
	)

	hr.ReportHealth(ctx)

	for {
		select {
		case <-ctx.Done():
			hr.logger.Info("heartbeat reporting stopped")
			return
		case <-ticker.C:
			report, err := hr.ReportHealth(ctx)
			if err != nil {
				hr.logger.Error("heartbeat report failed",
					zap.Error(err),
				)
				continue
			}

			hr.logger.Debug("heartbeat",
				zap.String("status", string(report.Status)),
				zap.Int64("tasks_processed", report.Metrics.TasksProcessed),
				zap.Int("active_workers", report.Metrics.ActiveWorkers),
			)
		}
	}
}

func (hr *HealthReporter) LastReport() *WorkerHealthReport {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.lastReport
}

func (hr *HealthReporter) MarshalReport(report *WorkerHealthReport) ([]byte, error) {
	data, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("marshal health report: %w", err)
	}
	return data, nil
}

func (hr *HealthReporter) checkComponents(ctx context.Context) []ComponentHealth {
	components := make([]ComponentHealth, 0)

	rmqHealth := hr.checkRabbitMQ(ctx)
	components = append(components, rmqHealth)

	poolMetrics := hr.pool.PoolMetrics()
	poolStatus := HealthStatusHealthy
	poolMsg := ""
	if poolMetrics.TotalWorkers == 0 {
		poolStatus = HealthStatusUnhealthy
		poolMsg = "no workers running"
	} else if float64(poolMetrics.BusyWorkers)/float64(poolMetrics.TotalWorkers) > 0.95 {
		poolStatus = HealthStatusDegraded
		poolMsg = "worker pool near capacity"
	}
	components = append(components, ComponentHealth{
		Name:      "worker_pool",
		Status:    poolStatus,
		Message:   poolMsg,
		LastCheck: time.Now().UTC(),
	})

	if !hr.pool.IsRunning() {
		components = append(components, ComponentHealth{
			Name:      "pool_runner",
			Status:    HealthStatusUnhealthy,
			Message:   "worker pool is not running",
			LastCheck: time.Now().UTC(),
		})
	}

	return components
}

func (hr *HealthReporter) checkRabbitMQ(ctx context.Context) ComponentHealth {
	start := time.Now().UTC()
	err := hr.rmq.HealthCheck(ctx)
	latency := time.Since(start)

	status := HealthStatusHealthy
	msg := ""
	if err != nil {
		status = HealthStatusUnhealthy
		msg = fmt.Sprintf("rabbitmq health check failed: %v", err)
	}

	return ComponentHealth{
		Name:      "rabbitmq",
		Status:    status,
		Message:   msg,
		LastCheck: time.Now().UTC(),
		Latency:   latency,
	}
}

func (hr *HealthReporter) aggregateStatus(components []ComponentHealth) HealthStatus {
	hasDegraded := false

	for _, c := range components {
		if c.Status == HealthStatusUnhealthy {
			return HealthStatusUnhealthy
		}
		if c.Status == HealthStatusDegraded {
			hasDegraded = true
		}
	}

	if hasDegraded {
		return HealthStatusDegraded
	}

	return HealthStatusHealthy
}

func (hr *HealthReporter) collectMetrics() HealthMetrics {
	poolMetrics := hr.pool.PoolMetrics()
	workerMetrics := hr.pool.Metrics()

	var totalProcessed, totalSucceeded, totalFailed, totalRetried, totalPanics int64
	var totalDuration float64

	for _, m := range workerMetrics {
		totalProcessed += m.TasksProcessed
		totalSucceeded += m.TasksSucceeded
		totalFailed += m.TasksFailed
		totalRetried += m.TasksRetried
		totalPanics += m.Panics
		totalDuration += m.AvgDuration.Seconds() * 1000
	}

	var avgProcessTime float64
	if totalProcessed > 0 {
		avgProcessTime = totalDuration / float64(len(workerMetrics))
	}

	errorRate := 0.0
	if totalProcessed > 0 {
		errorRate = float64(totalFailed) / float64(totalProcessed) * 100
	}

	return HealthMetrics{
		TasksProcessed: totalProcessed,
		TasksSucceeded: totalSucceeded,
		TasksFailed:    totalFailed,
		TasksRetried:   totalRetried,
		ActiveWorkers:  poolMetrics.BusyWorkers,
		IdleWorkers:    poolMetrics.IdleWorkers,
		QueueDepth:     poolMetrics.QueueDepth,
		QueueCapacity:  poolMetrics.QueueCapacity,
		AvgProcessTime: avgProcessTime,
		ErrorRate:      errorRate,
		PanicCount:     totalPanics,
	}
}

func (hr *HealthReporter) collectSystemInfo() SystemInfo {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return SystemInfo{
		GoVersion:     runtime.Version(),
		NumCPU:        runtime.NumCPU(),
		NumGoroutine:  runtime.NumGoroutine(),
		MemoryAllocMB: float64(m.Alloc) / 1024 / 1024,
		MemoryTotalMB: float64(m.TotalAlloc) / 1024 / 1024,
		MemorySysMB:   float64(m.Sys) / 1024 / 1024,
	}
}

func (hr *HealthReporter) HeartbeatCount() int64 {
	return atomic.LoadInt64(&hr.heartbeats)
}

var _ = uuid.New
