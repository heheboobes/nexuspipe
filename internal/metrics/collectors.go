package metrics

import (
	"database/sql"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type PipelineCollectors struct {
	registry       *MetricsRegistry
	execDuration   *prometheus.HistogramVec
	tasksProcessed *prometheus.CounterVec
	activeWorkers  prometheus.Gauge
	queueDepth     prometheus.Gauge
	errorsTotal    *prometheus.CounterVec
	dbStats        *DBStatsCollector
}

type PipelineCollectorsConfig struct {
	ExecutionBuckets []float64
	ErrorLabels      []string
	TaskTypeLabels   []string
}

func DefaultCollectorsConfig() PipelineCollectorsConfig {
	return PipelineCollectorsConfig{
		ExecutionBuckets: prometheus.DefBuckets,
		ErrorLabels:      []string{"type", "stage", "severity"},
		TaskTypeLabels:   []string{"type", "status"},
	}
}

func NewPipelineCollectors(reg *MetricsRegistry, cfg PipelineCollectorsConfig) *PipelineCollectors {
	c := &PipelineCollectors{registry: reg}

	c.execDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "execution_duration_seconds",
		Help:      "Pipeline execution duration in seconds",
		Buckets:   cfg.ExecutionBuckets,
	}, []string{"pipeline", "stage"})

	c.tasksProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "tasks_processed_total",
		Help:      "Total number of tasks processed",
	}, cfg.TaskTypeLabels)

	c.activeWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "active_workers",
		Help:      "Current number of active workers",
	})

	c.queueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "queue_depth",
		Help:      "Current depth of the task queue",
	})

	c.errorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "errors_total",
		Help:      "Total number of errors encountered",
	}, cfg.ErrorLabels)

	collectors := map[string]prometheus.Collector{
		"execution_duration": c.execDuration,
		"tasks_processed":    c.tasksProcessed,
		"active_workers":     c.activeWorkers,
		"queue_depth":        c.queueDepth,
		"errors_total":       c.errorsTotal,
	}
	for name, col := range collectors {
		reg.MustRegister(name, col)
	}

	return c
}

func (c *PipelineCollectors) ObserveExecution(pipeline, stage string, dur time.Duration) {
	c.execDuration.WithLabelValues(pipeline, stage).Observe(dur.Seconds())
}

func (c *PipelineCollectors) IncTask(taskType, status string) {
	c.tasksProcessed.WithLabelValues(taskType, status).Inc()
}

func (c *PipelineCollectors) SetActiveWorkers(n int) {
	c.activeWorkers.Set(float64(n))
}

func (c *PipelineCollectors) IncActiveWorkers() {
	c.activeWorkers.Inc()
}

func (c *PipelineCollectors) DecActiveWorkers() {
	c.activeWorkers.Dec()
}

func (c *PipelineCollectors) SetQueueDepth(n int) {
	c.queueDepth.Set(float64(n))
}

func (c *PipelineCollectors) IncError(errType, stage, severity string) {
	c.errorsTotal.WithLabelValues(errType, stage, severity).Inc()
}

func (c *PipelineCollectors) WithDBStats(db *sql.DB) *PipelineCollectors {
	c.dbStats = NewDBStatsCollector(db, c.registry)
	c.registry.MustRegister("db_stats", c.dbStats)
	return c
}

type DBStatsCollector struct {
	db          *sql.DB
	mu          sync.Mutex
	descriptors []*prometheus.Desc
}

func NewDBStatsCollector(db *sql.DB, reg *MetricsRegistry) *DBStatsCollector {
	ns := reg.Namespace()
	ss := reg.Subsystem()
	return &DBStatsCollector{
		db: db,
		descriptors: []*prometheus.Desc{
			prometheus.NewDesc(prometheus.BuildFQName(ns, ss, "db_open_connections"), "Open connections", nil, nil),
			prometheus.NewDesc(prometheus.BuildFQName(ns, ss, "db_inuse_connections"), "In-use connections", nil, nil),
			prometheus.NewDesc(prometheus.BuildFQName(ns, ss, "db_idle_connections"), "Idle connections", nil, nil),
			prometheus.NewDesc(prometheus.BuildFQName(ns, ss, "db_wait_count_total"), "Total wait count", nil, nil),
			prometheus.NewDesc(prometheus.BuildFQName(ns, ss, "db_wait_duration_seconds_total"), "Total wait duration", nil, nil),
			prometheus.NewDesc(prometheus.BuildFQName(ns, ss, "db_max_open_connections"), "Max open connections", nil, nil),
			prometheus.NewDesc(prometheus.BuildFQName(ns, ss, "db_max_idle_closed_total"), "Max idle closed", nil, nil),
			prometheus.NewDesc(prometheus.BuildFQName(ns, ss, "db_max_lifetime_closed_total"), "Max lifetime closed", nil, nil),
		},
	}
}

func (c *DBStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.descriptors {
		ch <- d
	}
}

func (c *DBStatsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := c.db.Stats()
	ch <- prometheus.MustNewConstMetric(c.descriptors[0], prometheus.GaugeValue, float64(stats.OpenConnections))
	ch <- prometheus.MustNewConstMetric(c.descriptors[1], prometheus.GaugeValue, float64(stats.InUse))
	ch <- prometheus.MustNewConstMetric(c.descriptors[2], prometheus.GaugeValue, float64(stats.Idle))
	ch <- prometheus.MustNewConstMetric(c.descriptors[3], prometheus.CounterValue, float64(stats.WaitCount))
	ch <- prometheus.MustNewConstMetric(c.descriptors[4], prometheus.CounterValue, stats.WaitDuration.Seconds())
	ch <- prometheus.MustNewConstMetric(c.descriptors[5], prometheus.GaugeValue, float64(stats.MaxOpenConnections))
	ch <- prometheus.MustNewConstMetric(c.descriptors[6], prometheus.CounterValue, float64(stats.MaxIdleClosed))
	ch <- prometheus.MustNewConstMetric(c.descriptors[7], prometheus.CounterValue, float64(stats.MaxLifetimeClosed))
}

func (c *PipelineCollectors) Describe(ch chan<- *prometheus.Desc) {
	if c.dbStats != nil {
		c.dbStats.Describe(ch)
	}
}

func (c *PipelineCollectors) Collect(ch chan<- prometheus.Metric) {
	if c.dbStats != nil {
		c.dbStats.Collect(ch)
	}
}

var _ prometheus.Collector = (*DBStatsCollector)(nil)
