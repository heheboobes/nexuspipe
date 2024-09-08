package metrics

import (
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

type HTTPMetrics struct {
	requestDuration *prometheus.HistogramVec
	requestsTotal   *prometheus.CounterVec
	responseSize    *prometheus.SummaryVec
	activeRequests  prometheus.Gauge
	inFlight        prometheus.Gauge
	registry        *MetricsRegistry
}

type HTTPMetricsConfig struct {
	DurationBuckets []float64
	SizeObjectives  map[float64]float64
}

func DefaultHTTPMetricsConfig() HTTPMetricsConfig {
	return HTTPMetricsConfig{
		DurationBuckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		SizeObjectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}
}

func NewHTTPMetrics(reg *MetricsRegistry, cfg HTTPMetricsConfig) *HTTPMetrics {
	m := &HTTPMetrics{registry: reg}

	m.requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request duration in seconds",
		Buckets:   cfg.DurationBuckets,
	}, []string{"method", "route", "status"})

	m.requestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "http_requests_total",
		Help:      "Total number of HTTP requests",
	}, []string{"method", "route", "status"})

	m.responseSize = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  reg.Namespace(),
		Subsystem:  reg.Subsystem(),
		Name:       "http_response_size_bytes",
		Help:       "HTTP response size in bytes",
		Objectives: cfg.SizeObjectives,
	}, []string{"method", "route"})

	m.activeRequests = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "http_active_requests",
		Help:      "Current number of active HTTP requests",
	})

	m.inFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: reg.Namespace(),
		Subsystem: reg.Subsystem(),
		Name:      "http_requests_in_flight",
		Help:      "Current number of in-flight HTTP requests",
	})

	collectors := map[string]prometheus.Collector{
		"http_request_duration": m.requestDuration,
		"http_requests_total":   m.requestsTotal,
		"http_response_size":    m.responseSize,
		"http_active_requests":  m.activeRequests,
		"http_in_flight":        m.inFlight,
	}
	for name, col := range collectors {
		reg.MustRegister(name, col)
	}

	return m
}

func (m *HTTPMetrics) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		m.activeRequests.Inc()
		m.inFlight.Inc()

		c.Next()

		status := strconv.Itoa(c.Writer.Status())
		route := c.FullPath()
		if route == "" {
			route = "unknown"
		}
		method := c.Request.Method

		duration := time.Since(start)
		m.requestDuration.WithLabelValues(method, route, status).Observe(duration.Seconds())
		m.requestsTotal.WithLabelValues(method, route, status).Inc()
		m.responseSize.WithLabelValues(method, route).Observe(float64(c.Writer.Size()))

		m.activeRequests.Dec()
		m.inFlight.Dec()
	}
}

type responseWriterWrapper struct {
	gin.ResponseWriter
	mu         sync.Mutex
	bodySize   int
	statusCode int
	written    bool
}

func wrapResponseWriter(w gin.ResponseWriter) *responseWriterWrapper {
	return &responseWriterWrapper{
		ResponseWriter: w,
		statusCode:     w.Status(),
	}
}

func (w *responseWriterWrapper) WriteHeader(code int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.written {
		w.statusCode = code
		w.written = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriterWrapper) Write(data []byte) (int, error) {
	w.mu.Lock()
	w.bodySize += len(data)
	w.mu.Unlock()
	return w.ResponseWriter.Write(data)
}

func (w *responseWriterWrapper) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bodySize
}

func (m *HTTPMetrics) InstrumentedHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		m.activeRequests.Inc()
		m.inFlight.Inc()

		wrapped := wrapResponseWriter(c.Writer)
		c.Writer = wrapped

		c.Next()

		status := strconv.Itoa(wrapped.statusCode)
		route := c.FullPath()
		if route == "" {
			route = "unknown"
		}
		method := c.Request.Method

		duration := time.Since(start)
		m.requestDuration.WithLabelValues(method, route, status).Observe(duration.Seconds())
		m.requestsTotal.WithLabelValues(method, route, status).Inc()
		m.responseSize.WithLabelValues(method, route).Observe(float64(wrapped.bodySize))

		m.activeRequests.Dec()
		m.inFlight.Dec()
	}
}
