package metrics

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	DefaultNamespace = "nexuspipe"
	DefaultSubsystem = "pipeline"
)

type MetricsRegistry struct {
	mu         sync.RWMutex
	registry   *prometheus.Registry
	namespace  string
	subsystem  string
	collectors map[string]prometheus.Collector
}

type RegistryOption func(*MetricsRegistry)

func WithNamespace(ns string) RegistryOption {
	return func(r *MetricsRegistry) {
		r.namespace = ns
	}
}

func WithSubsystem(ss string) RegistryOption {
	return func(r *MetricsRegistry) {
		r.subsystem = ss
	}
}

func NewMetricsRegistry(opts ...RegistryOption) *MetricsRegistry {
	r := &MetricsRegistry{
		registry:   prometheus.NewRegistry(),
		namespace:  DefaultNamespace,
		subsystem:  DefaultSubsystem,
		collectors: make(map[string]prometheus.Collector),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *MetricsRegistry) RegisterDefault() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	defaultCollectors := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	}
	for _, c := range defaultCollectors {
		if err := r.registry.Register(c); err != nil {
			if !isAlreadyRegistered(err) {
				return fmt.Errorf("register default collector: %w", err)
			}
		}
	}
	return nil
}

func (r *MetricsRegistry) Register(name string, c prometheus.Collector) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.collectors[name]; exists {
		return fmt.Errorf("collector %q already registered", name)
	}
	if err := r.registry.Register(c); err != nil {
		if isAlreadyRegistered(err) {
			return nil
		}
		return fmt.Errorf("register collector %q: %w", name, err)
	}
	r.collectors[name] = c
	return nil
}

func (r *MetricsRegistry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	c, ok := r.collectors[name]
	if !ok {
		return false
	}
	ok = r.registry.Unregister(c)
	if ok {
		delete(r.collectors, name)
	}
	return ok
}

func (r *MetricsRegistry) MustRegister(name string, c prometheus.Collector) {
	if err := r.Register(name, c); err != nil {
		panic(err)
	}
}

func (r *MetricsRegistry) Registry() *prometheus.Registry {
	return r.registry
}

func (r *MetricsRegistry) Namespace() string {
	return r.namespace
}

func (r *MetricsRegistry) Subsystem() string {
	return r.subsystem
}

func (r *MetricsRegistry) FQName(name string) string {
	return prometheus.BuildFQName(r.namespace, r.subsystem, name)
}

func (r *MetricsRegistry) Collector(name string) (prometheus.Collector, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.collectors[name]
	return c, ok
}

func (r *MetricsRegistry) RegisteredNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.collectors))
	for n := range r.collectors {
		names = append(names, n)
	}
	return names
}

func (r *MetricsRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry = prometheus.NewRegistry()
	r.collectors = make(map[string]prometheus.Collector)
}

func isAlreadyRegistered(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(prometheus.AlreadyRegisteredError)
	return ok
}
