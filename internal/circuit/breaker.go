package circuit

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type State int32

const (
	StateClosed   State = 0
	StateOpen     State = 1
	StateHalfOpen State = 2
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type MetricSnapshot struct {
	State           State
	TotalCalls      uint64
	FailedCalls     uint64
	SuccessCalls    uint64
	OpenCount       uint64
	LastFailure     time.Time
	LastStateChange time.Time
}

type Config struct {
	FailureThreshold      uint32
	SuccessThreshold      uint32
	Timeout               time.Duration
	HalfOpenMaxCalls      uint32
	HalfOpenProbeInterval time.Duration
	OnStateChange         func(old, new State)
}

type CircuitBreaker struct {
	config Config
	state  int32

	failures     uint32
	successes    uint32
	totalCalls   uint64
	failedCalls  uint64
	successCalls uint64
	openCount    uint64

	lastFailure     atomic.Value
	lastStateChange time.Time

	halfOpenTicker *time.Ticker
	mu             sync.Mutex
}

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrTooManyRequests = errors.New("too many requests in half-open state")
)

func NewConfig() Config {
	return Config{
		FailureThreshold:      5,
		SuccessThreshold:      3,
		Timeout:               30 * time.Second,
		HalfOpenMaxCalls:      1,
		HalfOpenProbeInterval: 5 * time.Second,
	}
}

func New(config Config) *CircuitBreaker {
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold == 0 {
		config.SuccessThreshold = 3
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.HalfOpenMaxCalls == 0 {
		config.HalfOpenMaxCalls = 1
	}
	if config.HalfOpenProbeInterval == 0 {
		config.HalfOpenProbeInterval = 5 * time.Second
	}

	cb := &CircuitBreaker{
		config:          config,
		state:           int32(StateClosed),
		lastStateChange: time.Now(),
	}
	cb.lastFailure.Store(time.Time{})

	if config.HalfOpenProbeInterval > 0 {
		cb.halfOpenTicker = time.NewTicker(config.HalfOpenProbeInterval)
		go cb.probeLoop()
	}

	return cb
}

func (cb *CircuitBreaker) State() State {
	return State(atomic.LoadInt32(&cb.state))
}

func (cb *CircuitBreaker) Call(fn func() (interface{}, error)) (interface{}, error) {
	atomic.AddUint64(&cb.totalCalls, 1)

	if !cb.ready() {
		atomic.AddUint64(&cb.failedCalls, 1)
		return nil, ErrCircuitOpen
	}

	result, err := fn()
	if err != nil {
		cb.recordFailure()
		return result, err
	}

	cb.recordSuccess()
	return result, nil
}

func (cb *CircuitBreaker) ready() bool {
	state := cb.State()
	switch state {
	case StateClosed:
		return true
	case StateOpen:
		if cb.config.Timeout > 0 && time.Since(cb.lastStateChange) > cb.config.Timeout {
			cb.setState(StateHalfOpen)
			return true
		}
		return false
	case StateHalfOpen:
		cb.mu.Lock()
		defer cb.mu.Unlock()
		failures := atomic.LoadUint32(&cb.failures)
		successes := atomic.LoadUint32(&cb.successes)
		total := failures + successes
		if total >= cb.config.HalfOpenMaxCalls {
			return false
		}
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) recordFailure() {
	atomic.AddUint64(&cb.failedCalls, 1)
	cb.lastFailure.Store(time.Now())

	state := cb.State()
	if state == StateHalfOpen {
		cb.mu.Lock()
		atomic.AddUint32(&cb.failures, 1)
		cb.mu.Unlock()
		cb.setState(StateOpen)
		return
	}

	if state == StateClosed {
		f := atomic.AddUint32(&cb.failures, 1)
		if f >= cb.config.FailureThreshold {
			cb.setState(StateOpen)
		}
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	atomic.AddUint64(&cb.successCalls, 1)

	state := cb.State()
	if state == StateHalfOpen {
		cb.mu.Lock()
		s := atomic.AddUint32(&cb.successes, 1)
		cb.mu.Unlock()
		if s >= cb.config.SuccessThreshold {
			cb.reset()
		}
		return
	}

	if state == StateClosed {
		atomic.StoreUint32(&cb.failures, 0)
	}
}

func (cb *CircuitBreaker) setState(newState State) {
	old := cb.State()
	if old == newState {
		return
	}

	cb.mu.Lock()
	atomic.StoreInt32(&cb.state, int32(newState))
	cb.lastStateChange = time.Now()
	atomic.StoreUint32(&cb.failures, 0)
	atomic.StoreUint32(&cb.successes, 0)

	if newState == StateOpen {
		atomic.AddUint64(&cb.openCount, 1)
	}
	cb.mu.Unlock()

	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(old, newState)
	}
}

func (cb *CircuitBreaker) reset() {
	cb.mu.Lock()
	atomic.StoreInt32(&cb.state, int32(StateClosed))
	atomic.StoreUint32(&cb.failures, 0)
	atomic.StoreUint32(&cb.successes, 0)
	cb.lastStateChange = time.Now()
	cb.mu.Unlock()

	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(StateHalfOpen, StateClosed)
	}
}

func (cb *CircuitBreaker) probeLoop() {
	for range cb.halfOpenTicker.C {
		if cb.State() == StateOpen {
			if cb.config.Timeout > 0 && time.Since(cb.lastStateChange) > cb.config.Timeout {
				cb.setState(StateHalfOpen)
			}
		}
	}
}

func (cb *CircuitBreaker) Metrics() MetricSnapshot {
	t := cb.lastFailure.Load().(time.Time)
	return MetricSnapshot{
		State:           cb.State(),
		TotalCalls:      atomic.LoadUint64(&cb.totalCalls),
		FailedCalls:     atomic.LoadUint64(&cb.failedCalls),
		SuccessCalls:    atomic.LoadUint64(&cb.successCalls),
		OpenCount:       atomic.LoadUint64(&cb.openCount),
		LastFailure:     t,
		LastStateChange: cb.lastStateChange,
	}
}

func (cb *CircuitBreaker) Close() {
	if cb.halfOpenTicker != nil {
		cb.halfOpenTicker.Stop()
	}
}
