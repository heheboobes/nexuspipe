package queue

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrConnectionClosed = errors.New("rabbitmq connection is closed")
	ErrChannelClosed    = errors.New("rabbitmq channel is closed")
	ErrNotConnected     = errors.New("not connected to rabbitmq")
)

type ConnectionState int

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateReconnecting
)

type Config struct {
	URL             string
	TLSConfig       *tls.Config
	MaxRetry        int
	ReconnectBase   time.Duration
	ReconnectMax    time.Duration
	Heartbeat       time.Duration
	ConnectionName  string
	ChannelPoolSize int
}

func DefaultConfig() Config {
	return Config{
		MaxRetry:        10,
		ReconnectBase:   time.Second,
		ReconnectMax:    30 * time.Second,
		Heartbeat:       10 * time.Second,
		ChannelPoolSize: 5,
	}
}

type RabbitMQ struct {
	cfg         Config
	conn        *amqp.Connection
	state       ConnectionState
	stateMu     sync.RWMutex
	channels    chan *amqp.Channel
	notifyClose chan *amqp.Error
	done        chan struct{}
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func NewRabbitMQ(cfg Config) (*RabbitMQ, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("rabbitmq URL is required")
	}
	if cfg.ReconnectBase == 0 {
		cfg.ReconnectBase = DefaultConfig().ReconnectBase
	}
	if cfg.ReconnectMax == 0 {
		cfg.ReconnectMax = DefaultConfig().ReconnectMax
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = DefaultConfig().Heartbeat
	}
	if cfg.ChannelPoolSize == 0 {
		cfg.ChannelPoolSize = DefaultConfig().ChannelPoolSize
	}

	rmq := &RabbitMQ{
		cfg:      cfg,
		state:    StateDisconnected,
		done:     make(chan struct{}),
		channels: make(chan *amqp.Channel, cfg.ChannelPoolSize),
	}

	if err := rmq.connect(); err != nil {
		return nil, fmt.Errorf("initial connection failed: %w", err)
	}

	return rmq, nil
}

func (r *RabbitMQ) connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.setState(StateConnecting)

	var conn *amqp.Connection
	var err error

	opts := []amqp.Config{
		{
			Heartbeat: r.cfg.Heartbeat,
			Properties: amqp.Table{
				"connection_name": r.cfg.ConnectionName,
			},
			TLSClientConfig: r.cfg.TLSConfig,
		},
	}

	if r.cfg.TLSConfig != nil {
		conn, err = amqp.DialTLS(r.cfg.URL, r.cfg.TLSConfig)
	} else {
		conn, err = amqp.DialConfig(r.cfg.URL, opts[0])
	}

	if err != nil {
		r.setState(StateDisconnected)
		return fmt.Errorf("dial failed: %w", err)
	}

	r.conn = conn
	r.notifyClose = make(chan *amqp.Error, 1)
	r.conn.NotifyClose(r.notifyClose)

	r.setState(StateConnected)

	if err := r.initChannelPool(); err != nil {
		r.conn.Close()
		r.setState(StateDisconnected)
		return fmt.Errorf("channel pool init failed: %w", err)
	}

	go r.reconnectLoop()

	return nil
}

func (r *RabbitMQ) reconnectLoop() {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("reconnect loop panicked: %v", rec)
		}
	}()

	select {
	case <-r.done:
		return
	case err, ok := <-r.notifyClose:
		if !ok {
			return
		}
		log.Printf("connection closed: %v, starting reconnection", err)
	}

	for attempt := 1; attempt <= r.cfg.MaxRetry; attempt++ {
		select {
		case <-r.done:
			return
		default:
		}

		r.setState(StateReconnecting)
		backoff := calculateBackoff(attempt, r.cfg.ReconnectBase, r.cfg.ReconnectMax)
		log.Printf("reconnection attempt %d/%d in %v", attempt, r.cfg.MaxRetry, backoff)

		timer := time.NewTimer(backoff)
		select {
		case <-r.done:
			timer.Stop()
			return
		case <-timer.C:
		}
		timer.Stop()

		r.mu.Lock()
		oldConn := r.conn
		r.mu.Unlock()

		if oldConn != nil && !oldConn.IsClosed() {
			_ = oldConn.Close()
		}

		if err := r.connect(); err != nil {
			log.Printf("reconnection attempt %d failed: %v", attempt, err)
			continue
		}

		log.Printf("successfully reconnected on attempt %d", attempt)
		return
	}

	log.Printf("exhausted all %d reconnection attempts", r.cfg.MaxRetry)
	r.setState(StateDisconnected)
}

func (r *RabbitMQ) initChannelPool() error {
	for i := 0; i < r.cfg.ChannelPoolSize; i++ {
		ch, err := r.conn.Channel()
		if err != nil {
			return fmt.Errorf("create channel %d: %w", i, err)
		}
		r.channels <- ch
	}
	return nil
}

func (r *RabbitMQ) AcquireChannel() (*amqp.Channel, error) {
	select {
	case ch := <-r.channels:
		if ch.IsClosed() {
			var err error
			ch, err = r.newChannel()
			if err != nil {
				return nil, err
			}
		}
		return ch, nil
	default:
		return r.newChannel()
	}
}

func (r *RabbitMQ) ReleaseChannel(ch *amqp.Channel) {
	if ch == nil {
		return
	}
	select {
	case r.channels <- ch:
	default:
		_ = ch.Close()
	}
}

func (r *RabbitMQ) newChannel() (*amqp.Channel, error) {
	r.mu.Lock()
	conn := r.conn
	r.mu.Unlock()

	if conn == nil || conn.IsClosed() {
		return nil, ErrConnectionClosed
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return ch, nil
}

func (r *RabbitMQ) Connection() *amqp.Connection {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conn
}

func (r *RabbitMQ) State() ConnectionState {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	return r.state
}

func (r *RabbitMQ) IsConnected() bool {
	return r.State() == StateConnected
}

func (r *RabbitMQ) HealthCheck(ctx context.Context) error {
	if !r.IsConnected() {
		return ErrNotConnected
	}

	ch, err := r.AcquireChannel()
	if err != nil {
		return fmt.Errorf("health check acquire channel: %w", err)
	}
	defer r.ReleaseChannel(ch)

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	_, err = ch.QueueDeclarePassive("", false, true, true, false, nil)
	if err != nil {
		r.setState(StateDisconnected)
		return fmt.Errorf("health check queue declare: %w", err)
	}

	return nil
}

func (r *RabbitMQ) Shutdown(ctx context.Context) error {
	close(r.done)

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	close(r.channels)
	for ch := range r.channels {
		if ch != nil && !ch.IsClosed() {
			_ = ch.Close()
		}
	}

	if r.conn != nil && !r.conn.IsClosed() {
		if err := r.conn.Close(); err != nil {
			return fmt.Errorf("close connection: %w", err)
		}
	}

	r.setState(StateDisconnected)
	return nil
}

func (r *RabbitMQ) setState(s ConnectionState) {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	r.state = s
}

func calculateBackoff(attempt int, base, max time.Duration) time.Duration {
	d := base * (1 << min(attempt-1, 30))
	if d > max {
		d = max
	}
	jitter := time.Duration(0)
	if d > time.Millisecond {
		jitter = time.Duration(randInt64(int64(d / 4)))
	}
	return d + jitter
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randInt64(n int64) int64 {
	return time.Now().UnixNano() % n
}
