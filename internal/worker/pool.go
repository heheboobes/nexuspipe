package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/queue"
)

type WorkerMetrics struct {
	WorkerID       string
	TasksProcessed int64
	TasksSucceeded int64
	TasksFailed    int64
	TasksRetried   int64
	TotalDuration  time.Duration
	AvgDuration    time.Duration
	LastActivity   time.Time
	IsBusy         bool
	CurrentTaskID  string
	Panics         int64
}

type PoolConfig struct {
	MinWorkers      int
	MaxWorkers      int
	ScaleUpFactor   float64
	ScaleDownFactor float64
	IdleThreshold   time.Duration
	CheckInterval   time.Duration
	QueueLength     int
	TaskTimeout     time.Duration
	ShutdownTimeout time.Duration
}

func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MinWorkers:      1,
		MaxWorkers:      20,
		ScaleUpFactor:   0.75,
		ScaleDownFactor: 0.25,
		IdleThreshold:   30 * time.Second,
		CheckInterval:   5 * time.Second,
		QueueLength:     100,
		TaskTimeout:     60 * time.Second,
		ShutdownTimeout: 15 * time.Second,
	}
}

type WorkerPool struct {
	cfg       PoolConfig
	workers   []*poolWorker
	taskCh    chan workerTask
	metrics   sync.Map
	logger    *zap.Logger
	mu        sync.RWMutex
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	running   atomic.Bool
	errors    []error
	errMu     sync.Mutex
	workerSeq atomic.Int64
}

type poolWorker struct {
	id       string
	index    int
	started  time.Time
	tasks    int64
	panics   int64
	lastTask time.Time
	busy     atomic.Bool
	current  atomic.Value
}

type workerTask struct {
	ctx    context.Context
	msg    amqp091.Delivery
	env    *queue.Envelope
	info   *queue.DeliveryInfo
	result chan error
}

func NewWorkerPool(cfg PoolConfig, logger *zap.Logger) *WorkerPool {
	if cfg.MinWorkers <= 0 {
		cfg.MinWorkers = 1
	}
	if cfg.MaxWorkers < cfg.MinWorkers {
		cfg.MaxWorkers = cfg.MinWorkers
	}

	return &WorkerPool{
		cfg:    cfg,
		taskCh: make(chan workerTask, cfg.QueueLength),
		logger: logger.With(zap.String("component", "worker_pool")),
	}
}

func (wp *WorkerPool) Start(ctx context.Context) error {
	ctx, wp.cancel = context.WithCancel(ctx)
	wp.running.Store(true)

	wp.logger.Info("starting worker pool",
		zap.Int("min_workers", wp.cfg.MinWorkers),
		zap.Int("max_workers", wp.cfg.MaxWorkers),
	)

	for i := 0; i < wp.cfg.MinWorkers; i++ {
		wp.addWorker()
	}

	go wp.monitorLoop(ctx)
	go wp.scaleLoop(ctx)

	return nil
}

func (wp *WorkerPool) Stop() error {
	if !wp.running.Load() {
		return nil
	}
	wp.running.Store(false)
	wp.cancel()

	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(wp.cfg.ShutdownTimeout)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		wp.logger.Warn("worker pool shutdown timed out",
			zap.Duration("timeout", wp.cfg.ShutdownTimeout),
		)
	}

	wp.mu.Lock()
	wp.workers = nil
	wp.mu.Unlock()

	wp.logger.Info("worker pool stopped")
	return nil
}

func (wp *WorkerPool) Submit(ctx context.Context, msg amqp091.Delivery, env *queue.Envelope, info *queue.DeliveryInfo) error {
	if !wp.running.Load() {
		return fmt.Errorf("worker pool is not running")
	}

	task := workerTask{
		ctx:    ctx,
		msg:    msg,
		env:    env,
		info:   info,
		result: make(chan error, 1),
	}

	select {
	case wp.taskCh <- task:
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("task queue is full (%d capacity)", wp.cfg.QueueLength)
	}

	return nil
}

func (wp *WorkerPool) Metrics() []WorkerMetrics {
	var result []WorkerMetrics
	wp.metrics.Range(func(key, value interface{}) bool {
		if m, ok := value.(WorkerMetrics); ok {
			result = append(result, m)
		}
		return true
	})
	return result
}

func (wp *WorkerPool) PoolMetrics() PoolMetrics {
	total := len(wp.workers)
	busy := 0
	for _, w := range wp.workers {
		if w.busy.Load() {
			busy++
		}
	}
	return PoolMetrics{
		TotalWorkers:  total,
		BusyWorkers:   busy,
		IdleWorkers:   total - busy,
		QueueDepth:    len(wp.taskCh),
		QueueCapacity: cap(wp.taskCh),
	}
}

func (wp *WorkerPool) Errors() []error {
	wp.errMu.Lock()
	defer wp.errMu.Unlock()
	errs := make([]error, len(wp.errors))
	copy(errs, wp.errors)
	return errs
}

func (wp *WorkerPool) IsRunning() bool {
	return wp.running.Load()
}

func (wp *WorkerPool) addWorker() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	seq := wp.workerSeq.Add(1)
	workerID := fmt.Sprintf("worker-%d", seq)

	w := &poolWorker{
		id:      workerID,
		index:   len(wp.workers),
		started: time.Now().UTC(),
	}

	wp.workers = append(wp.workers, w)

	wp.wg.Add(1)
	go wp.workerRun(w)

	wp.logger.Debug("worker added", zap.String("worker_id", workerID))
}

func (wp *WorkerPool) removeWorker(index int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if index < 0 || index >= len(wp.workers) {
		return
	}

	wp.workers = append(wp.workers[:index], wp.workers[index+1:]...)
}

func (wp *WorkerPool) workerRun(w *poolWorker) {
	defer wp.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&w.panics, 1)
			wp.logger.Error("worker panicked",
				zap.String("worker_id", w.id),
				zap.Any("panic", r),
			)
		}
	}()

	wp.logger.Debug("worker started", zap.String("worker_id", w.id))

	for {
		select {
		case <-wp.cancelContext().Done():
			wp.logger.Debug("worker stopping",
				zap.String("worker_id", w.id),
			)
			return
		case task, ok := <-wp.taskCh:
			if !ok {
				return
			}
			wp.processTask(w, task)
		}
	}
}

func (wp *WorkerPool) processTask(w *poolWorker, task workerTask) {
	w.busy.Store(true)
	atomic.AddInt64(&w.tasks, 1)
	w.current.Store(task.env.ID)
	start := time.Now().UTC()

	defer func() {
		w.busy.Store(false)
		w.lastTask = time.Now().UTC()
		w.current.Store("")
		wp.updateMetrics(w, start, task.result)
	}()

	select {
	case task.result <- nil:
	case <-task.ctx.Done():
		wp.logger.Warn("task context cancelled",
			zap.String("task_id", task.env.ID),
			zap.String("worker_id", w.id),
		)
	}

	close(task.result)
}

func (wp *WorkerPool) updateMetrics(w *poolWorker, start time.Time, resultCh chan error) {
	var err error
	select {
	case err = <-resultCh:
	default:
	}

	duration := time.Since(start)

	m := WorkerMetrics{
		WorkerID:       w.id,
		TasksProcessed: atomic.LoadInt64(&w.tasks),
		LastActivity:   time.Now().UTC(),
		IsBusy:         false,
		Panics:         atomic.LoadInt64(&w.panics),
		TotalDuration:  duration,
	}

	if err != nil {
		m.TasksFailed++
		wp.addError(err)
	} else {
		m.TasksSucceeded++
	}

	wp.metrics.Store(w.id, m)
}

func (wp *WorkerPool) addError(err error) {
	wp.errMu.Lock()
	defer wp.errMu.Unlock()
	wp.errors = append(wp.errors, err)
	if len(wp.errors) > 1000 {
		wp.errors = wp.errors[len(wp.errors)-500:]
	}
}

func (wp *WorkerPool) cancelContext() context.Context {
	return context.Background()
}

func (wp *WorkerPool) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(wp.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			wp.checkWorkerHealth()
		}
	}
}

func (wp *WorkerPool) checkWorkerHealth() {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	indicesToRemove := make([]int, 0)
	for i, w := range wp.workers {
		if w.panics > 3 {
			wp.logger.Warn("worker exceeded panic threshold, marking for removal",
				zap.String("worker_id", w.id),
				zap.Int64("panics", w.panics),
			)
			indicesToRemove = append(indicesToRemove, i)
		}
	}

	for _, idx := range indicesToRemove {
		wp.removeWorker(idx)
	}
}

func (wp *WorkerPool) scaleLoop(ctx context.Context) {
	ticker := time.NewTicker(wp.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			wp.scale()
		}
	}
}

func (wp *WorkerPool) scale() {
	wp.mu.RLock()
	total := len(wp.workers)
	queueLen := len(wp.taskCh)
	cap := cap(wp.taskCh)
	wp.mu.RUnlock()

	if total == 0 {
		wp.mu.Lock()
		wp.addWorker()
		wp.mu.Unlock()
		return
	}

	utilization := float64(queueLen) / float64(cap)

	if utilization >= wp.cfg.ScaleUpFactor && total < wp.cfg.MaxWorkers {
		scaleBy := max(1, int(float64(total)*0.25))
		if total+scaleBy > wp.cfg.MaxWorkers {
			scaleBy = wp.cfg.MaxWorkers - total
		}
		for i := 0; i < scaleBy; i++ {
			wp.addWorker()
		}
		wp.logger.Info("scaled up workers",
			zap.Int("added", scaleBy),
			zap.Int("total", total+scaleBy),
			zap.Float64("utilization", utilization),
		)
		return
	}

	busyCount := 0
	totalWorkers := 0
	{
		wp.mu.RLock()
		for _, w := range wp.workers {
			totalWorkers++
			if w.busy.Load() {
				busyCount++
			}
		}
		wp.mu.RUnlock()
	}

	if totalWorkers > 0 {
		idleRatio := float64(totalWorkers-busyCount) / float64(totalWorkers)
		if idleRatio >= wp.cfg.ScaleDownFactor && totalWorkers > wp.cfg.MinWorkers && queueLen == 0 {
			scaleBy := max(1, int(float64(totalWorkers)*0.25))
			removeFrom := totalWorkers - scaleBy
			if removeFrom < wp.cfg.MinWorkers {
				removeFrom = wp.cfg.MinWorkers
			}

			wp.mu.Lock()
			for i := len(wp.workers) - 1; i >= removeFrom && i >= wp.cfg.MinWorkers; i-- {
				wp.workers = wp.workers[:i]
			}
			wp.mu.Unlock()

			wp.logger.Info("scaled down workers",
				zap.Int("removed", totalWorkers-removeFrom),
				zap.Int("total", removeFrom),
				zap.Float64("idle_ratio", idleRatio),
			)
		}
	}
}

type PoolMetrics struct {
	TotalWorkers  int
	BusyWorkers   int
	IdleWorkers   int
	QueueDepth    int
	QueueCapacity int
}

var _ = uuid.New
