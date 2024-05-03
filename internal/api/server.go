package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/rs/cors"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/config"
	"github.com/heheboobes/nexuspipe/internal/repository"
)

type Server struct {
	cfg      *config.Config
	logger   *zap.Logger
	router   *gin.Engine
	httpSrv  *http.Server
	db       *pgxpool.Pool
	rdb      *redis.Client
	amqpConn *amqp091.Connection

	pipelineRepo *repository.PipelineRepository
	taskRepo     *repository.TaskRepository
	scheduleRepo *repository.ScheduleRepository
}

type ServerOption func(*Server)

func WithDB(pool *pgxpool.Pool) ServerOption {
	return func(s *Server) {
		s.db = pool
	}
}

func WithRedis(client *redis.Client) ServerOption {
	return func(s *Server) {
		s.rdb = client
	}
}

func WithAMQP(conn *amqp091.Connection) ServerOption {
	return func(s *Server) {
		s.amqpConn = conn
	}
}

func NewServer(cfg *config.Config, logger *zap.Logger, opts ...ServerOption) *Server {
	if cfg.App.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(requestIDMiddleware())
	router.Use(loggerMiddleware(logger))

	s := &Server{
		cfg:    cfg,
		logger: logger,
		router: router,
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.db != nil {
		s.pipelineRepo = repository.NewPipelineRepository(s.db)
		s.taskRepo = repository.NewTaskRepository(s.db)
		s.scheduleRepo = repository.NewScheduleRepository(s.db)
	}

	s.setupGlobalMiddleware()
	s.registerRoutes()
	s.mountMetrics()

	return s
}

func (s *Server) setupGlobalMiddleware() {
	s.router.Use(corsMiddleware(s.cfg))
	s.router.Use(rateLimitMiddleware(s.cfg))
}

func (s *Server) registerRoutes() {
	apiGroup := s.router.Group("/api/v1")
	apiGroup.Use(contentTypeMiddleware("application/json"))

	RegisterRoutes(s, apiGroup)
}

func (s *Server) mountMetrics() {
	if !s.cfg.Metrics.Enabled {
		return
	}
	s.router.GET(s.cfg.Metrics.Path, gin.WrapH(promhttp.Handler()))
	s.logger.Info("metrics endpoint enabled", zap.String("path", s.cfg.Metrics.Path))
}

func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.App.Host, s.cfg.App.Port)

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   s.cfg.App.CORSOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowedHeaders:   []string{"Authorization", "Content-Type", "X-Request-ID", "X-API-Key"},
		AllowCredentials: true,
		MaxAge:           300,
	})

	s.httpSrv = &http.Server{
		Addr:         addr,
		Handler:      corsHandler.Handler(s.router),
		ReadTimeout:  s.cfg.App.ReadTimeout,
		WriteTimeout: s.cfg.App.WriteTimeout,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		s.logger.Info("HTTP server listening", zap.String("addr", addr))
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Fatal("server error", zap.Error(err))
		}
	}()

	return nil
}

func (s *Server) Shutdown() error {
	gracePeriod := s.cfg.App.ShutdownGracePeriod
	if gracePeriod == 0 {
		gracePeriod = 15 * time.Second
	}

	s.logger.Info("initiating graceful shutdown", zap.Duration("grace_period", gracePeriod))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), gracePeriod)
	defer cancel()

	if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}

	if s.amqpConn != nil && !s.amqpConn.IsClosed() {
		if err := s.amqpConn.Close(); err != nil {
			s.logger.Warn("amqp connection close error", zap.Error(err))
		}
	}

	if s.rdb != nil {
		if err := s.rdb.Close(); err != nil {
			s.logger.Warn("redis close error", zap.Error(err))
		}
	}

	if s.db != nil {
		s.db.Close()
	}

	s.logger.Info("server exited gracefully")
	return nil
}

func (s *Server) WaitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sig := <-quit
	s.logger.Info("received signal", zap.String("signal", sig.String()))
}

func (s *Server) Run() error {
	if err := s.Start(); err != nil {
		return err
	}
	s.WaitForShutdown()
	return s.Shutdown()
}

func (s *Server) Router() *gin.Engine {
	return s.router
}

func (s *Server) Config() *config.Config {
	return s.cfg
}

func (s *Server) Logger() *zap.Logger {
	return s.logger
}

func (s *Server) PipelineRepo() *repository.PipelineRepository {
	return s.pipelineRepo
}

func (s *Server) TaskRepo() *repository.TaskRepository {
	return s.taskRepo
}

func (s *Server) ScheduleRepo() *repository.ScheduleRepository {
	return s.scheduleRepo
}

func (s *Server) DB() *pgxpool.Pool {
	return s.db
}

func (s *Server) Redis() *redis.Client {
	return s.rdb
}

func (s *Server) AMQP() *amqp091.Connection {
	return s.amqpConn
}
