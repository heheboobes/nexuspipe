package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"github.com/spf13/cobra"

	"github.com/heheboobes/nexuspipe/internal/config"
	"github.com/heheboobes/nexuspipe/internal/logger"
)

var (
	cfgFile    string
	commitHash string
	buildTime  string
)

var rootCmd = &cobra.Command{
	Use:   "nexuspipe-api",
	Short: "NexusPipe API server",
	Long: `NexusPipe API server provides RESTful endpoints for managing
pipelines, events, and tasks in the distributed event processing system.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runAPI(cmd.Context())
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "config.yaml", "path to config file")
	rootCmd.PersistentFlags().Bool("verbose", false, "enable verbose output")

	rootCmd.AddCommand(versionCmd())
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("NexusPipe API\n")
			fmt.Printf("  Version:    %s\n", configVersion())
			fmt.Printf("  Commit:     %s\n", commitHash)
			fmt.Printf("  Build Time: %s\n", buildTime)
		},
	}
}

func configVersion() string {
	if commitHash != "" && buildTime != "" {
		return fmt.Sprintf("%s (built %s)", commitHash[:7], buildTime)
	}
	return "development"
}

func runAPI(ctx context.Context) error {
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	logCfg := logger.Config{
		Level:     cfg.Log.Level,
		Format:    cfg.Log.Format,
		AddSource: cfg.Log.AddSource,
	}
	zapLog, err := logger.NewLogger(logCfg)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer zapLog.Sync()

	sugared := zapLog.Sugar()
	sugared.Infow("starting NexusPipe API server",
		"version", configVersion(),
		"host", cfg.App.Host,
		"port", cfg.App.Port,
		"environment", cfg.App.Environment,
	)

	if cfg.App.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.LoggerWithWriter(gin.DefaultWriter))

	apiGroup := router.Group("/api/v1")
	{
		apiGroup.GET("/health", healthHandler(zapLog))
		apiGroup.GET("/ready", readinessHandler())

		pipelines := apiGroup.Group("/pipelines")
		{
			pipelines.POST("/", createPipelineHandler())
			pipelines.GET("/", listPipelinesHandler())
			pipelines.GET("/:id", getPipelineHandler())
			pipelines.PUT("/:id", updatePipelineHandler())
			pipelines.DELETE("/:id", deletePipelineHandler())
			pipelines.POST("/:id/execute", executePipelineHandler())
		}

		events := apiGroup.Group("/events")
		{
			events.POST("/", emitEventHandler())
			events.GET("/", listEventsHandler())
			events.GET("/:id", getEventHandler())
			events.PUT("/:id/status", updateEventStatusHandler())
		}

		tasks := apiGroup.Group("/tasks")
		{
			tasks.GET("/", listTasksHandler())
			tasks.GET("/:id", getTaskHandler())
			tasks.PUT("/:id/retry", retryTaskHandler())
			tasks.POST("/:id/cancel", cancelTaskHandler())
		}
	}

	if cfg.Metrics.Enabled {
		router.GET(cfg.Metrics.Path, gin.WrapH(promhttp.Handler()))
		sugared.Infow("metrics endpoint enabled", "path", cfg.Metrics.Path)
	}

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   cfg.App.CORSOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowedHeaders:   []string{"Authorization", "Content-Type", "X-Request-ID"},
		AllowCredentials: true,
		MaxAge:           300,
	})

	addr := fmt.Sprintf("%s:%d", cfg.App.Host, cfg.App.Port)

	server := &http.Server{
		Addr:         addr,
		Handler:      corsHandler.Handler(router),
		ReadTimeout:  cfg.App.ReadTimeout,
		WriteTimeout: cfg.App.WriteTimeout,
	}

	go func() {
		sugared.Infow("HTTP server listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sugared.Fatalw("server error", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	sugared.Infow("shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.App.ShutdownGracePeriod)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		sugared.Fatalw("server forced to shutdown", "error", err)
	}

	sugared.Infow("server exited gracefully")
	return nil
}

func healthHandler(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"service":   "nexuspipe-api",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

func readinessHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ready",
		})
	}
}

func createPipelineHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func listPipelinesHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func getPipelineHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func updatePipelineHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func deletePipelineHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func executePipelineHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func emitEventHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func listEventsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func getEventHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func updateEventStatusHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func listTasksHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func getTaskHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func retryTaskHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func cancelTaskHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v", err)
	}
}
