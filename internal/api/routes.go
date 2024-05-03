package api

import (
	"github.com/gin-gonic/gin"

	"github.com/heheboobes/nexuspipe/internal/api/handlers"
)

func RegisterRoutes(s *Server, r *gin.RouterGroup) {
	h := handlers.New(s.PipelineRepo(), s.TaskRepo(), s.ScheduleRepo(), s.Logger())

	health := handlers.NewHealthHandler(s.DB(), s.AMQP(), s.Logger())
	webhook := handlers.NewWebhookHandler(s.Logger())

	r.GET("/health", health.Health)
	r.GET("/ready", health.Readiness)
	r.GET("/live", health.Liveness)

	pipelines := r.Group("/pipelines")
	{
		pipelines.POST("", h.CreatePipeline)
		pipelines.GET("", h.ListPipelines)
		pipelines.GET("/:id", h.GetPipeline)
		pipelines.PUT("/:id", h.UpdatePipeline)
		pipelines.DELETE("/:id", h.DeletePipeline)
		pipelines.POST("/:id/execute", h.ExecutePipeline)
	}

	webhooks := r.Group("/webhooks")
	{
		webhooks.POST("", webhook.Register)
		webhooks.GET("", webhook.List)
		webhooks.GET("/:id", webhook.Get)
		webhooks.PUT("/:id", webhook.Update)
		webhooks.DELETE("/:id", webhook.Delete)
		webhooks.GET("/:id/deliveries", webhook.DeliveryHistory)
		webhooks.POST("/:id/deliveries/:delivery_id/retry", webhook.RetryDelivery)
	}

	protected := r.Group("")
	protected.Use(authMiddleware(s.Config()))
	{
		tasks := protected.Group("/tasks")
		{
			tasks.GET("", h.ListTasks)
			tasks.GET("/:id", h.GetTask)
			tasks.PUT("/:id/retry", h.RetryTask)
			tasks.POST("/:id/cancel", h.CancelTask)
		}

		events := protected.Group("/events")
		{
			events.POST("", h.EmitEvent)
			events.GET("", h.ListEvents)
			events.GET("/:id", h.GetEvent)
			events.PUT("/:id/status", h.UpdateEventStatus)
		}

		schedules := protected.Group("/schedules")
		{
			schedules.POST("", h.CreateSchedule)
			schedules.GET("", h.ListSchedules)
			schedules.GET("/:id", h.GetSchedule)
			schedules.PUT("/:id", h.UpdateSchedule)
			schedules.DELETE("/:id", h.DeleteSchedule)
			schedules.POST("/:id/toggle", h.ToggleSchedule)
		}
	}

	admin := r.Group("/admin")
	admin.Use(authMiddleware(s.Config()))
	admin.Use(adminMiddleware(s.Config()))
	{
		admin.GET("/stats", h.SystemStats)
		admin.GET("/logs", h.SystemLogs)
	}

	r.GET("/metrics", gin.WrapH(s.router.RedirectFixedPath))
}
