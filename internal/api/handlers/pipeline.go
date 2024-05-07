package handlers

import (
	"errors"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/models"
	"github.com/heheboobes/nexuspipe/internal/repository"
)

type Handler struct {
	pipelineRepo *repository.PipelineRepository
	taskRepo     *repository.TaskRepository
	scheduleRepo *repository.ScheduleRepository
	logger       *zap.Logger
}

func New(
	pipelineRepo *repository.PipelineRepository,
	taskRepo *repository.TaskRepository,
	scheduleRepo *repository.ScheduleRepository,
	logger *zap.Logger,
) *Handler {
	return &Handler{
		pipelineRepo: pipelineRepo,
		taskRepo:     taskRepo,
		scheduleRepo: scheduleRepo,
		logger:       logger,
	}
}

type createPipelineRequest struct {
	Name        string                `json:"name" binding:"required,min=1,max=255"`
	Description string                `json:"description,omitempty" binding:"max=2000"`
	Config      models.PipelineConfig `json:"config"`
	Tags        map[string]string     `json:"tags,omitempty"`
}

type updatePipelineRequest struct {
	Name        *string                `json:"name,omitempty" binding:"omitempty,min=1,max=255"`
	Description *string                `json:"description,omitempty" binding:"omitempty,max=2000"`
	Status      *models.PipelineStatus `json:"status,omitempty"`
	Config      *models.PipelineConfig `json:"config,omitempty"`
	Tags        *map[string]string     `json:"tags,omitempty"`
}

type listPipelinesQuery struct {
	Status  string `form:"status"`
	Search  string `form:"search"`
	Page    int    `form:"page"`
	PerPage int    `form:"per_page"`
}

type executePipelineRequest struct {
	Input       string `json:"input,omitempty"`
	TriggeredBy string `json:"triggered_by,omitempty"`
}

func (h *Handler) CreatePipeline(c *gin.Context) {
	var req createPipelineRequest
	if !bindJSON(c, &req) {
		return
	}

	createdBy := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	pipeline := models.NewPipeline(req.Name, createdBy)
	pipeline.Description = req.Description
	pipeline.Tags = req.Tags
	if req.Config.MaxRetries > 0 {
		pipeline.Config = req.Config
	}

	if err := h.pipelineRepo.Create(c.Request.Context(), pipeline); err != nil {
		h.logger.Error("failed to create pipeline", zap.Error(err))
		internalError(c, "Failed to create pipeline")
		return
	}

	created(c, pipeline)
}

func (h *Handler) GetPipeline(c *gin.Context) {
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		badRequest(c, "Invalid pipeline ID")
		return
	}

	pipeline, err := h.pipelineRepo.Get(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			notFound(c, "Pipeline not found")
			return
		}
		h.logger.Error("failed to get pipeline", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to get pipeline")
		return
	}

	success(c, pipeline)
}

func (h *Handler) UpdatePipeline(c *gin.Context) {
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		badRequest(c, "Invalid pipeline ID")
		return
	}

	var req updatePipelineRequest
	if !bindJSON(c, &req) {
		return
	}

	pipeline, err := h.pipelineRepo.Get(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			notFound(c, "Pipeline not found")
			return
		}
		h.logger.Error("failed to fetch pipeline for update", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to update pipeline")
		return
	}

	if req.Name != nil {
		pipeline.Name = *req.Name
	}
	if req.Description != nil {
		pipeline.Description = *req.Description
	}
	if req.Status != nil {
		if !req.Status.IsValid() {
			badRequest(c, "Invalid pipeline status")
			return
		}
		pipeline.Status = *req.Status
	}
	if req.Config != nil {
		pipeline.Config = *req.Config
	}
	if req.Tags != nil {
		pipeline.Tags = *req.Tags
	}

	if err := h.pipelineRepo.Update(c.Request.Context(), pipeline); err != nil {
		if errors.Is(err, repository.ErrConcurrentModification) {
			conflict(c, "Pipeline was modified by another request")
			return
		}
		h.logger.Error("failed to update pipeline", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to update pipeline")
		return
	}

	success(c, pipeline)
}

func (h *Handler) DeletePipeline(c *gin.Context) {
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		badRequest(c, "Invalid pipeline ID")
		return
	}

	if err := h.pipelineRepo.Delete(c.Request.Context(), id); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			notFound(c, "Pipeline not found")
			return
		}
		h.logger.Error("failed to delete pipeline", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to delete pipeline")
		return
	}

	noContent(c)
}

func (h *Handler) ListPipelines(c *gin.Context) {
	var query listPipelinesQuery
	if !bindQuery(c, &query) {
		return
	}

	page := query.Page
	if page < 1 {
		page = 1
	}

	perPage := query.PerPage
	if perPage < 1 || perPage > 100 {
		perPage = 20
	}

	offset := (page - 1) * perPage

	filter := repository.PipelineFilter{
		Status: models.PipelineStatus(query.Status),
		Limit:  perPage,
		Offset: offset,
	}

	pipelines, err := h.pipelineRepo.List(c.Request.Context(), filter)
	if err != nil {
		h.logger.Error("failed to list pipelines", zap.Error(err))
		internalError(c, "Failed to list pipelines")
		return
	}

	var total int64
	if len(pipelines) > 0 {
		count, err := h.pipelineRepo.Count(c.Request.Context(), models.PipelineStatus(query.Status))
		if err != nil {
			h.logger.Warn("failed to count pipelines", zap.Error(err))
		} else {
			total = int64(count)
		}
	}

	paginated(c, pipelines, page, perPage, total)
}

func (h *Handler) ExecutePipeline(c *gin.Context) {
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		badRequest(c, "Invalid pipeline ID")
		return
	}

	var req executePipelineRequest
	if !bindJSON(c, &req) {
		return
	}

	pipeline, err := h.pipelineRepo.Get(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			notFound(c, "Pipeline not found")
			return
		}
		h.logger.Error("failed to get pipeline for execution", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to get pipeline")
		return
	}

	triggeredBy := req.TriggeredBy
	if triggeredBy == "" {
		triggeredBy = "api"
	}

	execution := models.PipelineExecution{
		ID:          uuid.New(),
		PipelineID:  pipeline.ID,
		Status:      models.PipelineStatusActive,
		Input:       req.Input,
		TriggeredBy: triggeredBy,
		CreatedAt:   time.Now().UTC(),
	}

	success(c, execution)
}

func (h *Handler) ListTasks(c *gin.Context) {
	pipelineID := c.Query("pipeline_id")
	status := c.Query("status")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	perPage, _ := strconv.Atoi(c.DefaultQuery("per_page", "50"))

	if perPage < 1 || perPage > 200 {
		perPage = 50
	}
	offset := (page - 1) * perPage

	filter := repository.TaskFilter{
		PipelineID: pipelineID,
		Status:     models.TaskStatus(status),
		Limit:      perPage,
		Offset:     offset,
	}

	tasks, err := h.taskRepo.GetByPipeline(c.Request.Context(), pipelineID, filter)
	if err != nil {
		h.logger.Error("failed to list tasks", zap.Error(err))
		internalError(c, "Failed to list tasks")
		return
	}

	success(c, tasks)
}

func (h *Handler) GetTask(c *gin.Context) {
	id := c.Param("id")

	task, err := h.taskRepo.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			notFound(c, "Task not found")
			return
		}
		h.logger.Error("failed to get task", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to get task")
		return
	}

	success(c, task)
}

func (h *Handler) RetryTask(c *gin.Context) {
	id := c.Param("id")

	task, err := h.taskRepo.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			notFound(c, "Task not found")
			return
		}
		h.logger.Error("failed to get task for retry", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to get task")
		return
	}

	if err := h.taskRepo.IncrementRetry(c.Request.Context(), id, "manual retry"); err != nil {
		h.logger.Error("failed to retry task", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to retry task")
		return
	}

	task.Status = models.TaskStatusPending
	success(c, task)
}

func (h *Handler) CancelTask(c *gin.Context) {
	id := c.Param("id")

	_, err := h.taskRepo.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			notFound(c, "Task not found")
			return
		}
		h.logger.Error("failed to get task for cancellation", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to get task")
		return
	}

	if err := h.taskRepo.UpdateStatus(c.Request.Context(), id, models.TaskStatusCancelled, "cancelled by user"); err != nil {
		h.logger.Error("failed to cancel task", zap.String("id", id), zap.Error(err))
		internalError(c, "Failed to cancel task")
		return
	}

	noContent(c)
}

func (h *Handler) EmitEvent(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) ListEvents(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) GetEvent(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) UpdateEventStatus(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) CreateSchedule(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) ListSchedules(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) GetSchedule(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) UpdateSchedule(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) DeleteSchedule(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) ToggleSchedule(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) SystemStats(c *gin.Context) {
	notImplemented(c)
}

func (h *Handler) SystemLogs(c *gin.Context) {
	notImplemented(c)
}
