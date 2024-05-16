package handlers

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Webhook struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	URL        string            `json:"url"`
	PipelineID string            `json:"pipeline_id"`
	Secret     string            `json:"secret,omitempty"`
	Events     []string          `json:"events"`
	Headers    map[string]string `json:"headers,omitempty"`
	Active     bool              `json:"active"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type WebhookDelivery struct {
	ID           string    `json:"id"`
	WebhookID    string    `json:"webhook_id"`
	EventType    string    `json:"event_type"`
	URL          string    `json:"url"`
	Status       string    `json:"status"`
	StatusCode   int       `json:"status_code"`
	RequestBody  string    `json:"request_body"`
	ResponseBody string    `json:"response_body,omitempty"`
	DurationMS   int64     `json:"duration_ms"`
	Attempt      int       `json:"attempt"`
	MaxRetries   int       `json:"max_retries"`
	Error        string    `json:"error,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type WebhookHandler struct {
	logger     *zap.Logger
	webhooks   map[string]*Webhook
	deliveries map[string][]*WebhookDelivery
}

func NewWebhookHandler(logger *zap.Logger) *WebhookHandler {
	return &WebhookHandler{
		logger:     logger,
		webhooks:   make(map[string]*Webhook),
		deliveries: make(map[string][]*WebhookDelivery),
	}
}

type registerWebhookRequest struct {
	Name       string            `json:"name" binding:"required"`
	URL        string            `json:"url" binding:"required,url"`
	PipelineID string            `json:"pipeline_id" binding:"required"`
	Secret     string            `json:"secret,omitempty"`
	Events     []string          `json:"events" binding:"required,min=1"`
	Headers    map[string]string `json:"headers,omitempty"`
}

type updateWebhookRequest struct {
	Name    *string            `json:"name,omitempty"`
	URL     *string            `json:"url,omitempty"`
	Secret  *string            `json:"secret,omitempty"`
	Events  *[]string          `json:"events,omitempty"`
	Headers *map[string]string `json:"headers,omitempty"`
	Active  *bool              `json:"active,omitempty"`
}

type deliveryHistoryQuery struct {
	Page    int `form:"page"`
	PerPage int `form:"per_page"`
}

type retryDeliveryResponse struct {
	DeliveryID string    `json:"delivery_id"`
	Status     string    `json:"status"`
	RetriedAt  time.Time `json:"retried_at"`
	NewAttempt int       `json:"new_attempt"`
}

func (h *WebhookHandler) Register(c *gin.Context) {
	var req registerWebhookRequest
	if !bindJSON(c, &req) {
		return
	}

	now := time.Now().UTC()
	webhook := &Webhook{
		ID:         uuid.New().String(),
		Name:       req.Name,
		URL:        req.URL,
		PipelineID: req.PipelineID,
		Secret:     req.Secret,
		Events:     req.Events,
		Headers:    req.Headers,
		Active:     true,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if webhook.Headers == nil {
		webhook.Headers = make(map[string]string)
	}

	h.webhooks[webhook.ID] = webhook
	h.deliveries[webhook.ID] = make([]*WebhookDelivery, 0)

	h.logger.Info("webhook registered",
		zap.String("id", webhook.ID),
		zap.String("name", webhook.Name),
		zap.String("url", webhook.URL),
	)

	created(c, webhook)
}

func (h *WebhookHandler) List(c *gin.Context) {
	webhooks := make([]*Webhook, 0, len(h.webhooks))
	for _, wh := range h.webhooks {
		webhooks = append(webhooks, wh)
	}

	success(c, webhooks)
}

func (h *WebhookHandler) Get(c *gin.Context) {
	id := c.Param("id")

	wh, exists := h.webhooks[id]
	if !exists {
		notFound(c, "Webhook not found")
		return
	}

	success(c, wh)
}

func (h *WebhookHandler) Update(c *gin.Context) {
	id := c.Param("id")

	wh, exists := h.webhooks[id]
	if !exists {
		notFound(c, "Webhook not found")
		return
	}

	var req updateWebhookRequest
	if !bindJSON(c, &req) {
		return
	}

	if req.Name != nil {
		wh.Name = *req.Name
	}
	if req.URL != nil {
		wh.URL = *req.URL
	}
	if req.Secret != nil {
		wh.Secret = *req.Secret
	}
	if req.Events != nil {
		wh.Events = *req.Events
	}
	if req.Headers != nil {
		wh.Headers = *req.Headers
	}
	if req.Active != nil {
		wh.Active = *req.Active
	}
	wh.UpdatedAt = time.Now().UTC()

	success(c, wh)
}

func (h *WebhookHandler) Delete(c *gin.Context) {
	id := c.Param("id")

	if _, exists := h.webhooks[id]; !exists {
		notFound(c, "Webhook not found")
		return
	}

	delete(h.webhooks, id)
	delete(h.deliveries, id)

	h.logger.Info("webhook deleted", zap.String("id", id))
	noContent(c)
}

func (h *WebhookHandler) DeliveryHistory(c *gin.Context) {
	id := c.Param("id")

	if _, exists := h.webhooks[id]; !exists {
		notFound(c, "Webhook not found")
		return
	}

	var query deliveryHistoryQuery
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

	deliveries := h.deliveries[id]
	total := int64(len(deliveries))

	start := (page - 1) * perPage
	if start >= len(deliveries) {
		deliveries = make([]*WebhookDelivery, 0)
	} else {
		end := start + perPage
		if end > len(deliveries) {
			end = len(deliveries)
		}
		deliveries = deliveries[start:end]
	}

	if deliveries == nil {
		deliveries = make([]*WebhookDelivery, 0)
	}

	paginated(c, deliveries, page, perPage, total)
}

func (h *WebhookHandler) RetryDelivery(c *gin.Context) {
	id := c.Param("id")
	deliveryID := c.Param("delivery_id")

	if _, exists := h.webhooks[id]; !exists {
		notFound(c, "Webhook not found")
		return
	}

	deliveries, exists := h.deliveries[id]
	if !exists {
		notFound(c, "No deliveries found for this webhook")
		return
	}

	var delivery *WebhookDelivery
	for i, d := range deliveries {
		if d.ID == deliveryID {
			delivery = d
			deliveries[i].Attempt++
			deliveries[i].Status = "retrying"
			deliveries[i].Error = ""
			break
		}
	}

	if delivery == nil {
		notFound(c, "Delivery not found")
		return
	}

	h.logger.Info("webhook delivery retry scheduled",
		zap.String("webhook_id", id),
		zap.String("delivery_id", deliveryID),
		zap.Int("attempt", delivery.Attempt),
	)

	success(c, retryDeliveryResponse{
		DeliveryID: deliveryID,
		Status:     "retrying",
		RetriedAt:  time.Now().UTC(),
		NewAttempt: delivery.Attempt,
	})
}
