package webhook

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type WebhookStatus string

const (
	WebhookStatusActive   WebhookStatus = "active"
	WebhookStatusInactive WebhookStatus = "inactive"
	WebhookStatusDisabled WebhookStatus = "disabled"
)

type Webhook struct {
	ID          uuid.UUID         `json:"id"`
	Name        string            `json:"name"`
	URL         string            `json:"url"`
	Secret      string            `json:"secret,omitempty"`
	Status      WebhookStatus     `json:"status"`
	Events      []string          `json:"events"`
	Headers     map[string]string `json:"headers,omitempty"`
	RetryConfig DeliveryConfig    `json:"retry_config"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type WebhookRegistration struct {
	Name        string            `json:"name"`
	URL         string            `json:"url"`
	Secret      string            `json:"secret,omitempty"`
	Events      []string          `json:"events"`
	Headers     map[string]string `json:"headers,omitempty"`
	RetryConfig *DeliveryConfig   `json:"retry_config,omitempty"`
}

type WebhookUpdate struct {
	Name        *string            `json:"name,omitempty"`
	URL         *string            `json:"url,omitempty"`
	Secret      *string            `json:"secret,omitempty"`
	Status      *WebhookStatus     `json:"status,omitempty"`
	Events      *[]string          `json:"events,omitempty"`
	Headers     *map[string]string `json:"headers,omitempty"`
	RetryConfig *DeliveryConfig    `json:"retry_config,omitempty"`
}

type Subscription struct {
	WebhookID uuid.UUID `json:"webhook_id"`
	EventType string    `json:"event_type"`
	Active    bool      `json:"active"`
}

type EventRouter struct {
	webhookID uuid.UUID
	name      string
	events    []string
	active    bool
}

type WebhookManager struct {
	mu          sync.RWMutex
	webhooks    map[uuid.UUID]*Webhook
	eventRoutes map[string][]EventRouter
	deliverer   *Deliverer
	history     *WebhookHistory
	logger      *zap.Logger
}

func NewWebhookManager(deliverer *Deliverer, history *WebhookHistory, logger *zap.Logger) *WebhookManager {
	return &WebhookManager{
		webhooks:    make(map[uuid.UUID]*Webhook),
		eventRoutes: make(map[string][]EventRouter),
		deliverer:   deliverer,
		history:     history,
		logger:      logger.With(zap.String("component", "webhook_manager")),
	}
}

func (m *WebhookManager) RegisterWebhook(reg WebhookRegistration) (*Webhook, error) {
	if reg.Name == "" {
		return nil, fmt.Errorf("webhook name is required")
	}
	if reg.URL == "" {
		return nil, fmt.Errorf("webhook URL is required")
	}
	if len(reg.Events) == 0 {
		return nil, fmt.Errorf("at least one event type is required")
	}

	now := time.Now().UTC()
	wh := &Webhook{
		ID:        uuid.New(),
		Name:      reg.Name,
		URL:       reg.URL,
		Secret:    reg.Secret,
		Status:    WebhookStatusActive,
		Events:    make([]string, len(reg.Events)),
		Headers:   make(map[string]string),
		CreatedAt: now,
		UpdatedAt: now,
	}

	if reg.RetryConfig != nil {
		wh.RetryConfig = *reg.RetryConfig
	} else {
		wh.RetryConfig = DefaultDeliveryConfig()
	}

	copy(wh.Events, reg.Events)
	for k, v := range reg.Headers {
		wh.Headers[k] = v
	}

	m.mu.Lock()
	m.webhooks[wh.ID] = wh
	m.indexEventRoutes(wh)
	m.mu.Unlock()

	m.logger.Info("webhook registered",
		zap.String("id", wh.ID.String()),
		zap.String("name", wh.Name),
		zap.String("url", wh.URL),
		zap.Int("events", len(wh.Events)),
	)

	return wh, nil
}

func (m *WebhookManager) UpdateWebhook(id uuid.UUID, update WebhookUpdate) (*Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wh, exists := m.webhooks[id]
	if !exists {
		return nil, fmt.Errorf("webhook not found: %s", id)
	}

	if update.Name != nil {
		wh.Name = *update.Name
	}
	if update.URL != nil {
		wh.URL = *update.URL
	}
	if update.Secret != nil {
		wh.Secret = *update.Secret
	}
	if update.Status != nil {
		wh.Status = *update.Status
	}
	if update.Events != nil {
		wh.Events = *update.Events
		m.rebuildEventRoutes()
	}
	if update.Headers != nil {
		wh.Headers = *update.Headers
	}
	if update.RetryConfig != nil {
		wh.RetryConfig = *update.RetryConfig
	}

	wh.UpdatedAt = time.Now().UTC()

	m.logger.Info("webhook updated",
		zap.String("id", id.String()),
		zap.String("name", wh.Name),
	)

	return wh, nil
}

func (m *WebhookManager) DeleteWebhook(id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.webhooks[id]; !exists {
		return fmt.Errorf("webhook not found: %s", id)
	}

	delete(m.webhooks, id)
	m.rebuildEventRoutes()

	m.logger.Info("webhook deleted",
		zap.String("id", id.String()),
	)

	return nil
}

func (m *WebhookManager) GetWebhook(id uuid.UUID) (*Webhook, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	wh, ok := m.webhooks[id]
	return wh, ok
}

func (m *WebhookManager) ListWebhooks() []*Webhook {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*Webhook, 0, len(m.webhooks))
	for _, wh := range m.webhooks {
		result = append(result, wh)
	}
	return result
}

func (m *WebhookManager) ListActiveWebhooks() []*Webhook {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*Webhook, 0)
	for _, wh := range m.webhooks {
		if wh.Status == WebhookStatusActive {
			result = append(result, wh)
		}
	}
	return result
}

func (m *WebhookManager) ActivateWebhook(id uuid.UUID) error {
	return m.setWebhookStatus(id, WebhookStatusActive)
}

func (m *WebhookManager) DeactivateWebhook(id uuid.UUID) error {
	return m.setWebhookStatus(id, WebhookStatusInactive)
}

func (m *WebhookManager) setWebhookStatus(id uuid.UUID, status WebhookStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	wh, exists := m.webhooks[id]
	if !exists {
		return fmt.Errorf("webhook not found: %s", id)
	}

	wh.Status = status
	wh.UpdatedAt = time.Now().UTC()

	m.logger.Info("webhook status changed",
		zap.String("id", id.String()),
		zap.String("status", string(status)),
	)

	return nil
}

func (m *WebhookManager) GetWebhooksForEvent(eventType string) []*Webhook {
	m.mu.RLock()
	defer m.mu.RUnlock()

	routers, exists := m.eventRoutes[eventType]
	if !exists {
		return nil
	}

	result := make([]*Webhook, 0, len(routers))
	for _, router := range routers {
		if !router.active {
			continue
		}
		if wh, ok := m.webhooks[router.webhookID]; ok && wh.Status == WebhookStatusActive {
			result = append(result, wh)
		}
	}

	return result
}

func (m *WebhookManager) SubscribeToEvents(webhookID uuid.UUID, events []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	wh, exists := m.webhooks[webhookID]
	if !exists {
		return fmt.Errorf("webhook not found: %s", webhookID)
	}

	wh.Events = events
	m.rebuildEventRoutes()

	return nil
}

func (m *WebhookManager) AddEventSubscription(webhookID uuid.UUID, eventType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	wh, exists := m.webhooks[webhookID]
	if !exists {
		return fmt.Errorf("webhook not found: %s", webhookID)
	}

	for _, e := range wh.Events {
		if e == eventType {
			return nil
		}
	}

	wh.Events = append(wh.Events, eventType)
	m.rebuildEventRoutes()

	return nil
}

func (m *WebhookManager) RemoveEventSubscription(webhookID uuid.UUID, eventType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	wh, exists := m.webhooks[webhookID]
	if !exists {
		return fmt.Errorf("webhook not found: %s", webhookID)
	}

	filtered := make([]string, 0, len(wh.Events))
	for _, e := range wh.Events {
		if e != eventType {
			filtered = append(filtered, e)
		}
	}
	wh.Events = filtered
	m.rebuildEventRoutes()

	return nil
}

func (m *WebhookManager) GetSubscriptions(webhookID uuid.UUID) []Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()

	wh, exists := m.webhooks[webhookID]
	if !exists {
		return nil
	}

	subscriptions := make([]Subscription, len(wh.Events))
	for i, eventType := range wh.Events {
		subscriptions[i] = Subscription{
			WebhookID: wh.ID,
			EventType: eventType,
			Active:    wh.Status == WebhookStatusActive,
		}
	}
	return subscriptions
}

func (m *WebhookManager) HasSubscribers(eventType string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	routers, exists := m.eventRoutes[eventType]
	if !exists {
		return false
	}

	for _, router := range routers {
		if router.active {
			wh, ok := m.webhooks[router.webhookID]
			if ok && wh.Status == WebhookStatusActive {
				return true
			}
		}
	}
	return false
}

func (m *WebhookManager) SubscriberCount(eventType string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	routers, exists := m.eventRoutes[eventType]
	if !exists {
		return 0
	}

	count := 0
	for _, router := range routers {
		if router.active {
			if wh, ok := m.webhooks[router.webhookID]; ok && wh.Status == WebhookStatusActive {
				count++
			}
		}
	}
	return count
}

func (m *WebhookManager) MatchWebhooks(eventType string) []*Webhook {
	return m.GetWebhooksForEvent(eventType)
}

func (m *WebhookManager) indexEventRoutes(wh *Webhook) {
	for _, eventType := range wh.Events {
		router := EventRouter{
			webhookID: wh.ID,
			name:      wh.Name,
			events:    wh.Events,
			active:    wh.Status == WebhookStatusActive,
		}
		m.eventRoutes[eventType] = append(m.eventRoutes[eventType], router)
	}
}

func (m *WebhookManager) rebuildEventRoutes() {
	m.eventRoutes = make(map[string][]EventRouter)
	for _, wh := range m.webhooks {
		m.indexEventRoutes(wh)
	}
}

func (m *WebhookManager) CountWebhooks() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.webhooks)
}

func (m *WebhookManager) CountActiveWebhooks() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, wh := range m.webhooks {
		if wh.Status == WebhookStatusActive {
			count++
		}
	}
	return count
}
