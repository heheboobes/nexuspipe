package webhook

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type HistoryFilter struct {
	WebhookID *uuid.UUID       `json:"webhook_id,omitempty"`
	Status    []DeliveryStatus `json:"status,omitempty"`
	EventType string           `json:"event_type,omitempty"`
	From      *time.Time       `json:"from,omitempty"`
	To        *time.Time       `json:"to,omitempty"`
	Page      int              `json:"page"`
	PerPage   int              `json:"per_page"`
	OrderBy   string           `json:"order_by"`
}

func DefaultHistoryFilter() HistoryFilter {
	return HistoryFilter{
		Page:    1,
		PerPage: 20,
		OrderBy: "created_at DESC",
	}
}

type HistoryEntry struct {
	Delivery    *WebhookDelivery `json:"delivery"`
	WebhookName string           `json:"webhook_name"`
	WebhookURL  string           `json:"webhook_url"`
}

type PaginatedResult struct {
	Entries    []HistoryEntry `json:"entries"`
	Total      int64          `json:"total"`
	Page       int            `json:"page"`
	PerPage    int            `json:"per_page"`
	TotalPages int            `json:"total_pages"`
}

type RetentionConfig struct {
	MaxEntries        int           `json:"max_entries"`
	RetentionDuration time.Duration `json:"retention_duration"`
	CleanupInterval   time.Duration `json:"cleanup_interval"`
}

func DefaultRetentionConfig() RetentionConfig {
	return RetentionConfig{
		MaxEntries:        10000,
		RetentionDuration: 7 * 24 * time.Hour,
		CleanupInterval:   1 * time.Hour,
	}
}

type WebhookHistory struct {
	mu        sync.RWMutex
	entries   []HistoryEntry
	byID      map[uuid.UUID]*HistoryEntry
	webhooks  map[uuid.UUID]WebhookSummary
	logger    *zap.Logger
	retention RetentionConfig
	stopCh    chan struct{}
}

type WebhookSummary struct {
	ID   uuid.UUID
	Name string
	URL  string
}

func NewWebhookHistory(retention RetentionConfig, logger *zap.Logger) *WebhookHistory {
	h := &WebhookHistory{
		entries:   make([]HistoryEntry, 0, 1024),
		byID:      make(map[uuid.UUID]*HistoryEntry),
		webhooks:  make(map[uuid.UUID]WebhookSummary),
		logger:    logger.With(zap.String("component", "webhook_history")),
		retention: retention,
		stopCh:    make(chan struct{}),
	}
	go h.retentionLoop()
	return h
}

func (h *WebhookHistory) RecordDelivery(delivery *WebhookDelivery, webhookName, webhookURL string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry := HistoryEntry{
		Delivery:    delivery,
		WebhookName: webhookName,
		WebhookURL:  webhookURL,
	}

	h.entries = append(h.entries, entry)
	h.byID[delivery.ID] = &h.entries[len(h.entries)-1]

	h.webhooks[delivery.WebhookID] = WebhookSummary{
		ID:   delivery.WebhookID,
		Name: webhookName,
		URL:  webhookURL,
	}

	h.logger.Debug("delivery recorded",
		zap.String("delivery_id", delivery.ID.String()),
		zap.String("webhook_id", delivery.WebhookID.String()),
		zap.String("status", string(delivery.Status)),
	)
}

func (h *WebhookHistory) GetDelivery(deliveryID uuid.UUID) (*HistoryEntry, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	entry, ok := h.byID[deliveryID]
	if !ok {
		return nil, false
	}
	return entry, true
}

func (h *WebhookHistory) GetHistory(filter HistoryFilter) PaginatedResult {
	h.mu.RLock()
	defer h.mu.RUnlock()

	filtered := h.applyFilter(filter)

	page := filter.Page
	if page < 1 {
		page = 1
	}
	perPage := filter.PerPage
	if perPage < 1 || perPage > 100 {
		perPage = 20
	}

	total := int64(len(filtered))
	totalPages := int(total) / perPage
	if int(total)%perPage != 0 {
		totalPages++
	}

	start := (page - 1) * perPage
	if start >= len(filtered) {
		return PaginatedResult{
			Entries:    []HistoryEntry{},
			Total:      total,
			Page:       page,
			PerPage:    perPage,
			TotalPages: totalPages,
		}
	}

	end := start + perPage
	if end > len(filtered) {
		end = len(filtered)
	}

	result := make([]HistoryEntry, end-start)
	copy(result, filtered[start:end])

	return PaginatedResult{
		Entries:    result,
		Total:      total,
		Page:       page,
		PerPage:    perPage,
		TotalPages: totalPages,
	}
}

func (h *WebhookHistory) GetDeliveriesByWebhook(webhookID uuid.UUID, filter HistoryFilter) PaginatedResult {
	filter.WebhookID = &webhookID
	return h.GetHistory(filter)
}

func (h *WebhookHistory) GetDeliveriesByStatus(status DeliveryStatus, filter HistoryFilter) PaginatedResult {
	filter.Status = []DeliveryStatus{status}
	return h.GetHistory(filter)
}

func (h *WebhookHistory) GetRecentDeliveries(limit int) []HistoryEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if limit <= 0 || limit > len(h.entries) {
		limit = len(h.entries)
	}

	result := make([]HistoryEntry, limit)
	for i := 0; i < limit; i++ {
		result[i] = h.entries[len(h.entries)-1-i]
	}
	return result
}

func (h *WebhookHistory) CountDeliveries(status DeliveryStatus) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	count := 0
	for _, entry := range h.entries {
		if entry.Delivery.Status == status {
			count++
		}
	}
	return count
}

func (h *WebhookHistory) applyFilter(filter HistoryFilter) []HistoryEntry {
	var filtered []HistoryEntry

	for _, entry := range h.entries {
		if filter.WebhookID != nil && entry.Delivery.WebhookID != *filter.WebhookID {
			continue
		}
		if len(filter.Status) > 0 {
			statusMatch := false
			for _, s := range filter.Status {
				if entry.Delivery.Status == s {
					statusMatch = true
					break
				}
			}
			if !statusMatch {
				continue
			}
		}
		if filter.EventType != "" && entry.Delivery.EventType != filter.EventType {
			continue
		}
		if filter.From != nil && entry.Delivery.CreatedAt.Before(*filter.From) {
			continue
		}
		if filter.To != nil && entry.Delivery.CreatedAt.After(*filter.To) {
			continue
		}
		filtered = append(filtered, entry)
	}

	h.sortEntries(filtered, filter.OrderBy)

	return filtered
}

func (h *WebhookHistory) sortEntries(entries []HistoryEntry, orderBy string) {
	sort.SliceStable(entries, func(i, j int) bool {
		switch orderBy {
		case "created_at ASC":
			return entries[i].Delivery.CreatedAt.Before(entries[j].Delivery.CreatedAt)
		case "duration_ms ASC":
			return entries[i].Delivery.DurationMS < entries[j].Delivery.DurationMS
		case "duration_ms DESC":
			return entries[i].Delivery.DurationMS > entries[j].Delivery.DurationMS
		case "status ASC":
			return entries[i].Delivery.Status < entries[j].Delivery.Status
		case "status DESC":
			return entries[i].Delivery.Status > entries[j].Delivery.Status
		default:
			return entries[i].Delivery.CreatedAt.After(entries[j].Delivery.CreatedAt)
		}
	})
}

func (h *WebhookHistory) UpdateDeliveryStatus(deliveryID uuid.UUID, status DeliveryStatus, statusCode int, responseBody, errMsg string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry, ok := h.byID[deliveryID]
	if !ok {
		return
	}

	entry.Delivery.Status = status
	entry.Delivery.StatusCode = statusCode
	entry.Delivery.ResponseBody = responseBody
	if errMsg != "" {
		entry.Delivery.Error = errMsg
	}
	entry.Delivery.UpdatedAt = time.Now().UTC()
}

func (h *WebhookHistory) UpdateDeliveryAttempt(deliveryID uuid.UUID, attempt int, durationMS int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry, ok := h.byID[deliveryID]
	if !ok {
		return
	}

	entry.Delivery.Attempt = attempt
	entry.Delivery.DurationMS = durationMS
	entry.Delivery.UpdatedAt = time.Now().UTC()
}

func (h *WebhookHistory) retentionLoop() {
	ticker := time.NewTicker(h.retention.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.enforceRetention()
		case <-h.stopCh:
			return
		}
	}
}

func (h *WebhookHistory) enforceRetention() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.entries) == 0 {
		return
	}

	cutoff := time.Now().UTC().Add(-h.retention.RetentionDuration)

	var retained []HistoryEntry
	newByID := make(map[uuid.UUID]*HistoryEntry)

	for _, entry := range h.entries {
		keepByAge := entry.Delivery.CreatedAt.After(cutoff)
		keepByCount := len(retained) < h.retention.MaxEntries

		if keepByAge && keepByCount {
			retained = append(retained, entry)
			newByID[entry.Delivery.ID] = &retained[len(retained)-1]
		}
	}

	h.entries = retained
	h.byID = newByID

	h.logger.Info("retention enforced",
		zap.Int("entries_after", len(h.entries)),
		zap.Int("entries_removed", len(h.entries)-len(retained)),
	)
}

func (h *WebhookHistory) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = make([]HistoryEntry, 0, 1024)
	h.byID = make(map[uuid.UUID]*HistoryEntry)
}

func (h *WebhookHistory) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

func (h *WebhookHistory) Stop() {
	close(h.stopCh)
}

func (h *WebhookHistory) GetWebhookStats(webhookID uuid.UUID) map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := map[string]interface{}{
		"total_deliveries": 0,
		"successful":       0,
		"failed":           0,
		"retrying":         0,
		"pending":          0,
		"avg_duration_ms":  float64(0),
		"last_delivery_at": nil,
	}

	var totalDuration int64
	var deliveryCount int

	for _, entry := range h.entries {
		if entry.Delivery.WebhookID != webhookID {
			continue
		}

		stats["total_deliveries"] = stats["total_deliveries"].(int) + 1

		switch entry.Delivery.Status {
		case DeliveryStatusSuccess:
			totalDuration += entry.Delivery.DurationMS
			deliveryCount++
			stats["successful"] = stats["successful"].(int) + 1
		case DeliveryStatusFailed:
			stats["failed"] = stats["failed"].(int) + 1
		case DeliveryStatusRetrying:
			stats["retrying"] = stats["retrying"].(int) + 1
		case DeliveryStatusPending:
			stats["pending"] = stats["pending"].(int) + 1
		}

		if stats["last_delivery_at"] == nil || entry.Delivery.CreatedAt.After(stats["last_delivery_at"].(time.Time)) {
			stats["last_delivery_at"] = entry.Delivery.CreatedAt
		}
	}

	if deliveryCount > 0 {
		stats["avg_duration_ms"] = float64(totalDuration) / float64(deliveryCount)
	}

	return stats
}

func (h *WebhookHistory) ExportToJSON() (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	data, err := json.Marshal(h.entries)
	if err != nil {
		return "", fmt.Errorf("marshal history: %w", err)
	}
	return string(data), nil
}
