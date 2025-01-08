package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type DeliveryStatus string

const (
	DeliveryStatusPending    DeliveryStatus = "pending"
	DeliveryStatusDelivering DeliveryStatus = "delivering"
	DeliveryStatusSuccess    DeliveryStatus = "success"
	DeliveryStatusFailed     DeliveryStatus = "failed"
	DeliveryStatusRetrying   DeliveryStatus = "retrying"
	DeliveryStatusCancelled  DeliveryStatus = "cancelled"
)

func (s DeliveryStatus) IsTerminal() bool {
	return s == DeliveryStatusSuccess || s == DeliveryStatusFailed || s == DeliveryStatusCancelled
}

type DeliveryResult struct {
	StatusCode   int
	ResponseBody string
	Duration     time.Duration
	Attempt      int
	Error        error
}

type WebhookDelivery struct {
	ID           uuid.UUID       `json:"id"`
	WebhookID    uuid.UUID       `json:"webhook_id"`
	EventType    string          `json:"event_type"`
	URL          string          `json:"url"`
	Status       DeliveryStatus  `json:"status"`
	StatusCode   int             `json:"status_code"`
	RequestBody  json.RawMessage `json:"request_body"`
	ResponseBody string          `json:"response_body,omitempty"`
	DurationMS   int64           `json:"duration_ms"`
	Attempt      int             `json:"attempt"`
	MaxRetries   int             `json:"max_retries"`
	Error        string          `json:"error,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

type DeliveryConfig struct {
	TimeoutPerAttempt time.Duration
	MaxRetries        int
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
	JitterFactor      float64
}

func DefaultDeliveryConfig() DeliveryConfig {
	return DeliveryConfig{
		TimeoutPerAttempt: 30 * time.Second,
		MaxRetries:        3,
		BaseBackoff:       1 * time.Second,
		MaxBackoff:        60 * time.Second,
		JitterFactor:      0.2,
	}
}

type Deliverer struct {
	client *http.Client
	config DeliveryConfig
	logger *zap.Logger
}

func NewDeliverer(config DeliveryConfig, logger *zap.Logger) *Deliverer {
	client := &http.Client{
		Timeout: config.TimeoutPerAttempt,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return &Deliverer{
		client: client,
		config: config,
		logger: logger.With(zap.String("component", "webhook_deliverer")),
	}
}

func (d *Deliverer) Deliver(ctx context.Context, webhookID uuid.UUID, url, eventType string, secret string, headers map[string]string, payload interface{}) (*DeliveryResult, error) {
	body, err := serializePayload(payload)
	if err != nil {
		return nil, fmt.Errorf("serialize payload: %w", err)
	}

	signature := generateHMAC(body, secret)

	var lastResult *DeliveryResult
	for attempt := 1; attempt <= d.config.MaxRetries; attempt++ {
		if attempt > 1 {
			backoff := d.calculateBackoff(attempt)
			select {
			case <-ctx.Done():
				return lastResult, ctx.Err()
			case <-time.After(backoff):
			}
		}

		result := d.sendAttempt(ctx, webhookID, url, eventType, body, signature, headers, attempt)
		lastResult = result

		if result.Error == nil && result.StatusCode >= 200 && result.StatusCode < 300 {
			return result, nil
		}

		if result.Error != nil {
			d.logger.Warn("webhook delivery attempt failed",
				zap.String("webhook_id", webhookID.String()),
				zap.String("url", url),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", d.config.MaxRetries),
				zap.Int("status_code", result.StatusCode),
				zap.Error(result.Error),
			)
		}
	}

	return lastResult, fmt.Errorf("webhook delivery failed after %d attempts: %s", d.config.MaxRetries, lastResult.Error)
}

func (d *Deliverer) sendAttempt(ctx context.Context, webhookID uuid.UUID, url, eventType string, body []byte, signature string, headers map[string]string, attempt int) *DeliveryResult {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return &DeliveryResult{Attempt: attempt, Error: fmt.Errorf("create request: %w", err)}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "NexusPipe-Webhook/1.0")
	req.Header.Set("X-NexusPipe-Event-Type", eventType)
	req.Header.Set("X-NexusPipe-Delivery-Attempt", fmt.Sprintf("%d", attempt))
	req.Header.Set("X-NexusPipe-Webhook-ID", webhookID.String())

	if signature != "" {
		req.Header.Set("X-NexusPipe-Signature-256", signature)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := d.client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return &DeliveryResult{
			Attempt:  attempt,
			Duration: duration,
			Error:    fmt.Errorf("http request failed: %w", err),
		}
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return &DeliveryResult{
			StatusCode:   resp.StatusCode,
			ResponseBody: string(respBody),
			Duration:     duration,
			Attempt:      attempt,
		}
	}

	return &DeliveryResult{
		StatusCode:   resp.StatusCode,
		ResponseBody: string(respBody),
		Duration:     duration,
		Attempt:      attempt,
		Error:        fmt.Errorf("unexpected status: %d", resp.StatusCode),
	}
}

func (d *Deliverer) DeliverSync(ctx context.Context, webhookID uuid.UUID, url, eventType string, secret string, headers map[string]string, payload interface{}) (*DeliveryResult, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, d.config.TimeoutPerAttempt)
	defer cancel()

	body, err := serializePayload(payload)
	if err != nil {
		return nil, err
	}

	signature := generateHMAC(body, secret)
	return d.sendAttempt(timeoutCtx, webhookID, url, eventType, body, signature, headers, 1), nil
}

func (d *Deliverer) calculateBackoff(attempt int) time.Duration {
	delay := time.Duration(float64(d.config.BaseBackoff) * math.Pow(2, float64(attempt-2)))
	if delay > d.config.MaxBackoff {
		delay = d.config.MaxBackoff
	}
	if d.config.JitterFactor > 0 {
		jitter := time.Duration(float64(delay) * d.config.JitterFactor * (rand.Float64()*2 - 1))
		delay += jitter
		if delay < 0 {
			delay = d.config.BaseBackoff
		}
	}
	return delay
}

func serializePayload(payload interface{}) ([]byte, error) {
	switch v := payload.(type) {
	case []byte:
		return v, nil
	case json.RawMessage:
		return []byte(v), nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("json marshal: %w", err)
		}
		return data, nil
	}
}

func generateHMAC(payload []byte, secret string) string {
	if secret == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifyHMAC(payload []byte, secret string, signature string) bool {
	if secret == "" || signature == "" {
		return false
	}
	expected := generateHMAC(payload, secret)
	return hmac.Equal([]byte(expected), []byte(signature))
}
