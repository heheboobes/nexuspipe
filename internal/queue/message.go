package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageType string

const (
	MessageTypeEvent        MessageType = "event"
	MessageTypeCommand      MessageType = "command"
	MessageTypeNotification MessageType = "notification"
	MessageTypeHeartbeat    MessageType = "heartbeat"
	MessageTypeError        MessageType = "error"
	MessageTypeAck          MessageType = "ack"
	MessageTypeRequest      MessageType = "request"
	MessageTypeResponse     MessageType = "response"
)

var (
	ErrInvalidMessageType = errors.New("invalid message type")
	ErrInvalidPayload     = errors.New("invalid message payload")
)

type Headers map[string]interface{}

func (h Headers) Get(key string) (interface{}, bool) {
	v, ok := h[key]
	return v, ok
}

func (h Headers) Set(key string, value interface{}) {
	h[key] = value
}

func (h Headers) String(key string) string {
	if v, ok := h[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

type Envelope struct {
	ID            string      `json:"id"`
	Type          MessageType `json:"type"`
	Source        string      `json:"source,omitempty"`
	Version       string      `json:"version,omitempty"`
	Timestamp     time.Time   `json:"timestamp"`
	Headers       Headers     `json:"headers,omitempty"`
	Payload       []byte      `json:"payload"`
	ContentType   string      `json:"content_type,omitempty"`
	Encoding      string      `json:"encoding,omitempty"`
	CorrelationID string      `json:"correlation_id,omitempty"`
	ReplyTo       string      `json:"reply_to,omitempty"`
	Priority      int         `json:"priority,omitempty"`
	TTL           int         `json:"ttl,omitempty"`
	RetryCount    int         `json:"retry_count,omitempty"`
}

type DeliveryInfo struct {
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string
	Queue       string
}

func NewEnvelope(msgType MessageType, payload []byte) *Envelope {
	return &Envelope{
		Type:      msgType,
		Timestamp: time.Now().UTC(),
		Headers:   make(Headers),
		Payload:   payload,
	}
}

func NewEnvelopeWithID(id string, msgType MessageType, payload []byte) *Envelope {
	return &Envelope{
		ID:        id,
		Type:      msgType,
		Timestamp: time.Now().UTC(),
		Headers:   make(Headers),
		Payload:   payload,
	}
}

func (e *Envelope) Validate() error {
	if e.Type == "" {
		return fmt.Errorf("%w: type is empty", ErrInvalidMessageType)
	}
	if e.Payload == nil {
		return fmt.Errorf("%w: payload is nil", ErrInvalidPayload)
	}
	return nil
}

func (e *Envelope) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}

	if e.Headers == nil {
		e.Headers = make(Headers)
	}

	e.Headers.Set("x-message-type", string(e.Type))
	e.Headers.Set("x-message-version", e.Version)

	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}
	return data, nil
}

func (e *Envelope) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("%w: empty data", ErrInvalidPayload)
	}

	if err := json.Unmarshal(data, e); err != nil {
		return fmt.Errorf("unmarshal envelope: %w", err)
	}

	if e.Headers == nil {
		e.Headers = make(Headers)
	}

	return e.Validate()
}

func (e *Envelope) DecodePayload(v interface{}) error {
	if len(e.Payload) == 0 {
		return fmt.Errorf("%w: empty payload", ErrInvalidPayload)
	}
	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	return nil
}

func (e *Envelope) WithHeader(key string, value interface{}) *Envelope {
	if e.Headers == nil {
		e.Headers = make(Headers)
	}
	e.Headers.Set(key, value)
	return e
}

func (e *Envelope) WithCorrelationID(id string) *Envelope {
	e.CorrelationID = id
	return e
}

func (e *Envelope) WithReplyTo(queue string) *Envelope {
	e.ReplyTo = queue
	return e
}

func (e *Envelope) WithTTL(ttl int) *Envelope {
	e.TTL = ttl
	return e
}

func (e *Envelope) WithPriority(p int) *Envelope {
	e.Priority = p
	return e
}

func (e *Envelope) Clone() *Envelope {
	headers := make(Headers, len(e.Headers))
	for k, v := range e.Headers {
		headers[k] = v
	}

	payload := make([]byte, len(e.Payload))
	copy(payload, e.Payload)

	return &Envelope{
		ID:            e.ID,
		Type:          e.Type,
		Source:        e.Source,
		Version:       e.Version,
		Timestamp:     e.Timestamp,
		Headers:       headers,
		Payload:       payload,
		ContentType:   e.ContentType,
		Encoding:      e.Encoding,
		CorrelationID: e.CorrelationID,
		ReplyTo:       e.ReplyTo,
		Priority:      e.Priority,
		TTL:           e.TTL,
		RetryCount:    e.RetryCount,
	}
}

func Serialize(msgType MessageType, payload interface{}) (*Envelope, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("serialize payload: %w", err)
	}
	return NewEnvelope(msgType, data), nil
}

func Deserialize(data []byte) (*Envelope, error) {
	env := &Envelope{}
	if err := env.Unmarshal(data); err != nil {
		return nil, err
	}
	return env, nil
}

func EnvelopeFromDelivery(d *amqp.Delivery) (*Envelope, *DeliveryInfo, error) {
	env, err := Deserialize(d.Body)
	if err != nil {
		return nil, nil, err
	}

	info := &DeliveryInfo{
		ConsumerTag: d.ConsumerTag,
		DeliveryTag: d.DeliveryTag,
		Redelivered: d.Redelivered,
		Exchange:    d.Exchange,
		RoutingKey:  d.RoutingKey,
	}

	return env, info, nil
}
