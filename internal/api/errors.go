package api

import (
	"errors"
	"fmt"
	"net/http"
)

type ErrorCode string

const (
	ErrInternal         ErrorCode = "INTERNAL_ERROR"
	ErrNotFound         ErrorCode = "NOT_FOUND"
	ErrValidation       ErrorCode = "VALIDATION_ERROR"
	ErrUnauthorized     ErrorCode = "UNAUTHORIZED"
	ErrForbidden        ErrorCode = "FORBIDDEN"
	ErrConflict         ErrorCode = "CONFLICT"
	ErrRateLimited      ErrorCode = "RATE_LIMITED"
	ErrBadRequest       ErrorCode = "BAD_REQUEST"
	ErrPipelineExec     ErrorCode = "PIPELINE_EXECUTION_ERROR"
	ErrQueueUnavailable ErrorCode = "QUEUE_UNAVAILABLE"
	ErrDBUnavailable    ErrorCode = "DATABASE_UNAVAILABLE"
	ErrTimeout          ErrorCode = "TIMEOUT"
)

type APIError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Details any       `json:"details,omitempty"`
	status  int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *APIError) StatusCode() int {
	return e.status
}

func NewAPIError(code ErrorCode, message string, status int) *APIError {
	return &APIError{Code: code, Message: message, status: status}
}

func NotFound(resource string, id string) *APIError {
	return NewAPIError(ErrNotFound, fmt.Sprintf("%s '%s' not found", resource, id), http.StatusNotFound)
}

func ValidationError(details any) *APIError {
	return &APIError{
		Code:    ErrValidation,
		Message: "request validation failed",
		Details: details,
		status:  http.StatusBadRequest,
	}
}

func Conflict(resource string, id string) *APIError {
	return NewAPIError(ErrConflict, fmt.Sprintf("%s '%s' already exists", resource, id), http.StatusConflict)
}

func InternalError(err error) *APIError {
	return NewAPIError(ErrInternal, fmt.Sprintf("internal server error: %v", err), http.StatusInternalServerError)
}

func Unauthorized(msg string) *APIError {
	if msg == "" {
		msg = "authentication required"
	}
	return NewAPIError(ErrUnauthorized, msg, http.StatusUnauthorized)
}

func RateLimited(retryAfter int) *APIError {
	return &APIError{
		Code:    ErrRateLimited,
		Message: fmt.Sprintf("rate limit exceeded, retry after %d seconds", retryAfter),
		Details: map[string]int{"retry_after_seconds": retryAfter},
		status:  http.StatusTooManyRequests,
	}
}

func ClassifyError(err error) *APIError {
	if err == nil {
		return nil
	}

	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return NewAPIError(ErrTimeout, "request timed out", http.StatusGatewayTimeout)
	}

	return InternalError(err)
}

type ValidationErrors struct {
	Fields []FieldError `json:"fields"`
}

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Tag     string `json:"tag,omitempty"`
	Value   any    `json:"value,omitempty"`
}

func NewValidationErrors() *ValidationErrors {
	return &ValidationErrors{Fields: make([]FieldError, 0)}
}

func (v *ValidationErrors) Add(field, message, tag string, value any) {
	v.Fields = append(v.Fields, FieldError{
		Field:   field,
		Message: message,
		Tag:     tag,
		Value:   value,
	})
}

func (v *ValidationErrors) HasErrors() bool {
	return len(v.Fields) > 0
}

func (v *ValidationErrors) Error() string {
	return fmt.Sprintf("validation failed with %d errors", len(v.Fields))
}
