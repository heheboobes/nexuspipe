package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type APIErrorCode string

const (
	ErrCodeValidation       APIErrorCode = "VALIDATION_ERROR"
	ErrCodeNotFound         APIErrorCode = "NOT_FOUND"
	ErrCodeConflict         APIErrorCode = "CONFLICT"
	ErrCodeInternal         APIErrorCode = "INTERNAL_ERROR"
	ErrCodeUnauthorized     APIErrorCode = "UNAUTHORIZED"
	ErrCodeForbidden        APIErrorCode = "FORBIDDEN"
	ErrCodeRateLimited      APIErrorCode = "RATE_LIMITED"
	ErrCodeBadRequest       APIErrorCode = "BAD_REQUEST"
	ErrCodeDependencyFailed APIErrorCode = "DEPENDENCY_FAILED"
	ErrCodeConcurrentMod    APIErrorCode = "CONCURRENT_MODIFICATION"
)

type APIResponse struct {
	Success   bool          `json:"success"`
	Data      interface{}   `json:"data,omitempty"`
	Error     *APIError     `json:"error,omitempty"`
	Meta      *ResponseMeta `json:"meta,omitempty"`
	RequestID string        `json:"request_id,omitempty"`
}

type APIError struct {
	Code    APIErrorCode `json:"code"`
	Message string       `json:"message"`
	Details interface{}  `json:"details,omitempty"`
}

type ResponseMeta struct {
	Page       int   `json:"page"`
	PerPage    int   `json:"per_page"`
	Total      int64 `json:"total"`
	TotalPages int   `json:"total_pages"`
}

type ValidationErrorDetail struct {
	Field   string `json:"field"`
	Tag     string `json:"tag"`
	Message string `json:"message"`
}

func requestIDFromContext(c *gin.Context) string {
	if id, exists := c.Get("request_id"); exists {
		if rid, ok := id.(string); ok {
			return rid
		}
	}
	if id := c.GetHeader("X-Request-ID"); id != "" {
		return id
	}
	return uuid.New().String()
}

func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, APIResponse{
		Success:   true,
		Data:      data,
		RequestID: requestIDFromContext(c),
	})
}

func Created(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, APIResponse{
		Success:   true,
		Data:      data,
		RequestID: requestIDFromContext(c),
	})
}

func NoContent(c *gin.Context) {
	c.JSON(http.StatusNoContent, APIResponse{
		Success:   true,
		RequestID: requestIDFromContext(c),
	})
}

func Paginated(c *gin.Context, data interface{}, page, perPage int, total int64) {
	totalPages := int(total) / perPage
	if int(total)%perPage != 0 {
		totalPages++
	}

	c.JSON(http.StatusOK, APIResponse{
		Success:   true,
		Data:      data,
		RequestID: requestIDFromContext(c),
		Meta: &ResponseMeta{
			Page:       page,
			PerPage:    perPage,
			Total:      total,
			TotalPages: totalPages,
		},
	})
}

func Error(c *gin.Context, status int, code APIErrorCode, message string, details ...interface{}) {
	var det interface{}
	if len(details) > 0 {
		det = details[0]
	}

	c.JSON(status, APIResponse{
		Success:   false,
		Error:     &APIError{Code: code, Message: message, Details: det},
		RequestID: requestIDFromContext(c),
	})
}

func ValidationError(c *gin.Context, message string, details []ValidationErrorDetail) {
	Error(c, http.StatusBadRequest, ErrCodeValidation, message, details)
}

func NotFound(c *gin.Context, message string) {
	Error(c, http.StatusNotFound, ErrCodeNotFound, message)
}

func Conflict(c *gin.Context, message string) {
	Error(c, http.StatusConflict, ErrCodeConflict, message)
}

func InternalError(c *gin.Context, message string) {
	Error(c, http.StatusInternalServerError, ErrCodeInternal, message)
}

func BadRequest(c *gin.Context, message string) {
	Error(c, http.StatusBadRequest, ErrCodeBadRequest, message)
}

func Unauthorized(c *gin.Context, message string) {
	Error(c, http.StatusUnauthorized, ErrCodeUnauthorized, message)
}

func Forbidden(c *gin.Context, message string) {
	Error(c, http.StatusForbidden, ErrCodeForbidden, message)
}

func ConcurrentModification(c *gin.Context, message string) {
	Error(c, http.StatusConflict, ErrCodeConcurrentMod, message)
}

func BindJSON(c *gin.Context, obj interface{}) bool {
	if err := c.ShouldBindJSON(obj); err != nil {
		BadRequest(c, "Invalid request body: "+err.Error())
		return false
	}
	return true
}

func BindQuery(c *gin.Context, obj interface{}) bool {
	if err := c.ShouldBindQuery(obj); err != nil {
		BadRequest(c, "Invalid query parameters: "+err.Error())
		return false
	}
	return true
}

func BindURI(c *gin.Context, obj interface{}) bool {
	if err := c.ShouldBindUri(obj); err != nil {
		BadRequest(c, "Invalid URI parameters: "+err.Error())
		return false
	}
	return true
}
