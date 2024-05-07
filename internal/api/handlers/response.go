package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type response struct {
	Success   bool          `json:"success"`
	Data      interface{}   `json:"data,omitempty"`
	Error     *apiError     `json:"error,omitempty"`
	Meta      *responseMeta `json:"meta,omitempty"`
	RequestID string        `json:"request_id,omitempty"`
}

type apiError struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details,omitempty"`
}

type responseMeta struct {
	Page       int   `json:"page"`
	PerPage    int   `json:"per_page"`
	Total      int64 `json:"total"`
	TotalPages int   `json:"total_pages"`
}

func reqID(c *gin.Context) string {
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

func success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, response{
		Success:   true,
		Data:      data,
		RequestID: reqID(c),
	})
}

func created(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, response{
		Success:   true,
		Data:      data,
		RequestID: reqID(c),
	})
}

func noContent(c *gin.Context) {
	c.JSON(http.StatusNoContent, response{
		Success:   true,
		RequestID: reqID(c),
	})
}

func paginated(c *gin.Context, data interface{}, page, perPage int, total int64) {
	totalPages := int(total) / perPage
	if int(total)%perPage != 0 {
		totalPages++
	}

	c.JSON(http.StatusOK, response{
		Success:   true,
		Data:      data,
		RequestID: reqID(c),
		Meta: &responseMeta{
			Page:       page,
			PerPage:    perPage,
			Total:      total,
			TotalPages: totalPages,
		},
	})
}

func sendError(c *gin.Context, status int, code, message string, details ...interface{}) {
	var det interface{}
	if len(details) > 0 {
		det = details[0]
	}

	c.JSON(status, response{
		Success:   false,
		Error:     &apiError{Code: code, Message: message, Details: det},
		RequestID: reqID(c),
	})
}

func badRequest(c *gin.Context, message string) {
	sendError(c, http.StatusBadRequest, "BAD_REQUEST", message)
}

func notFound(c *gin.Context, message string) {
	sendError(c, http.StatusNotFound, "NOT_FOUND", message)
}

func internalError(c *gin.Context, message string) {
	sendError(c, http.StatusInternalServerError, "INTERNAL_ERROR", message)
}

func conflict(c *gin.Context, message string) {
	sendError(c, http.StatusConflict, "CONFLICT", message)
}

func unauthorized(c *gin.Context, message string) {
	sendError(c, http.StatusUnauthorized, "UNAUTHORIZED", message)
}

func forbidden(c *gin.Context, message string) {
	sendError(c, http.StatusForbidden, "FORBIDDEN", message)
}

func notImplemented(c *gin.Context) {
	sendError(c, http.StatusNotImplemented, "NOT_IMPLEMENTED", "This endpoint is not yet implemented")
}

func bindJSON(c *gin.Context, obj interface{}) bool {
	if err := c.ShouldBindJSON(obj); err != nil {
		badRequest(c, "Invalid request body: "+err.Error())
		return false
	}
	return true
}

func bindQuery(c *gin.Context, obj interface{}) bool {
	if err := c.ShouldBindQuery(obj); err != nil {
		badRequest(c, "Invalid query parameters: "+err.Error())
		return false
	}
	return true
}

var serverStartTime = time.Now()
