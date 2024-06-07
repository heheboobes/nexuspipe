package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/models"
	"github.com/heheboobes/nexuspipe/internal/repository"
)

type TaskResult struct {
	TaskID     string
	Status     models.TaskStatus
	Output     []byte
	Error      string
	ErrorType  ErrorType
	Duration   time.Duration
	Retryable  bool
	RetryCount int
}

type ErrorType int

const (
	ErrorTypeUnknown ErrorType = iota
	ErrorTypeNetwork
	ErrorTypeTimeout
	ErrorTypeValidation
	ErrorTypeAuth
	ErrorTypeRateLimit
	ErrorTypeResource
	ErrorTypeInternal
)

func (e ErrorType) String() string {
	switch e {
	case ErrorTypeNetwork:
		return "network"
	case ErrorTypeTimeout:
		return "timeout"
	case ErrorTypeValidation:
		return "validation"
	case ErrorTypeAuth:
		return "auth"
	case ErrorTypeRateLimit:
		return "rate_limit"
	case ErrorTypeResource:
		return "resource"
	case ErrorTypeInternal:
		return "internal"
	default:
		return "unknown"
	}
}

func (e ErrorType) IsRetryable() bool {
	switch e {
	case ErrorTypeNetwork, ErrorTypeTimeout, ErrorTypeRateLimit, ErrorTypeResource:
		return true
	default:
		return false
	}
}

type TaskProcessor interface {
	ProcessTask(ctx context.Context, task *models.Task) (*TaskResult, error)
}

type DefaultProcessor struct {
	repo   *repository.TaskRepository
	logger *zap.Logger
	client *http.Client
}

func NewDefaultProcessor(repo *repository.TaskRepository, logger *zap.Logger) *DefaultProcessor {
	return &DefaultProcessor{
		repo:   repo,
		logger: logger.With(zap.String("component", "task_processor")),
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (p *DefaultProcessor) ProcessTask(ctx context.Context, task *models.Task) (*TaskResult, error) {
	start := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(task.Timeout)*time.Second)
	defer cancel()

	p.logger.Info("processing task",
		zap.String("task_id", task.ID.String()),
		zap.String("task_type", string(task.Type)),
		zap.String("task_name", task.Name),
	)

	var result *TaskResult

	switch task.Type {
	case models.TaskTypeHTTP:
		result = p.processHTTP(ctx, task)
	case models.TaskTypeGRPC:
		result = p.processGRPC(ctx, task)
	case models.TaskTypeSQL:
		result = p.processSQL(ctx, task)
	case models.TaskTypeScript:
		result = p.processScript(ctx, task)
	case models.TaskTypeShell:
		result = p.processShell(ctx, task)
	case models.TaskTypeWebhook:
		result = p.processWebhook(ctx, task)
	case models.TaskTypeTransform:
		result = p.processTransform(ctx, task)
	default:
		result = &TaskResult{
			TaskID:    task.ID.String(),
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("unsupported task type: %s", task.Type),
			ErrorType: ErrorTypeValidation,
			Duration:  time.Since(start),
		}
	}

	result.Duration = time.Since(start)
	result.TaskID = task.ID.String()

	if result.Error != "" {
		p.logger.Warn("task processing completed with error",
			zap.String("task_id", task.ID.String()),
			zap.String("error", result.Error),
			zap.String("error_type", result.ErrorType.String()),
			zap.Bool("retryable", result.Retryable),
		)
	} else {
		p.logger.Info("task processing completed successfully",
			zap.String("task_id", task.ID.String()),
			zap.Duration("duration", result.Duration),
		)
	}

	return result, nil
}

func (p *DefaultProcessor) processHTTP(ctx context.Context, task *models.Task) *TaskResult {
	cfg := task.Config
	method := strings.ToUpper(cfg.Method)
	if method == "" {
		method = http.MethodGet
	}

	var body io.Reader
	if cfg.Body != "" {
		body = strings.NewReader(cfg.Body)
	}

	req, err := http.NewRequestWithContext(ctx, method, cfg.URL, body)
	if err != nil {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("create request: %v", err),
			ErrorType: classifyError(err),
			Retryable: true,
		}
	}

	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("http request: %v", err),
			ErrorType: classifyError(err),
			Retryable: true,
		}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("read response: %v", err),
			ErrorType: ErrorTypeInternal,
			Retryable: false,
		}
	}

	if resp.StatusCode >= 500 {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("server error: %d %s", resp.StatusCode, string(respBody)),
			ErrorType: ErrorTypeNetwork,
			Retryable: true,
			Output:    respBody,
		}
	}

	if resp.StatusCode >= 400 {
		et := ErrorTypeValidation
		retryable := false
		if resp.StatusCode == http.StatusTooManyRequests {
			et = ErrorTypeRateLimit
			retryable = true
		}
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("client error: %d %s", resp.StatusCode, string(respBody)),
			ErrorType: et,
			Retryable: retryable,
			Output:    respBody,
		}
	}

	return &TaskResult{
		Status:    models.TaskStatusCompleted,
		Output:    respBody,
		Retryable: false,
	}
}

func (p *DefaultProcessor) processGRPC(ctx context.Context, task *models.Task) *TaskResult {
	return &TaskResult{
		Status:    models.TaskStatusFailed,
		Error:     "gRPC task processing not yet implemented",
		ErrorType: ErrorTypeInternal,
		Retryable: false,
	}
}

func (p *DefaultProcessor) processSQL(ctx context.Context, task *models.Task) *TaskResult {
	cfg := task.Config
	if cfg.SQL == "" {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     "empty SQL query",
			ErrorType: ErrorTypeValidation,
			Retryable: false,
		}
	}

	result, err := p.executeSQL(ctx, cfg)
	if err != nil {
		et := classifyError(err)
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("sql execution: %v", err),
			ErrorType: et,
			Retryable: et.IsRetryable(),
		}
	}

	return &TaskResult{
		Status:    models.TaskStatusCompleted,
		Output:    result,
		Retryable: false,
	}
}

func (p *DefaultProcessor) executeSQL(ctx context.Context, cfg models.TaskConfig) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"query":    cfg.SQL,
		"executed": true,
		"time":     time.Now().UTC(),
	})
}

func (p *DefaultProcessor) processScript(ctx context.Context, task *models.Task) *TaskResult {
	cfg := task.Config
	if cfg.Script == "" {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     "empty script content",
			ErrorType: ErrorTypeValidation,
			Retryable: false,
		}
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", cfg.Script)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		if errMsg == "" {
			errMsg = err.Error()
		}

		et := classifyError(err)
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("script execution: %s", errMsg),
			ErrorType: et,
			Retryable: et.IsRetryable(),
		}
	}

	return &TaskResult{
		Status:    models.TaskStatusCompleted,
		Output:    stdout.Bytes(),
		Retryable: false,
	}
}

func (p *DefaultProcessor) processShell(ctx context.Context, task *models.Task) *TaskResult {
	cfg := task.Config
	if cfg.Command == "" {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     "empty shell command",
			ErrorType: ErrorTypeValidation,
			Retryable: false,
		}
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", cfg.Command)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("shell command: %s", stderr.String()),
			ErrorType: classifyError(err),
			Retryable: true,
		}
	}

	return &TaskResult{
		Status:    models.TaskStatusCompleted,
		Output:    stdout.Bytes(),
		Retryable: false,
	}
}

func (p *DefaultProcessor) processWebhook(ctx context.Context, task *models.Task) *TaskResult {
	return p.processHTTP(ctx, task)
}

func (p *DefaultProcessor) processTransform(ctx context.Context, task *models.Task) *TaskResult {
	input := task.Input
	if len(input) == 0 {
		return &TaskResult{
			Status:    models.TaskStatusCompleted,
			Output:    nil,
			Retryable: false,
		}
	}

	var data interface{}
	if err := json.Unmarshal(input, &data); err != nil {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("unmarshal input: %v", err),
			ErrorType: ErrorTypeValidation,
			Retryable: false,
		}
	}

	output, err := json.Marshal(data)
	if err != nil {
		return &TaskResult{
			Status:    models.TaskStatusFailed,
			Error:     fmt.Sprintf("marshal output: %v", err),
			ErrorType: ErrorTypeInternal,
			Retryable: false,
		}
	}

	return &TaskResult{
		Status:    models.TaskStatusCompleted,
		Output:    output,
		Retryable: false,
	}
}

func classifyError(err error) ErrorType {
	if err == nil {
		return ErrorTypeUnknown
	}

	msg := strings.ToLower(err.Error())

	switch {
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return ErrorTypeTimeout
	case strings.Contains(msg, "connection") || strings.Contains(msg, "network"):
		return ErrorTypeNetwork
	case strings.Contains(msg, "no rows") || strings.Contains(msg, "not found"):
		return ErrorTypeValidation
	case strings.Contains(msg, "rate") || strings.Contains(msg, "too many"):
		return ErrorTypeRateLimit
	case strings.Contains(msg, "auth") || strings.Contains(msg, "unauthorized"):
		return ErrorTypeAuth
	case strings.Contains(msg, "resource") || strings.Contains(msg, "memory"):
		return ErrorTypeResource
	default:
		return ErrorTypeInternal
	}
}

var (
	_ sql.DB
	_ = uuid.New
)
