package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/models"
)

type HandlerFunc func(ctx context.Context, task *models.Task) (*TaskResult, error)

type RegisteredHandler struct {
	Name        string
	Handler     HandlerFunc
	Description string
	TaskType    models.TaskType
	Version     string
	Validators  []HandlerValidator
}

type HandlerValidator func(task *models.Task) error

type HandlerRegistry struct {
	handlers map[models.TaskType]*RegisteredHandler
	mu       sync.RWMutex
	logger   *zap.Logger
}

func NewHandlerRegistry(logger *zap.Logger) *HandlerRegistry {
	registry := &HandlerRegistry{
		handlers: make(map[models.TaskType]*RegisteredHandler),
		logger:   logger.With(zap.String("component", "handler_registry")),
	}

	registry.registerBuiltins()

	return registry
}

func (r *HandlerRegistry) Register(handler *RegisteredHandler) error {
	if handler == nil {
		return fmt.Errorf("cannot register nil handler")
	}
	if handler.TaskType == "" {
		return fmt.Errorf("handler task type is required")
	}
	if handler.Handler == nil {
		return fmt.Errorf("handler function is required for %s", handler.TaskType)
	}
	if handler.Name == "" {
		handler.Name = string(handler.TaskType)
	}

	handler.TaskType = models.TaskType(handler.TaskType)

	if !handler.TaskType.IsValid() {
		return fmt.Errorf("invalid task type: %s", handler.TaskType)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[handler.TaskType]; exists {
		return fmt.Errorf("handler already registered for task type: %s", handler.TaskType)
	}

	r.handlers[handler.TaskType] = handler

	r.logger.Info("handler registered",
		zap.String("name", handler.Name),
		zap.String("task_type", string(handler.TaskType)),
		zap.String("version", handler.Version),
	)

	return nil
}

func (r *HandlerRegistry) RegisterWithValidation(handler *RegisteredHandler, validators ...HandlerValidator) error {
	handler.Validators = append(handler.Validators, validators...)
	return r.Register(handler)
}

func (r *HandlerRegistry) Get(taskType models.TaskType) (*RegisteredHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, exists := r.handlers[taskType]
	if !exists {
		return nil, fmt.Errorf("no handler registered for task type: %s", taskType)
	}

	return handler, nil
}

func (r *HandlerRegistry) MustGet(taskType models.TaskType) *RegisteredHandler {
	handler, err := r.Get(taskType)
	if err != nil {
		panic(err)
	}
	return handler
}

func (r *HandlerRegistry) Has(taskType models.TaskType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.handlers[taskType]
	return exists
}

func (r *HandlerRegistry) List() []*RegisteredHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handlers := make([]*RegisteredHandler, 0, len(r.handlers))
	for _, h := range r.handlers {
		handlers = append(handlers, h)
	}
	return handlers
}

func (r *HandlerRegistry) Types() []models.TaskType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]models.TaskType, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

func (r *HandlerRegistry) Unregister(taskType models.TaskType) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[taskType]; !exists {
		return fmt.Errorf("no handler registered for task type: %s", taskType)
	}

	delete(r.handlers, taskType)

	r.logger.Info("handler unregistered",
		zap.String("task_type", string(taskType)),
	)

	return nil
}

func (r *HandlerRegistry) Validate(task *models.Task) error {
	handler, err := r.Get(task.Type)
	if err != nil {
		return fmt.Errorf("validate task %s: %w", task.ID, err)
	}

	for _, validator := range handler.Validators {
		if err := validator(task); err != nil {
			return fmt.Errorf("validation failed for task %s (%s): %w",
				task.ID, task.Type, err)
		}
	}

	return nil
}

func (r *HandlerRegistry) Execute(ctx context.Context, task *models.Task) (*TaskResult, error) {
	handler, err := r.Get(task.Type)
	if err != nil {
		return nil, err
	}

	if err := r.Validate(task); err != nil {
		return nil, err
	}

	return handler.Handler(ctx, task)
}

func (r *HandlerRegistry) registerBuiltins() {
	httpHandler := &RegisteredHandler{
		Name:        "http_handler",
		TaskType:    models.TaskTypeHTTP,
		Description: "Handles HTTP request tasks",
		Version:     "1.0.0",
		Handler: func(ctx context.Context, task *models.Task) (*TaskResult, error) {
			return nil, fmt.Errorf("http handler: use DefaultProcessor for execution")
		},
		Validators: []HandlerValidator{
			func(task *models.Task) error {
				if task.Config.URL == "" {
					return fmt.Errorf("http task requires a URL")
				}
				return nil
			},
			func(task *models.Task) error {
				method := task.Config.Method
				if method != "" && method != "GET" && method != "POST" &&
					method != "PUT" && method != "DELETE" && method != "PATCH" &&
					method != "HEAD" && method != "OPTIONS" {
					return fmt.Errorf("invalid HTTP method: %s", method)
				}
				return nil
			},
		},
	}
	r.handlers[httpHandler.TaskType] = httpHandler

	grpcHandler := &RegisteredHandler{
		Name:        "grpc_handler",
		TaskType:    models.TaskTypeGRPC,
		Description: "Handles gRPC call tasks",
		Version:     "1.0.0",
		Handler: func(ctx context.Context, task *models.Task) (*TaskResult, error) {
			return nil, fmt.Errorf("grpc handler: use DefaultProcessor for execution")
		},
		Validators: []HandlerValidator{
			func(task *models.Task) error {
				if task.Config.ServiceName == "" {
					return fmt.Errorf("gRPC task requires a service name")
				}
				if task.Config.Endpoint == "" {
					return fmt.Errorf("gRPC task requires an endpoint")
				}
				return nil
			},
		},
	}
	r.handlers[grpcHandler.TaskType] = grpcHandler

	sqlHandler := &RegisteredHandler{
		Name:        "sql_handler",
		TaskType:    models.TaskTypeSQL,
		Description: "Handles SQL query execution tasks",
		Version:     "1.0.0",
		Handler: func(ctx context.Context, task *models.Task) (*TaskResult, error) {
			return nil, fmt.Errorf("sql handler: use DefaultProcessor for execution")
		},
		Validators: []HandlerValidator{
			func(task *models.Task) error {
				if task.Config.SQL == "" {
					return fmt.Errorf("SQL task requires a query")
				}
				return nil
			},
		},
	}
	r.handlers[sqlHandler.TaskType] = sqlHandler

	scriptHandler := &RegisteredHandler{
		Name:        "script_handler",
		TaskType:    models.TaskTypeScript,
		Description: "Handles script execution tasks",
		Version:     "1.0.0",
		Handler: func(ctx context.Context, task *models.Task) (*TaskResult, error) {
			return nil, fmt.Errorf("script handler: use DefaultProcessor for execution")
		},
		Validators: []HandlerValidator{
			func(task *models.Task) error {
				if task.Config.Script == "" {
					return fmt.Errorf("script task requires script content")
				}
				return nil
			},
		},
	}
	r.handlers[scriptHandler.TaskType] = scriptHandler

	shellHandler := &RegisteredHandler{
		Name:        "shell_handler",
		TaskType:    models.TaskTypeShell,
		Description: "Handles shell command execution tasks",
		Version:     "1.0.0",
		Handler: func(ctx context.Context, task *models.Task) (*TaskResult, error) {
			return nil, fmt.Errorf("shell handler: use DefaultProcessor for execution")
		},
		Validators: []HandlerValidator{
			func(task *models.Task) error {
				if task.Config.Command == "" {
					return fmt.Errorf("shell task requires a command")
				}
				return nil
			},
		},
	}
	r.handlers[shellHandler.TaskType] = shellHandler

	webhookHandler := &RegisteredHandler{
		Name:        "webhook_handler",
		TaskType:    models.TaskTypeWebhook,
		Description: "Handles webhook trigger tasks",
		Version:     "1.0.0",
		Handler: func(ctx context.Context, task *models.Task) (*TaskResult, error) {
			return nil, fmt.Errorf("webhook handler: use DefaultProcessor for execution")
		},
		Validators: []HandlerValidator{
			func(task *models.Task) error {
				if task.Config.URL == "" {
					return fmt.Errorf("webhook task requires a URL")
				}
				return nil
			},
		},
	}
	r.handlers[webhookHandler.TaskType] = webhookHandler

	transformHandler := &RegisteredHandler{
		Name:        "transform_handler",
		TaskType:    models.TaskTypeTransform,
		Description: "Handles data transformation tasks",
		Version:     "1.0.0",
		Handler: func(ctx context.Context, task *models.Task) (*TaskResult, error) {
			return nil, fmt.Errorf("transform handler: use DefaultProcessor for execution")
		},
	}
	r.handlers[transformHandler.TaskType] = transformHandler

	notificationHandler := &RegisteredHandler{
		Name:        "notification_handler",
		TaskType:    models.TaskTypeNotification,
		Description: "Handles notification dispatch tasks",
		Version:     "1.0.0",
		Handler: func(ctx context.Context, task *models.Task) (*TaskResult, error) {
			return &TaskResult{
				TaskID:    task.ID.String(),
				Status:    models.TaskStatusCompleted,
				Retryable: false,
			}, nil
		},
		Validators: []HandlerValidator{
			func(task *models.Task) error {
				if task.Config.NotifyChannel == "" {
					return fmt.Errorf("notification task requires a notify channel")
				}
				return nil
			},
		},
	}
	r.handlers[notificationHandler.TaskType] = notificationHandler

	r.logger.Info("built-in handlers registered",
		zap.Int("count", len(r.handlers)),
	)
}

var _ = uuid.New
