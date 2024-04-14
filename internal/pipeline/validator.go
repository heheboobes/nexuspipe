package pipeline

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"nexuspipe/internal/models"
)

type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

type ValidationResult struct {
	Valid  bool
	Errors []ValidationError
}

func (r ValidationResult) Error() string {
	msgs := make([]string, 0, len(r.Errors))
	for _, e := range r.Errors {
		msgs = append(msgs, e.Error())
	}
	return fmt.Sprintf("validation failed: %s", strings.Join(msgs, "; "))
}

type PipelineValidator struct {
	maxStages      int
	maxConcurrency int
	maxTimeout     time.Duration
	namePattern    *regexp.Regexp
	forbiddenTypes map[models.TaskType]bool
}

func NewPipelineValidator() *PipelineValidator {
	return &PipelineValidator{
		maxStages:      50,
		maxConcurrency: 10,
		maxTimeout:     3600 * time.Second,
		namePattern:    regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_\-\.]{1,63}$`),
		forbiddenTypes: make(map[models.TaskType]bool),
	}
}

func (v *PipelineValidator) ValidatePipeline(pipeline *models.Pipeline) *ValidationResult {
	errors := make([]ValidationError, 0)

	if pipeline == nil {
		return &ValidationResult{Errors: append(errors, ValidationError{
			Field: "pipeline", Message: "pipeline must not be nil",
		})}
	}

	nameErrs := v.validateName(pipeline.Name)
	errors = append(errors, nameErrs...)

	configErrs := v.validateConfig(&pipeline.Config)
	errors = append(errors, configErrs...)

	stages, err := v.extractStages(pipeline)
	if err == nil {
		stageErrs := v.validateStages(stages)
		errors = append(errors, stageErrs...)

		depErrs := v.validateDependencies(stages)
		errors = append(errors, depErrs...)

		cycleErrs := v.detectCycles(stages)
		errors = append(errors, cycleErrs...)

		uniquenessErrs := v.validateStageNames(stages)
		errors = append(errors, uniquenessErrs...)

		resourceErrs := v.validateResourceLimits(stages)
		errors = append(errors, resourceErrs...)
	}

	return &ValidationResult{
		Valid:  len(errors) == 0,
		Errors: errors,
	}
}

func (v *PipelineValidator) validateName(name string) []ValidationError {
	if strings.TrimSpace(name) == "" {
		return []ValidationError{{Field: "name", Message: "pipeline name must not be empty"}}
	}
	if len(name) > 64 {
		return []ValidationError{{Field: "name", Message: "pipeline name must be at most 64 characters"}}
	}
	if !v.namePattern.MatchString(name) {
		return []ValidationError{{Field: "name", Message: "pipeline name must start with a letter and contain only alphanumeric, underscore, hyphen, or dot characters (2-64 chars)"}}
	}
	return nil
}

func (v *PipelineValidator) validateConfig(cfg *models.PipelineConfig) []ValidationError {
	var errs []ValidationError

	if cfg.TimeoutSeconds <= 0 {
		errs = append(errs, ValidationError{Field: "config.timeout_seconds", Message: "timeout must be positive"})
	}
	if cfg.TimeoutSeconds > int(v.maxTimeout.Seconds()) {
		errs = append(errs, ValidationError{Field: "config.timeout_seconds", Message: fmt.Sprintf("timeout must not exceed %v", v.maxTimeout)})
	}
	if cfg.Concurrency <= 0 {
		errs = append(errs, ValidationError{Field: "config.concurrency", Message: "concurrency must be positive"})
	}
	if cfg.Concurrency > v.maxConcurrency {
		errs = append(errs, ValidationError{Field: "config.concurrency", Message: fmt.Sprintf("concurrency must not exceed %d", v.maxConcurrency)})
	}
	if cfg.MaxRetries < 0 {
		errs = append(errs, ValidationError{Field: "config.max_retries", Message: "max retries must not be negative"})
	}
	if cfg.MaxRetries > 25 {
		errs = append(errs, ValidationError{Field: "config.max_retries", Message: "max retries must not exceed 25"})
	}
	if cfg.Priority < 0 || cfg.Priority > 100 {
		errs = append(errs, ValidationError{Field: "config.priority", Message: "priority must be between 0 and 100"})
	}
	if strings.TrimSpace(cfg.QueueName) == "" {
		errs = append(errs, ValidationError{Field: "config.queue_name", Message: "queue name must not be empty"})
	}
	if cfg.RetryBackoffMS < 0 {
		errs = append(errs, ValidationError{Field: "config.retry_backoff_ms", Message: "retry backoff must not be negative"})
	}
	if cfg.BackoffMultiplier <= 0 {
		errs = append(errs, ValidationError{Field: "config.backoff_multiplier", Message: "backoff multiplier must be positive"})
	}

	return errs
}

func (v *PipelineValidator) validateStages(stages []Stage) []ValidationError {
	var errs []ValidationError

	if len(stages) == 0 {
		return append(errs, ValidationError{Field: "stages", Message: "pipeline must have at least one stage"})
	}
	if len(stages) > v.maxStages {
		return append(errs, ValidationError{Field: "stages", Message: fmt.Sprintf("pipeline must not exceed %d stages", v.maxStages)})
	}

	for i, stage := range stages {
		if strings.TrimSpace(stage.Name) == "" {
			errs = append(errs, ValidationError{Field: fmt.Sprintf("stages[%d].name", i), Message: "stage name must not be empty"})
		}
		if !stage.Type.IsValid() {
			errs = append(errs, ValidationError{Field: fmt.Sprintf("stages[%d].type", i), Message: fmt.Sprintf("invalid stage type: %s", stage.Type)})
		}
		if v.forbiddenTypes[stage.Type] {
			errs = append(errs, ValidationError{Field: fmt.Sprintf("stages[%d].type", i), Message: fmt.Sprintf("forbidden stage type: %s", stage.Type)})
		}
		if stage.Timeout < 0 {
			errs = append(errs, ValidationError{Field: fmt.Sprintf("stages[%d].timeout", i), Message: "stage timeout must not be negative"})
		}
		if stage.MaxRetry < 0 {
			errs = append(errs, ValidationError{Field: fmt.Sprintf("stages[%d].max_retry", i), Message: "max retry must not be negative"})
		}
		if stage.MaxRetry > 10 {
			errs = append(errs, ValidationError{Field: fmt.Sprintf("stages[%d].max_retry", i), Message: "max retry must not exceed 10"})
		}
	}

	return errs
}

func (v *PipelineValidator) validateDependencies(stages []Stage) []ValidationError {
	var errs []ValidationError
	stageNames := make(map[string]int)
	for i, s := range stages {
		stageNames[s.Name] = i
	}

	for i, stage := range stages {
		for _, dep := range stage.DependsOn {
			if _, exists := stageNames[dep]; !exists {
				errs = append(errs, ValidationError{
					Field:   fmt.Sprintf("stages[%d].depends_on", i),
					Message: fmt.Sprintf("stage %q depends on non-existent stage %q", stage.Name, dep),
				})
			}
		}
	}

	return errs
}

func (v *PipelineValidator) detectCycles(stages []Stage) []ValidationError {
	stageIndex := make(map[string]int)
	for i, s := range stages {
		stageIndex[s.Name] = i
	}

	graph := make([][]int, len(stages))
	for i, s := range stages {
		for _, dep := range s.DependsOn {
			if j, ok := stageIndex[dep]; ok {
				graph[i] = append(graph[i], j)
			}
		}
	}

	color := make([]int, len(stages))
	hasCycle := false

	var dfs func(u int)
	dfs = func(u int) {
		color[u] = 1
		for _, v := range graph[u] {
			if color[v] == 1 {
				hasCycle = true
				return
			}
			if color[v] == 0 {
				dfs(v)
			}
		}
		color[u] = 2
	}

	for i := range stages {
		if color[i] == 0 {
			dfs(i)
			if hasCycle {
				return []ValidationError{{
					Field:   "stages",
					Message: "circular dependency detected in stage graph",
				}}
			}
		}
	}

	return nil
}

func (v *PipelineValidator) validateStageNames(stages []Stage) []ValidationError {
	seen := make(map[string]int)
	var errs []ValidationError

	for i, stage := range stages {
		if existing, ok := seen[stage.Name]; ok {
			errs = append(errs, ValidationError{
				Field:   fmt.Sprintf("stages[%d].name", i),
				Message: fmt.Sprintf("duplicate stage name %q (also at index %d)", stage.Name, existing),
			})
		}
		seen[stage.Name] = i
	}

	return errs
}

func (v *PipelineValidator) validateResourceLimits(stages []Stage) []ValidationError {
	var errs []ValidationError
	for i, stage := range stages {
		if stage.Timeout > v.maxTimeout {
			errs = append(errs, ValidationError{
				Field:   fmt.Sprintf("stages[%d].timeout", i),
				Message: fmt.Sprintf("stage timeout %v exceeds maximum %v", stage.Timeout, v.maxTimeout),
			})
		}
	}
	return errs
}

func (v *PipelineValidator) extractStages(pipeline *models.Pipeline) ([]Stage, error) {
	return GetStagesFromPipeline(pipeline), nil
}

func (v *PipelineValidator) ForbidType(t models.TaskType) {
	v.forbiddenTypes[t] = true
}

func (v *PipelineValidator) AllowType(t models.TaskType) {
	delete(v.forbiddenTypes, t)
}
