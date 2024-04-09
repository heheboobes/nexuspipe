package pipeline

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"nexuspipe/internal/models"
)

type PipelineBuilder struct {
	pipeline *models.Pipeline
	stages   []Stage
	errors   []error
}

func NewPipelineBuilder(name string) *PipelineBuilder {
	return &PipelineBuilder{
		pipeline: models.NewPipeline(name, uuid.Nil),
		stages:   make([]Stage, 0),
	}
}

func NewPipelineBuilderFromExisting(p *models.Pipeline) *PipelineBuilder {
	existing := *p
	return &PipelineBuilder{
		pipeline: &existing,
		stages:   GetStagesFromPipeline(p),
	}
}

func (b *PipelineBuilder) SetName(name string) *PipelineBuilder {
	if name == "" {
		b.errors = append(b.errors, fmt.Errorf("pipeline name must not be empty"))
		return b
	}
	b.pipeline.Name = name
	return b
}

func (b *PipelineBuilder) SetDescription(desc string) *PipelineBuilder {
	b.pipeline.Description = desc
	return b
}

func (b *PipelineBuilder) SetStatus(status models.PipelineStatus) *PipelineBuilder {
	if !status.IsValid() {
		b.errors = append(b.errors, fmt.Errorf("invalid pipeline status: %s", status))
		return b
	}
	b.pipeline.Status = status
	return b
}

func (b *PipelineBuilder) WithConfig(cfg models.PipelineConfig) *PipelineBuilder {
	b.pipeline.Config = cfg
	return b
}

func (b *PipelineBuilder) WithConfigModifier(fn func(cfg *models.PipelineConfig)) *PipelineBuilder {
	fn(&b.pipeline.Config)
	return b
}

func (b *PipelineBuilder) SetMaxRetries(retries int) *PipelineBuilder {
	if retries < 0 {
		b.errors = append(b.errors, fmt.Errorf("max retries must not be negative"))
		return b
	}
	b.pipeline.Config.MaxRetries = retries
	return b
}

func (b *PipelineBuilder) SetTimeout(seconds int) *PipelineBuilder {
	if seconds <= 0 {
		b.errors = append(b.errors, fmt.Errorf("timeout must be positive"))
		return b
	}
	b.pipeline.Config.TimeoutSeconds = seconds
	return b
}

func (b *PipelineBuilder) SetConcurrency(c int) *PipelineBuilder {
	if c <= 0 {
		b.errors = append(b.errors, fmt.Errorf("concurrency must be positive"))
		return b
	}
	b.pipeline.Config.Concurrency = c
	return b
}

func (b *PipelineBuilder) SetPriority(p int) *PipelineBuilder {
	if p < 0 || p > 100 {
		b.errors = append(b.errors, fmt.Errorf("priority must be between 0 and 100"))
		return b
	}
	b.pipeline.Config.Priority = p
	return b
}

func (b *PipelineBuilder) SetQueueName(name string) *PipelineBuilder {
	b.pipeline.Config.QueueName = name
	return b
}

func (b *PipelineBuilder) SetCreatedBy(userID uuid.UUID) *PipelineBuilder {
	b.pipeline.CreatedBy = userID
	return b
}

func (b *PipelineBuilder) AddTag(key, value string) *PipelineBuilder {
	if b.pipeline.Tags == nil {
		b.pipeline.Tags = make(map[string]string)
	}
	b.pipeline.Tags[key] = value
	return b
}

func (b *PipelineBuilder) AddStage(stage Stage) *PipelineBuilder {
	if stage.Name == "" {
		if b.pipeline.Name != "" {
			stage.Name = fmt.Sprintf("%s-stage-%d", b.pipeline.Name, len(b.stages)+1)
		} else {
			stage.Name = fmt.Sprintf("stage-%d", len(b.stages)+1)
		}
	}
	if stage.ID == uuid.Nil {
		stage.ID = uuid.New()
	}
	stage.Sequence = len(b.stages)
	b.stages = append(b.stages, stage)
	return b
}

func (b *PipelineBuilder) AddHTTPStage(name, url, method string) *PipelineBuilder {
	return b.AddStage(Stage{
		Name: name,
		Type: models.TaskTypeHTTP,
		Config: models.TaskConfig{
			URL:    url,
			Method: method,
		},
		Timeout:  time.Duration(b.pipeline.Config.TimeoutSeconds) * time.Second,
		MaxRetry: b.pipeline.Config.MaxRetries,
	})
}

func (b *PipelineBuilder) AddSQLStage(name, query string) *PipelineBuilder {
	return b.AddStage(Stage{
		Name: name,
		Type: models.TaskTypeSQL,
		Config: models.TaskConfig{
			SQL: query,
		},
		Timeout:  time.Duration(b.pipeline.Config.TimeoutSeconds) * time.Second,
		MaxRetry: b.pipeline.Config.MaxRetries,
	})
}

func (b *PipelineBuilder) AddTransformStage(name, transform string) *PipelineBuilder {
	return b.AddStage(Stage{
		Name: name,
		Type: models.TaskTypeTransform,
		Config: models.TaskConfig{
			Transform: transform,
		},
		Timeout:  time.Duration(b.pipeline.Config.TimeoutSeconds) * time.Second,
		MaxRetry: 0,
	})
}

func (b *PipelineBuilder) AddScriptStage(name, script string) *PipelineBuilder {
	return b.AddStage(Stage{
		Name: name,
		Type: models.TaskTypeScript,
		Config: models.TaskConfig{
			Script: script,
		},
		Timeout:  time.Duration(b.pipeline.Config.TimeoutSeconds) * time.Second,
		MaxRetry: b.pipeline.Config.MaxRetries,
	})
}

func (b *PipelineBuilder) AddNotificationStage(name, channel string) *PipelineBuilder {
	return b.AddStage(Stage{
		Name: name,
		Type: models.TaskTypeNotification,
		Config: models.TaskConfig{
			NotifyChannel:   channel,
			NotifyOnFailure: true,
		},
		Timeout:  30 * time.Second,
		MaxRetry: 3,
	})
}

func (b *PipelineBuilder) SetStages(stages []Stage) *PipelineBuilder {
	b.stages = make([]Stage, len(stages))
	for i, s := range stages {
		s.Sequence = i
		b.stages[i] = s
	}
	return b
}

func (b *PipelineBuilder) WithStageDependency(stageName string, dependsOn ...string) *PipelineBuilder {
	for i, stage := range b.stages {
		if stage.Name == stageName {
			b.stages[i].DependsOn = append(b.stages[i].DependsOn, dependsOn...)
			break
		}
	}
	return b
}

func (b *PipelineBuilder) Build() (*models.Pipeline, error) {
	if len(b.errors) > 0 {
		return nil, fmt.Errorf("pipeline build failed: %v", b.errors)
	}
	if b.pipeline.Name == "" {
		return nil, fmt.Errorf("pipeline name is required")
	}
	if len(b.stages) == 0 {
		return nil, fmt.Errorf("pipeline must have at least one stage")
	}

	pipeline := b.pipeline
	pipeline.UpdatedAt = pipeline.UpdatedAt

	return pipeline, nil
}

func (b *PipelineBuilder) BuildWithStages() (*models.Pipeline, []Stage, error) {
	p, err := b.Build()
	if err != nil {
		return nil, nil, err
	}
	return p, b.stages, nil
}

func (b *PipelineBuilder) Errors() []error {
	return b.errors
}

func (b *PipelineBuilder) Pipeline() *models.Pipeline {
	return b.pipeline
}

func (b *PipelineBuilder) Stages() []Stage {
	return b.stages
}

func (b *PipelineBuilder) Reset() *PipelineBuilder {
	b.pipeline = models.NewPipeline("", uuid.Nil)
	b.stages = make([]Stage, 0)
	b.errors = nil
	return b
}

func (b *PipelineBuilder) Clone() *PipelineBuilder {
	stages := make([]Stage, len(b.stages))
	copy(stages, b.stages)

	errs := make([]error, len(b.errors))
	copy(errs, b.errors)

	clone := *b.pipeline
	return &PipelineBuilder{
		pipeline: &clone,
		stages:   stages,
		errors:   errs,
	}
}
