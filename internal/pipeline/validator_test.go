package pipeline

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"nexuspipe/internal/models"
)

func TestValidateValidPipeline(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("my-pipeline", uuid.New())
	p.Config = models.PipelineConfig{
		MaxRetries:        3,
		TimeoutSeconds:    300,
		Concurrency:       1,
		Priority:          5,
		QueueName:         "test.queue",
		RetryBackoffMS:    1000,
		BackoffMultiplier: 2.0,
		Environment:       "production",
	}

	b := NewPipelineBuilderFromExisting(p)
	b.AddStage(Stage{
		Name:     "fetch-data",
		Type:     models.TaskTypeHTTP,
		Timeout:  30 * time.Second,
		MaxRetry: 2,
	})
	b.AddStage(Stage{
		Name:      "transform",
		Type:      models.TaskTypeTransform,
		Timeout:   10 * time.Second,
		MaxRetry:  0,
		DependsOn: []string{"fetch-data"},
	})
	b.AddStage(Stage{
		Name:      "notify",
		Type:      models.TaskTypeNotification,
		Timeout:   5 * time.Second,
		MaxRetry:  1,
		DependsOn: []string{"transform"},
	})

	built, stages, err := b.BuildWithStages()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	built.Config = b.pipeline.Config

	result := v.ValidatePipeline(built)
	if !result.Valid {
		t.Fatalf("expected valid pipeline, got errors: %v", result.Error())
	}

	_ = stages
}

func TestValidateNilPipeline(t *testing.T) {
	v := NewPipelineValidator()
	result := v.ValidatePipeline(nil)

	if result.Valid {
		t.Fatal("expected invalid pipeline for nil input")
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected at least one error for nil pipeline")
	}
	foundNil := false
	for _, e := range result.Errors {
		if e.Field == "pipeline" {
			foundNil = true
			break
		}
	}
	if !foundNil {
		t.Error("expected error about nil pipeline")
	}
}

func TestValidateEmptyName(t *testing.T) {
	v := NewPipelineValidator()
	p := &models.Pipeline{
		ID:     uuid.New(),
		Name:   "",
		Status: models.PipelineStatusDraft,
		Config: models.PipelineConfig{
			TimeoutSeconds:    30,
			Concurrency:       1,
			Priority:          0,
			QueueName:         "q",
			RetryBackoffMS:    100,
			BackoffMultiplier: 1.0,
		},
	}

	result := v.ValidatePipeline(p)
	if result.Valid {
		t.Fatal("expected invalid pipeline for empty name")
	}
}

func TestValidateNameTooLong(t *testing.T) {
	v := NewPipelineValidator()
	long := make([]byte, 100)
	for i := range long {
		long[i] = 'a'
	}
	p := models.NewPipeline(string(long), uuid.New())

	result := v.ValidatePipeline(p)
	if result.Valid {
		t.Fatal("expected invalid pipeline for name too long")
	}
}

func TestValidateInvalidNamePattern(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("123-invalid", uuid.New())

	result := v.ValidatePipeline(p)
	if result.Valid {
		t.Fatal("expected invalid pipeline for name starting with digit")
	}
}

func TestValidateCycleDetection(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("cycle-pipeline", uuid.New())

	b := NewPipelineBuilderFromExisting(p)
	b.AddStage(Stage{Name: "stage-a", Type: models.TaskTypeHTTP, Timeout: 10 * time.Second, MaxRetry: 0})
	b.AddStage(Stage{Name: "stage-b", Type: models.TaskTypeHTTP, Timeout: 10 * time.Second, MaxRetry: 0})
	b.AddStage(Stage{Name: "stage-c", Type: models.TaskTypeHTTP, Timeout: 10 * time.Second, MaxRetry: 0})
	b.WithStageDependency("stage-a", "stage-c")
	b.WithStageDependency("stage-b", "stage-a")
	b.WithStageDependency("stage-c", "stage-b")

	built, _, err := b.BuildWithStages()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	result := v.ValidatePipeline(built)
	if result.Valid {
		t.Fatal("expected cycle detection error")
	}
	foundCycle := false
	for _, e := range result.Errors {
		if e.Message == "circular dependency detected in stage graph" || e.Field == "stages" {
			foundCycle = true
			break
		}
	}
	if !foundCycle {
		t.Errorf("expected circular dependency error, got: %v", result.Errors)
	}
}

func TestValidateCycleSelfReference(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("self-cycle", uuid.New())

	b := NewPipelineBuilderFromExisting(p)
	b.AddStage(Stage{Name: "self", Type: models.TaskTypeHTTP, Timeout: 10 * time.Second, MaxRetry: 0})
	b.WithStageDependency("self", "self")

	built, _, err := b.BuildWithStages()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	result := v.ValidatePipeline(built)
	if result.Valid {
		t.Fatal("expected cycle detection error for self-reference")
	}
}

func TestValidateNameUniqueness(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("dup-test", uuid.New())

	b := NewPipelineBuilderFromExisting(p)
	b.AddStage(Stage{Name: "stage", Type: models.TaskTypeHTTP, Timeout: 10 * time.Second, MaxRetry: 0})
	b.AddStage(Stage{Name: "stage", Type: models.TaskTypeSQL, Timeout: 10 * time.Second, MaxRetry: 0})

	built, _, err := b.BuildWithStages()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	result := v.ValidatePipeline(built)
	if result.Valid {
		t.Fatal("expected validation error for duplicate stage names")
	}
}

func TestValidateStageDependencies(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("dep-test", uuid.New())

	b := NewPipelineBuilderFromExisting(p)
	b.AddStage(Stage{Name: "stage-a", Type: models.TaskTypeHTTP, Timeout: 10 * time.Second, MaxRetry: 0})
	b.AddStage(Stage{Name: "stage-b", Type: models.TaskTypeSQL, Timeout: 10 * time.Second, MaxRetry: 0})

	b.WithStageDependency("stage-b", "non-existent-stage")

	built, _, err := b.BuildWithStages()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	result := v.ValidatePipeline(built)
	if result.Valid {
		t.Fatal("expected validation error for missing dependency")
	}
	foundDep := false
	for _, e := range result.Errors {
		if e.Field == "stages[1].depends_on" {
			foundDep = true
			break
		}
	}
	if !foundDep {
		t.Errorf("expected dependency error on stages[1].depends_on, got: %v", result.Errors)
	}
}

func TestValidateConfigInvalidTimeout(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("timeout-test", uuid.New())
	p.Config.TimeoutSeconds = 0
	p.Config.Concurrency = 1
	p.Config.Priority = 0
	p.Config.QueueName = "q"
	p.Config.RetryBackoffMS = 100
	p.Config.BackoffMultiplier = 1.0

	result := v.ValidatePipeline(p)
	if result.Valid {
		t.Fatal("expected validation error for zero timeout")
	}
}

func TestValidateConfigNegativeRetries(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("neg-retries", uuid.New())
	p.Config.MaxRetries = -1
	p.Config.TimeoutSeconds = 30
	p.Config.Concurrency = 1
	p.Config.Priority = 0
	p.Config.QueueName = "q"
	p.Config.RetryBackoffMS = 100
	p.Config.BackoffMultiplier = 1.0

	result := v.ValidatePipeline(p)
	if result.Valid {
		t.Fatal("expected validation error for negative max retries")
	}
}

func TestValidateConfigExceedsMaxRetries(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("max-retries", uuid.New())
	p.Config.MaxRetries = 30
	p.Config.TimeoutSeconds = 30
	p.Config.Concurrency = 1
	p.Config.Priority = 0
	p.Config.QueueName = "q"
	p.Config.RetryBackoffMS = 100
	p.Config.BackoffMultiplier = 1.0

	result := v.ValidatePipeline(p)
	if result.Valid {
		t.Fatal("expected validation error for max retries > 25")
	}
}

func TestValidateEmptyQueueName(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("no-queue", uuid.New())
	p.Config.TimeoutSeconds = 30
	p.Config.Concurrency = 1
	p.Config.Priority = 0
	p.Config.QueueName = ""
	p.Config.RetryBackoffMS = 100
	p.Config.BackoffMultiplier = 1.0

	result := v.ValidatePipeline(p)
	if result.Valid {
		t.Fatal("expected validation error for empty queue name")
	}
}

func TestValidateForbiddenType(t *testing.T) {
	v := NewPipelineValidator()
	v.ForbidType(models.TaskTypeShell)

	p := models.NewPipeline("forbidden", uuid.New())
	b := NewPipelineBuilderFromExisting(p)
	b.AddStage(Stage{Name: "danger", Type: models.TaskTypeShell, Timeout: 10 * time.Second, MaxRetry: 0})

	built, _, err := b.BuildWithStages()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	result := v.ValidatePipeline(built)
	if result.Valid {
		t.Fatal("expected validation error for forbidden stage type")
	}
}

func TestValidateNoStages(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("empty", uuid.New())
	p.Config.TimeoutSeconds = 30
	p.Config.Concurrency = 1
	p.Config.Priority = 0
	p.Config.QueueName = "q"
	p.Config.RetryBackoffMS = 100
	p.Config.BackoffMultiplier = 1.0

	result := v.ValidatePipeline(p)
	if result.Valid {
		t.Fatal("expected validation error for pipeline with no stages")
	}
}

func TestValidateStageBadType(t *testing.T) {
	v := NewPipelineValidator()
	p := models.NewPipeline("bad-type", uuid.New())
	b := NewPipelineBuilderFromExisting(p)
	b.AddStage(Stage{Name: "weird", Type: "nonexistent", Timeout: 10 * time.Second, MaxRetry: 0})

	built, _, err := b.BuildWithStages()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	result := v.ValidatePipeline(built)
	if result.Valid {
		t.Fatal("expected validation error for invalid stage type")
	}
}
