package metrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type TraceProvider struct {
	provider    *sdktrace.TracerProvider
	tracer      trace.Tracer
	exporter    sdktrace.SpanExporter
	propagator  propagation.TextMapPropagator
	serviceName string
	environment string
}

type TraceConfig struct {
	ServiceName        string
	Environment        string
	Endpoint           string
	Insecure           bool
	SampleRate         float64
	MaxExportBatchSize int
}

func DefaultTraceConfig() TraceConfig {
	return TraceConfig{
		ServiceName:        "nexuspipe",
		Environment:        "production",
		Endpoint:           "localhost:4317",
		Insecure:           true,
		SampleRate:         0.1,
		MaxExportBatchSize: 512,
	}
}

func NewTraceProvider(ctx context.Context, cfg TraceConfig) (*TraceProvider, error) {
	var opts []otlptracegrpc.Option
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Endpoint))

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			attribute.String("deployment.environment", cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	sampler := sdktrace.ParentBased(
		sdktrace.TraceIDRatioBased(cfg.SampleRate),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxExportBatchSize(cfg.MaxExportBatchSize),
		),
		sdktrace.WithResource(res),
	)

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagator)

	tracer := tp.Tracer(cfg.ServiceName)

	return &TraceProvider{
		provider:    tp,
		tracer:      tracer,
		exporter:    exporter,
		propagator:  propagator,
		serviceName: cfg.ServiceName,
		environment: cfg.Environment,
	}, nil
}

func (tp *TraceProvider) Tracer() trace.Tracer {
	return tp.tracer
}

func (tp *TraceProvider) Shutdown(ctx context.Context) error {
	if err := tp.provider.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown tracer provider: %w", err)
	}
	return nil
}

func (tp *TraceProvider) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tp.tracer.Start(ctx, name, opts...)
}

func (tp *TraceProvider) InjectContext(ctx context.Context, carrier propagation.TextMapCarrier) {
	tp.propagator.Inject(ctx, carrier)
}

func (tp *TraceProvider) ExtractContext(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return tp.propagator.Extract(ctx, carrier)
}

func SpanIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return ""
	}
	return span.SpanContext().SpanID().String()
}

func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return ""
	}
	return span.SpanContext().TraceID().String()
}

func AddSpanEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

func SetSpanError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() && err != nil {
		span.RecordError(err)
		span.SetAttributes(attribute.String("error.message", err.Error()))
	}
}

func SetSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(attrs...)
	}
}
