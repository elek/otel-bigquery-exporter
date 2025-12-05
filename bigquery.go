package bigquery

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

var (
	typeStr = component.MustNewType("bigquery")
)

type Config struct {
	Project  string `mapstructure:"project"`
	Dataset  string `mapstructure:"dataset"`
	Table    string `mapstructure:"table"`
}

func createDefaultConfig() component.Config {
	return &Config{}
}

// NewFactory creates a factory for tailtracer receiver.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typeStr,
		createDefaultConfig,
		exporter.WithTraces(createTraces, component.StabilityLevelAlpha))
}

func createTraces(ctx context.Context, settings exporter.Settings, config component.Config) (exporter.Traces, error) {
	return &TraceExporter{
		log: settings.Logger,
		cfg: config.(*Config),
	}, nil
}

type TraceExporter struct {
	log    *zap.Logger
	client *BigQueryClient
	cfg    *Config
}

func (t *TraceExporter) Start(ctx context.Context, host component.Host) (err error) {
	t.client, err = NewBigQueryClient(ctx, t.cfg.Project, t.cfg.Dataset)
	if err != nil {
		return err
	}
	return nil
}

func (t *TraceExporter) Shutdown(ctx context.Context) error {
	return t.client.close()
}

func (t *TraceExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (t *TraceExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	records := map[string][]*Record{}
	for _, span := range td.ResourceSpans().All() {
		for _, scopeSpans := range span.ScopeSpans().All() {
			for _, span := range scopeSpans.Spans().All() {
				r := Record{
					Tags:       map[string]interface{}{},
				}
				r.Tags["id"] = span.SpanID().String()
				r.Tags["parent"] = span.ParentSpanID().String()
				r.Tags["trace"] = span.TraceID().String()
				r.Tags["name"] = span.Name()
				r.Tags["scope"] = scopeSpans.Scope().Name()
				r.Tags["timestamp"] = span.StartTimestamp().AsTime()
				r.Tags["duration_ms"] = span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Milliseconds()
				records[t.cfg.Table] = append(records[t.cfg.Table], &r)
			}
		}

	}
	err := t.client.SaveRecord(records)
	if err != nil {
		t.log.Error("Failed to save records to BigQuery", zap.Error(err))
	}
	return nil
}

var _ exporter.Traces = (*TraceExporter)(nil)
