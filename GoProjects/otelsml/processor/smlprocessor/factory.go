package smlprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const (
	typeStr   = "smlprocessor"
	stability = component.StabilityLevelAlpha
)

// NewFactory creates the processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, stability),
	)
}

func createMetricsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	pCfg := cfg.(*Config)
	return newProcessor(pCfg, next, set.TelemetrySettings), nil
}
