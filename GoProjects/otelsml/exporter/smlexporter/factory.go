package smlexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

const (
	typeStr   = "smlexporter"
	stability = component.StabilityLevelAlpha
)

// NewFactory creates the exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, stability),
	)
}

func createMetricsExporter(
	_ context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	eCfg := cfg.(*Config)
	return newExporter(eCfg, set.Logger), nil
}
