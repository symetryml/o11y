// Command otelsml runs the SymetryML OpenTelemetry Collector.
//
// This is a custom OTel Collector built with the standard OTLP receiver,
// the SymetryML denormalize processor, and the SymetryML exporter.
// It is designed to be chained after a stock OTel Collector.
package main

import (
	"fmt"
	"log"
	"os"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	// Standard OTel components
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"

	// SymetryML components
	"github.com/symetryml/otelsml/exporter/smlexporter"
	"github.com/symetryml/otelsml/processor/smlprocessor"
)

func main() {
	info := component.BuildInfo{
		Command:     "otelsml",
		Description: "SymetryML OpenTelemetry Collector",
		Version:     "0.1.0",
	}

	factories, err := components()
	if err != nil {
		log.Fatalf("failed to build components: %v", err)
	}

	settings := otelcol.CollectorSettings{
		BuildInfo: info,
		Factories: func() (otelcol.Factories, error) { return factories, nil },
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{
					fileprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
	}

	cmd := otelcol.NewCommand(settings)
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func components() (otelcol.Factories, error) {
	factories := otelcol.Factories{}

	// Receivers
	receivers, err := otelcol.MakeFactoryMap(
		otlpreceiver.NewFactory(),
	)
	if err != nil {
		return factories, err
	}
	factories.Receivers = receivers

	// Processors
	processors, err := otelcol.MakeFactoryMap(
		batchprocessor.NewFactory(),
		smlprocessor.NewFactory(),
	)
	if err != nil {
		return factories, err
	}
	factories.Processors = processors

	// Exporters
	exporters, err := otelcol.MakeFactoryMap(
		debugexporter.NewFactory(),
		otlpexporter.NewFactory(),
		otlphttpexporter.NewFactory(),
		smlexporter.NewFactory(),
	)
	if err != nil {
		return factories, err
	}
	factories.Exporters = exporters

	// Telemetry
	factories.Telemetry = otelconftelemetry.NewFactory()

	return factories, nil
}
