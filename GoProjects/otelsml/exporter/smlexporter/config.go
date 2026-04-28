// Package smlexporter implements a SymetryML exporter that outputs
// denormalized metrics as JSON lines (for debugging/testing) or forwards
// them to the SymetryML DEM API.
package smlexporter

import (
	"go.opentelemetry.io/collector/component"
)

// Config holds the exporter configuration.
type Config struct {
	// Format is the output format: "json" (one line per metric) or "csv" (wide-format).
	Format string `mapstructure:"format"`
	// OutputPath is the file path for output ("stdout" for console).
	OutputPath string `mapstructure:"output_path"`
	// DEMEndpoint is the SymetryML DEM API endpoint.
	// When set, metrics are streamed to the DEM server via the demclient.
	DEMEndpoint string `mapstructure:"dem_endpoint"`
	// ProjectName is the DEM project to stream data to.
	ProjectName string `mapstructure:"project_name"`
}

func createDefaultConfig() component.Config {
	return &Config{
		Format:     "json",
		OutputPath: "stdout",
	}
}
