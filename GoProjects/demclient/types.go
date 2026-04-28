// Package demclient provides a Go HTTP client for the SymetryML DEM REST API.
package demclient

import "encoding/json"

// SMLDataFrame is the SymetryML DataFrame JSON format used for data streaming.
type SMLDataFrame struct {
	AttributeNames []string `json:"attributeNames"`
	AttributeTypes []string `json:"attributeTypes"` // C=continuous, B=binary, S=string, T=categorical, X=ignore
	Data           [][]any  `json:"data"`
}

// Response wraps a DEM API response.
type Response struct {
	StatusCode   int             `json:"statusCode"`
	StatusString string          `json:"statusString"`
	Values       json.RawMessage `json:"values"`
	Raw          []byte          `json:"-"`
	Headers      map[string]string `json:"-"`
}

// ExploreContext is the request body for exploration endpoints.
type ExploreContext struct {
	Values []MLContext `json:"values"`
}

// MLContext specifies inputs and targets for exploration or model operations.
type MLContext struct {
	Targets             []string          `json:"targets"`
	InputAttributeNames []string          `json:"inputAttributeNames,omitempty"`
	InputAttributes     []string          `json:"inputAttributes,omitempty"`
	ExtraParameters     map[string]string `json:"extraParameters,omitempty"`
}
