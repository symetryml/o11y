package demclient

import (
	"encoding/json"
	"fmt"
	"net/url"
)

// GetUnivariateStats returns univariate statistics for a variable.
func (c *Client) GetUnivariateStats(pid, variable string) (map[string]any, error) {
	ctx := ExploreContext{
		Values: []MLContext{{
			Targets:             []string{},
			InputAttributeNames: []string{variable},
		}},
	}
	return c.exploreFirst(pid, "uni", &ctx)
}

// GetBivariateStats returns bivariate statistics for two variables.
func (c *Client) GetBivariateStats(pid, var1, var2 string) (map[string]any, error) {
	ctx := ExploreContext{
		Values: []MLContext{{
			Targets:             []string{},
			InputAttributeNames: []string{var1, var2},
		}},
	}
	return c.exploreFirst(pid, "bi", &ctx)
}

// GetExploration runs a batch exploration query.
func (c *Client) GetExploration(pid string, ctx *ExploreContext, metricType string) (*Response, error) {
	body, err := json.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("marshal explore context: %w", err)
	}

	reqURL := c.BaseURL() + "/projects/" + url.PathEscape(pid) +
		"/explore?metric=" + url.QueryEscape(metricType)

	return c.DoRequest("POST", reqURL, string(body))
}

// GetHistogram returns density estimation for a variable.
func (c *Client) GetHistogram(pid, attribute string, numBins int) (*Response, error) {
	if numBins <= 0 {
		numBins = 10
	}

	ctx := ExploreContext{
		Values: []MLContext{{
			Targets:             []string{},
			InputAttributeNames: []string{attribute},
			ExtraParameters: map[string]string{
				"hist_use_random": "true",
				"hist_bins":       fmt.Sprintf("%d", numBins),
			},
		}},
	}

	body, err := json.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("marshal histogram context: %w", err)
	}

	reqURL := c.BaseURL() + "/projects/" + url.PathEscape(pid) + "/densityEstimate"
	return c.DoRequest("POST", reqURL, string(body))
}

// exploreFirst runs an exploration query and extracts the first result from KSVDMap.
func (c *Client) exploreFirst(pid, metricType string, ctx *ExploreContext) (map[string]any, error) {
	resp, err := c.GetExploration(pid, ctx, metricType)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("exploration failed (%d): %s", resp.StatusCode, resp.StatusString)
	}

	var vals struct {
		KSVDMap struct {
			Values []map[string]any `json:"values"`
		} `json:"KSVDMap"`
	}
	if err := json.Unmarshal(resp.Values, &vals); err != nil {
		return nil, fmt.Errorf("parse exploration response: %w", err)
	}

	if len(vals.KSVDMap.Values) == 0 {
		return nil, fmt.Errorf("no exploration results returned")
	}

	return vals.KSVDMap.Values[0], nil
}
