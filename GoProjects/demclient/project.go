package demclient

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// ListProjects returns project names.
func (c *Client) ListProjects() ([]string, error) {
	reqURL := c.BaseURL() + "/projects/list"
	resp, err := c.DoRequest("GET", reqURL, "")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("list projects failed (%d): %s", resp.StatusCode, resp.StatusString)
	}

	var vals struct {
		StringList struct {
			Values []string `json:"values"`
		} `json:"stringList"`
	}
	if err := json.Unmarshal(resp.Values, &vals); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	names := make([]string, len(vals.StringList.Values))
	for i, v := range vals.StringList.Values {
		parts := strings.SplitN(v, ":", 2)
		names[i] = parts[0]
	}
	return names, nil
}

// CreateProject creates a new DEM project.
func (c *Client) CreateProject(pid, projectType string, persist bool) error {
	persistStr := "false"
	if persist {
		persistStr = "true"
	}
	reqURL := c.BaseURL() + "/projects?pid=" + url.QueryEscape(pid) +
		"&type=" + url.QueryEscape(projectType) +
		"&persist=" + persistStr

	resp, err := c.DoRequest("POST", reqURL, "{}")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return fmt.Errorf("create project failed (%d): %s", resp.StatusCode, resp.StatusString)
	}
	return nil
}

// EnsureProject creates a project if it doesn't exist.
func (c *Client) EnsureProject(pid, projectType string) error {
	projects, err := c.ListProjects()
	if err != nil {
		return c.CreateProject(pid, projectType, true)
	}
	for _, p := range projects {
		if p == pid {
			return nil
		}
	}
	return c.CreateProject(pid, projectType, true)
}

// GetProjectInfo returns project metadata.
func (c *Client) GetProjectInfo(pid string) (*Response, error) {
	reqURL := c.BaseURL() + "/projects/" + url.PathEscape(pid) + "/info"
	return c.DoRequest("GET", reqURL, "")
}

// DeleteProject deletes a project.
func (c *Client) DeleteProject(pid string) error {
	reqURL := c.BaseURL() + "/projects/" + url.PathEscape(pid)
	resp, err := c.DoRequest("DELETE", reqURL, "")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("delete project failed (%d): %s", resp.StatusCode, resp.StatusString)
	}
	return nil
}
