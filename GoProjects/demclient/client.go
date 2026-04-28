package demclient

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Config holds the DEM API connection parameters.
type Config struct {
	Server       string // e.g. "http://localhost:8080"
	SymKeyID     string // API key ID
	SymSecretKey string // Base64-encoded HMAC secret key
	SymVersion   string // API version (default: "5.2")
	ClientID     string // Client identifier for logging (default: "demclient")
}

// Client is a SymetryML DEM REST API client.
type Client struct {
	cfg        Config
	httpClient *http.Client
}

// NewClient creates a new DEM client.
func NewClient(cfg Config) *Client {
	if cfg.SymVersion == "" {
		cfg.SymVersion = "5.2"
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "demclient"
	}
	return &Client{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// SetHTTPClient replaces the default HTTP client (useful for testing).
func (c *Client) SetHTTPClient(hc *http.Client) {
	c.httpClient = hc
}

// BaseURL returns the API base URL including the key ID path segment.
func (c *Client) BaseURL() string {
	return strings.TrimRight(c.cfg.Server, "/") + "/symetry/rest/" + c.cfg.SymKeyID
}

// DoRequest executes an authenticated request against the DEM API.
func (c *Client) DoRequest(method, reqURL, body string) (*Response, error) {
	symDate := time.Now().UTC().Format("2006-01-02 15:04:05;000000")
	stringToSign := c.buildStringToSign(method, reqURL, body, symDate)
	signature := c.computeSignature(stringToSign)

	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}

	req, err := http.NewRequest(method, reqURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("sym-date", symDate)
	req.Header.Set("Authorization", signature)
	req.Header.Set("sym-version", c.cfg.SymVersion)
	req.Header.Set("sym-client", c.cfg.ClientID)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	result := &Response{
		StatusCode: resp.StatusCode,
		Raw:        rawBody,
		Headers:    make(map[string]string),
	}
	for k := range resp.Header {
		result.Headers[strings.ToLower(k)] = resp.Header.Get(k)
	}

	// Parse SymetryML response envelope
	var envelope struct {
		StatusCode   any             `json:"statusCode"`
		StatusString string          `json:"statusString"`
		Values       json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal(rawBody, &envelope); err == nil {
		switch v := envelope.StatusCode.(type) {
		case float64:
			result.StatusCode = int(v)
		case string:
			if v == "OK" {
				result.StatusCode = 200
			}
		}
		result.StatusString = envelope.StatusString
		result.Values = envelope.Values
	}

	return result, nil
}

// buildStringToSign constructs the canonical string for HMAC signing.
// Matches pymetry's Header.make_string_to_sign exactly.
func (c *Client) buildStringToSign(method, reqURL, body, symDate string) string {
	var sb strings.Builder

	sb.WriteString(method + "\n")

	if body != "" {
		h := md5.Sum([]byte(body))
		sb.WriteString(base64.StdEncoding.EncodeToString(h[:]) + "\n")
	} else {
		sb.WriteString("\n")
	}

	sb.WriteString(c.cfg.SymSecretKey + "\n")
	sb.WriteString(symDate + "\n")
	sb.WriteString(c.cfg.SymKeyID + "\n")

	if body != "" {
		sb.WriteString(body + "\n")
	}

	parsed, _ := url.Parse(reqURL)
	sb.WriteString(parsed.Path + "\n")

	if parsed.RawQuery != "" {
		sb.WriteString(parsed.RawQuery + "\n")
	}

	return sb.String()
}

func (c *Client) computeSignature(stringToSign string) string {
	secretKeyBytes, err := base64.StdEncoding.DecodeString(c.cfg.SymSecretKey)
	if err != nil {
		secretKeyBytes = []byte(c.cfg.SymSecretKey)
	}

	mac := hmac.New(sha256.New, secretKeyBytes)
	mac.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
