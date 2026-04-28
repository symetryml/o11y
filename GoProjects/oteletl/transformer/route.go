package transformer

import "regexp"

// RoutePattern pairs a regex with its replacement placeholder.
type RoutePattern struct {
	re          *regexp.Regexp
	replacement string
}

// DefaultRoutePatterns defines the standard parameterization patterns.
var DefaultRoutePatterns = []RoutePattern{
	{regexp.MustCompile(`[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}`), "{uuid}"},
	{regexp.MustCompile(`(?:^|/)([a-fA-F0-9]{16,})(?:/|$)`), "/{hex_id}/"},
	{regexp.MustCompile(`(?:^|/)([a-fA-F0-9]{24})(?:/|$)`), "/{object_id}/"},
	{regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`), "{email}"},
	{regexp.MustCompile(`\d{4}-\d{2}-\d{2}`), "{date}"},
	{regexp.MustCompile(`(?:^|/)(\d{10,13})(?:/|$)`), "/{timestamp}/"},
	{regexp.MustCompile(`(?:^|/)(\d+)(?:/|$)`), "/{id}/"},
	{regexp.MustCompile(`(?:^|/)([a-zA-Z0-9_\-]{20,})(?:/|$)`), "/{slug}/"},
}

var reDoubleSlash = regexp.MustCompile(`//+`)
var reTrailingSlash = regexp.MustCompile(`/+$`)

// ParameterizeRoute replaces dynamic route segments with placeholders.
func ParameterizeRoute(route string) string {
	if route == "" {
		return route
	}

	result := route
	for _, p := range DefaultRoutePatterns {
		result = p.re.ReplaceAllString(result, p.replacement)
	}

	// Clean up double slashes introduced by replacements
	result = reDoubleSlash.ReplaceAllString(result, "/")
	// Restore original trailing slash behavior
	if len(route) > 0 && route[len(route)-1] != '/' && len(result) > 1 {
		result = reTrailingSlash.ReplaceAllString(result, "")
	}

	return result
}
