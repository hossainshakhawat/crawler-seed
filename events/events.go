package events

import "time"

// DiscoveredURL carries a URL to be fetched.
type DiscoveredURL struct {
	URL        string    `json:"url"`
	Depth      int       `json:"depth"`
	SourceURL  string    `json:"source_url"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}
