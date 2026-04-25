// Package events defines the Kafka message schemas shared across all crawler
// microservices.  Keep this file identical in every project.
package events

import "time"

// DiscoveredURL carries a URL to be fetched.
type DiscoveredURL struct {
	URL        string    `json:"url"`
	Depth      int       `json:"depth"`
	SourceURL  string    `json:"source_url"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// CrawledPage carries a fetched page body. Body is gzip-compressed.
type CrawledPage struct {
	URL        string    `json:"url"`
	FinalURL   string    `json:"final_url"`
	Depth      int       `json:"depth"`
	HTTPStatus int       `json:"http_status"`
	Body       []byte    `json:"body"` // gzip-compressed HTML
	CrawledAt  time.Time `json:"crawled_at"`
}
