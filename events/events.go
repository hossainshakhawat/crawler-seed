// Package events defines the Kafka message schemas shared across all crawler
// microservices.  Keep this file identical in every project.
package events

import "time"

// TopicDiscovered is the Kafka topic that carries URLs waiting to be fetched.
const TopicDiscovered = "discovered-urls"

// TopicCrawled is the Kafka topic that carries fetched page bodies.
const TopicCrawled = "crawled-urls"

// DiscoveredURL is published to TopicDiscovered whenever a new URL is found.
type DiscoveredURL struct {
	URL        string    `json:"url"`
	Depth      int       `json:"depth"`
	SourceURL  string    `json:"source_url"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// CrawledPage is published to TopicCrawled after a page has been fetched.
// Body is gzip-compressed to keep Kafka message sizes small.
type CrawledPage struct {
	URL        string    `json:"url"`
	FinalURL   string    `json:"final_url"`
	Depth      int       `json:"depth"`
	HTTPStatus int       `json:"http_status"`
	Body       []byte    `json:"body"` // gzip-compressed HTML
	CrawledAt  time.Time `json:"crawled_at"`
}
