package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hossainshakhawat/crawler-seed/events"
	"github.com/hossainshakhawat/crawler-seed/internal/kafkaconn"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds the handler-specific configuration.
type Config struct {
	DefaultDepth int
	Topic        string
}

type seedRequest struct {
	URL   string `json:"url"`
	Depth *int   `json:"depth,omitempty"`
}

type seedResponse struct {
	Published bool   `json:"published"`
	URL       string `json:"url"`
	Depth     int    `json:"depth"`
}

// SeedHandler returns an http.HandlerFunc for POST /seed.
// It decodes a JSON body with "url" and optional "depth", then publishes to Kafka.
func SeedHandler(ctx context.Context, kafkaClient *kgo.Client, cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req seedRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		req.URL = strings.TrimSpace(req.URL)
		if req.URL == "" {
			http.Error(w, `"url" is required`, http.StatusBadRequest)
			return
		}
		if _, err := url.ParseRequestURI(req.URL); err != nil {
			http.Error(w, "invalid url", http.StatusBadRequest)
			return
		}

		depth := cfg.DefaultDepth
		if req.Depth != nil {
			depth = *req.Depth
		}

		if err := PublishSeed(ctx, kafkaClient, Config{DefaultDepth: depth, Topic: cfg.Topic}, req.URL); err != nil {
			log.Printf("publish error for %s: %v", req.URL, err)
			http.Error(w, "failed to publish seed", http.StatusInternalServerError)
			return
		}

		log.Printf("published via HTTP → %s (depth %d)", req.URL, depth)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(seedResponse{Published: true, URL: req.URL, Depth: depth})
	}
}

// PublishSeed sends a single seed URL to the Kafka topic.
func PublishSeed(ctx context.Context, kafkaClient *kgo.Client, cfg Config, seedURL string) error {
	event := events.DiscoveredURL{
		URL:        seedURL,
		Depth:      cfg.DefaultDepth,
		SourceURL:  "",
		EnqueuedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return kafkaconn.Publish(ctx, kafkaClient, cfg.Topic, []byte(seedURL), payload)
}
