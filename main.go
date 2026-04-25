// Command crawler-seed publishes seed URLs to the "discovered-urls" Kafka topic,
// kicking off the distributed crawler pipeline.
//
// Usage:
//
//	crawler-seed -seed https://example.com [-seed https://other.com] [flags]
//
// Flags:
//
//	-seed    seed URL (may be repeated)
//	-kafka   Kafka broker address (default localhost:9092)
//	-depth   starting crawl depth for seeds (default 0)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/hossainshakhawat/crawler-seed/events"
	"github.com/hossainshakhawat/crawler-seed/internal/kafkaconn"
)

type seedList []string

func (s *seedList) String() string     { return strings.Join(*s, ", ") }
func (s *seedList) Set(v string) error { *s = append(*s, v); return nil }

type config struct {
	seeds seedList
	kafka string
	depth int
}

func parseFlags() config {
	var cfg config
	flag.Var(&cfg.seeds, "seed", "Seed URL (may be repeated)")
	flag.StringVar(&cfg.kafka, "kafka", "localhost:9092", "Kafka broker address")
	flag.IntVar(&cfg.depth, "depth", 0, "Starting crawl depth for seed URLs")
	flag.Parse()
	cfg.seeds = append(cfg.seeds, flag.Args()...)
	return cfg
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := parseFlags()

	if len(cfg.seeds) == 0 {
		fmt.Fprintln(os.Stderr, "error: at least one -seed URL is required")
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	kafkaClient, err := kafkaconn.New(ctx, cfg.kafka)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer kafkaClient.Close()

	for _, seedURL := range cfg.seeds {
		event := events.DiscoveredURL{
			URL:        seedURL,
			Depth:      cfg.depth,
			SourceURL:  "",
			EnqueuedAt: time.Now().UTC(),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			log.Printf("marshal %s: %v", seedURL, err)
			continue
		}
		if err := kafkaconn.Publish(ctx, kafkaClient, events.TopicDiscovered, []byte(seedURL), payload); err != nil {
			log.Fatalf("publish %s: %v", seedURL, err)
		}
		log.Printf("published → %s (depth %d)", seedURL, cfg.depth)
	}

	log.Printf("done: %d seed(s) published to %s", len(cfg.seeds), events.TopicDiscovered)
}
