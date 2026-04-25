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

	"github.com/shakhawathossain/crawler-seed/events"
	"github.com/shakhawathossain/crawler-seed/internal/kafkaconn"
	"github.com/twmb/franz-go/pkg/kgo"
)

type seedList []string

func (s *seedList) String() string     { return strings.Join(*s, ", ") }
func (s *seedList) Set(v string) error { *s = append(*s, v); return nil }

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	var seeds seedList
	flag.Var(&seeds, "seed", "Seed URL (may be repeated)")
	kafka := flag.String("kafka", "localhost:9092", "Kafka broker address")
	depth := flag.Int("depth", 0, "Starting crawl depth for seed URLs")
	flag.Parse()

	seeds = append(seeds, flag.Args()...)
	if len(seeds) == 0 {
		fmt.Fprintln(os.Stderr, "error: at least one -seed URL is required")
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	cl, err := kafkaconn.New(ctx, *kafka)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer cl.Close()

	for _, u := range seeds {
		ev := events.DiscoveredURL{
			URL:        u,
			Depth:      *depth,
			SourceURL:  "",
			EnqueuedAt: time.Now().UTC(),
		}
		val, err := json.Marshal(ev)
		if err != nil {
			log.Printf("marshal %s: %v", u, err)
			continue
		}
		rec := &kgo.Record{
			Topic: events.TopicDiscovered,
			Key:   []byte(u),
			Value: val,
		}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			log.Fatalf("publish %s: %v", u, err)
		}
		log.Printf("published → %s (depth %d)", u, *depth)
	}

	log.Printf("done: %d seed(s) published to %s", len(seeds), events.TopicDiscovered)
}
