// Command crawler-seed publishes seed URLs to the "discovered-urls" Kafka topic,
// kicking off the distributed crawler pipeline.
//
// Configuration is loaded in this priority order (highest wins):
//
//  1. CLI flags
//  2. Environment variables  (prefix SEED_, e.g. SEED_KAFKA_BROKER)
//  3. config.yml             (must be in the working directory)
//  4. Built-in defaults
//
// Note: seed URLs must always be provided via -seed flags or positional arguments.
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
	"github.com/spf13/viper"
)

type seedList []string

func (s *seedList) String() string     { return strings.Join(*s, ", ") }
func (s *seedList) Set(v string) error { *s = append(*s, v); return nil }

type config struct {
	seeds seedList
	kafka string
	depth int
}

func loadConfig() config {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	viper.SetDefault("kafka_broker", "localhost:9092")
	viper.SetDefault("depth", 0)

	viper.SetEnvPrefix("SEED")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("config: %v", err)
		}
	}

	// Flags override env vars and config.yml; defaults come from Viper
	// so env vars and config.yml flow through when flags are not set.
	var seeds seedList
	kafka := flag.String("kafka", viper.GetString("kafka_broker"), "Kafka broker address")
	depth := flag.Int("depth", viper.GetInt("depth"), "Starting crawl depth for seed URLs")
	flag.Var(&seeds, "seed", "Seed URL (may be repeated)")
	flag.Parse()
	seeds = append(seeds, flag.Args()...)

	return config{
		seeds: seeds,
		kafka: *kafka,
		depth: *depth,
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := loadConfig()

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
