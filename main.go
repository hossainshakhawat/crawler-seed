// Command crawler-seed publishes seed URLs to the "discovered-urls" Kafka topic,
// kicking off the distributed crawler pipeline.
//
// Configuration is loaded in this priority order (highest wins):
//
//  1. Environment variables  (prefix SEED_, e.g. SEED_SEEDS, SEED_KAFKA_BROKER)
//  2. config.yml             (must be in the working directory)
//
// Seeds are read from the "seeds" key in config.yml (a YAML list) or from the
// SEED_SEEDS environment variable (comma-separated URLs).
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/hossainshakhawat/crawler-seed/events"
	"github.com/hossainshakhawat/crawler-seed/internal/kafkaconn"
	"github.com/spf13/viper"
)

type config struct {
	seeds           []string
	kafka           string
	depth           int
	topicDiscovered string
}

func loadConfig() config {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	viper.SetEnvPrefix("SEED")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("config: %v", err)
		}
	}

	seeds := viper.GetStringSlice("seeds")
	// Viper doesn't split comma-separated env vars for slice keys; do it manually.
	if raw := os.Getenv("SEED_SEEDS"); raw != "" && len(seeds) == 1 && strings.Contains(seeds[0], ",") {
		seeds = strings.Split(seeds[0], ",")
	}
	return config{
		seeds:           seeds,
		kafka:           viper.GetString("kafka_broker"),
		depth:           viper.GetInt("depth"),
		topicDiscovered: viper.GetString("topic_discovered"),
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := loadConfig()

	if len(cfg.seeds) == 0 {
		log.Fatal("no seed URLs configured: set 'seeds' in config.yml or SEED_SEEDS env var")
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
		if err := kafkaconn.Publish(ctx, kafkaClient, cfg.topicDiscovered, []byte(seedURL), payload); err != nil {
			log.Fatalf("publish %s: %v", seedURL, err)
		}
		log.Printf("published → %s (depth %d)", seedURL, cfg.depth)
	}

	log.Printf("done: %d seed(s) published to %s", len(cfg.seeds), cfg.topicDiscovered)
}
