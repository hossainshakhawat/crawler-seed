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
	"errors"
	"log"
	"net/http"

	"github.com/hossainshakhawat/crawler-seed/internal/handler"
	"github.com/hossainshakhawat/crawler-seed/internal/kafkaconn"
	"github.com/spf13/viper"
)

type config struct {
	kafka           string
	depth           int
	topicDiscovered string
	httpAddr        string
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

	return config{
		kafka:           viper.GetString("kafka_broker"),
		depth:           viper.GetInt("depth"),
		topicDiscovered: viper.GetString("topic_discovered"),
		httpAddr:        viper.GetString("http_addr"),
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := loadConfig()

	ctx := context.Background()

	kafkaClient, err := kafkaconn.New(ctx, cfg.kafka)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer kafkaClient.Close()

	hCfg := handler.Config{DefaultDepth: cfg.depth, Topic: cfg.topicDiscovered}

	addr := cfg.httpAddr
	if addr == "" {
		addr = ":8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /seed", handler.SeedHandler(ctx, kafkaClient, hCfg))

	log.Printf("HTTP server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http: %v", err)
	}
}
