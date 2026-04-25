package main

import (
	"os"
	"testing"

	"github.com/spf13/viper"
)

func TestLoadConfig_EnvOverride(t *testing.T) {
	viper.Reset()
	t.Setenv("SEED_KAFKA_BROKER", "test-kafka:9092")
	t.Setenv("SEED_DEPTH", "3")
	t.Setenv("SEED_SEEDS", "https://a.com,https://b.com")
	defer viper.Reset()

	cfg := loadConfig()

	if cfg.kafka != "test-kafka:9092" {
		t.Errorf("kafka: got %q, want %q", cfg.kafka, "test-kafka:9092")
	}
	if cfg.depth != 3 {
		t.Errorf("depth: got %d, want 3", cfg.depth)
	}
	if len(cfg.seeds) != 2 {
		t.Errorf("seeds: got %d, want 2", len(cfg.seeds))
	}
}

func TestLoadConfig_ConfigFile(t *testing.T) {
	viper.Reset()
	defer viper.Reset()
	for _, key := range []string{"SEED_KAFKA_BROKER", "SEED_DEPTH", "SEED_SEEDS"} {
		os.Unsetenv(key)
	}

	cfg := loadConfig()

	if cfg.kafka == "" {
		t.Error("kafka should not be empty when read from config.yml")
	}
	// config.yml has seeds list with at least one entry
	if len(cfg.seeds) == 0 {
		t.Error("seeds should not be empty when read from config.yml")
	}
}

func TestLoadConfig_SeedsFromEnv(t *testing.T) {
	viper.Reset()
	t.Setenv("SEED_SEEDS", "https://example.com,https://other.org,https://third.net")
	defer viper.Reset()

	cfg := loadConfig()

	if len(cfg.seeds) != 3 {
		t.Errorf("seeds: got %d, want 3; seeds=%v", len(cfg.seeds), cfg.seeds)
	}
}
