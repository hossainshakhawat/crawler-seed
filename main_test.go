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
	defer viper.Reset()

	cfg := loadConfig()

	if cfg.kafka != "test-kafka:9092" {
		t.Errorf("kafka: got %q, want %q", cfg.kafka, "test-kafka:9092")
	}
	if cfg.depth != 3 {
		t.Errorf("depth: got %d, want 3", cfg.depth)
	}
}

func TestLoadConfig_ConfigFile(t *testing.T) {
	viper.Reset()
	defer viper.Reset()
	for _, key := range []string{"SEED_KAFKA_BROKER", "SEED_DEPTH"} {
		os.Unsetenv(key)
	}

	cfg := loadConfig()

	if cfg.kafka == "" {
		t.Error("kafka should not be empty when read from config.yml")
	}
	// Verify that depth can be read from config file
	if cfg.depth < 0 {
		t.Error("depth should be a valid value when read from config.yml")
	}
}

func TestLoadConfig_TopicDiscovered(t *testing.T) {
	viper.Reset()
	t.Setenv("SEED_TOPIC_DISCOVERED", "test-topic")
	defer viper.Reset()

	cfg := loadConfig()

	if cfg.topicDiscovered != "test-topic" {
		t.Errorf("topicDiscovered: got %q, want %q", cfg.topicDiscovered, "test-topic")
	}
}
