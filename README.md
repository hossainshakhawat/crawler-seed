# crawler-seed

Bootstraps the distributed web crawler pipeline by publishing one or more seed URLs to the `discovered-urls` Kafka topic. Every other service in the pipeline is driven by the events this tool emits.

## Architecture overview

```
crawler-seed  ──►  [discovered-urls]  ──►  crawler-worker  ──►  [crawled-urls]  ──►  crawler-parser
                        ▲                                                                    │
                        └────────────────────────────────────────────────────────────────────┘
                                            (newly discovered links)
```

`crawler-seed` is the entry point. Run it once (or repeatedly) with a list of URLs to start or extend a crawl.

## Prerequisites

| Dependency | Minimum version | Notes |
|------------|-----------------|-------|
| Go         | 1.25            |       |
| Kafka      | 3.x             | Topic `discovered-urls` must exist |

## Configuration

Configuration is read from `config.yml` in the working directory. All keys can be overridden by environment variables with the `SEED_` prefix (e.g. `SEED_KAFKA_BROKER`).

| Key                | Default              | Env var                   | Description                                                             |
|--------------------|----------------------|---------------------------|-------------------------------------------------------------------------|
| `kafka_broker`     | `localhost:9092`     | `SEED_KAFKA_BROKER`       | Kafka broker address                                                    |
| `depth`            | `0`                  | `SEED_DEPTH`              | Starting crawl depth assigned to seed URLs                              |
| `topic_discovered` | `discovered-urls`    | `SEED_TOPIC_DISCOVERED`   | Kafka topic to publish seed URLs to                                     |
| `seeds`            | *(empty)*            | `SEED_SEEDS`              | Seed URLs — YAML list in config.yml, or comma-separated in the env var  |

## Running

```bash
# Build
go build -o crawler-seed ./...

# Run with defaults (reads config.yml — seeds list must be set there or via env)
./crawler-seed

# Override seeds and broker via environment variables
SEED_SEEDS="https://example.com,https://other.com" \
SEED_KAFKA_BROKER=broker:9092 \
./crawler-seed

# Use a custom topic and non-zero starting depth
SEED_TOPIC_DISCOVERED=my-urls \
SEED_DEPTH=1 \
SEED_SEEDS="https://example.com" \
./crawler-seed
```

## What it does

1. Reads seed URLs from `seeds` in `config.yml` (or `SEED_SEEDS` env var).
2. Connects to Kafka and pings the broker to verify connectivity.
3. For each seed URL, constructs a `DiscoveredURL` event:
   ```json
   {
     "url": "https://example.com",
     "depth": 0,
     "source_url": "",
     "enqueued_at": "2026-04-25T10:00:00Z"
   }
   ```
4. Synchronously produces each event to the `discovered-urls` topic (keyed by URL).
5. Exits after all seeds are published.

## Kafka topics

Topic names are configurable via `config.yml` or environment variables (see Configuration above).

| Config key         | Default           | Direction | Message type    |
|--------------------|-------------------|-----------|-----------------|
| `topic_discovered` | `discovered-urls` | Produce   | `DiscoveredURL` |
