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

All options are passed as command-line flags.

| Flag      | Default           | Description                              |
|-----------|-------------------|------------------------------------------|
| `-seed`   | *(required)*      | Seed URL. May be repeated for multiple URLs. Also accepts bare positional arguments. |
| `-kafka`  | `localhost:9092`  | Kafka broker address                     |
| `-depth`  | `0`               | Starting crawl depth assigned to seeds   |

## Running

```bash
# Build
go build -o crawler-seed ./...

# Seed a single URL
./crawler-seed -seed https://example.com

# Seed multiple URLs
./crawler-seed \
  -seed https://example.com \
  -seed https://other.com

# Point at a remote Kafka broker and set a non-zero starting depth
./crawler-seed -kafka broker:9092 -depth 1 -seed https://example.com

# Positional arguments also work
./crawler-seed -kafka broker:9092 https://example.com https://other.com
```

## What it does

1. Parses flags and validates that at least one seed URL is provided.
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

| Topic              | Direction | Message type    |
|--------------------|-----------|-----------------|
| `discovered-urls`  | Produce   | `DiscoveredURL` |
