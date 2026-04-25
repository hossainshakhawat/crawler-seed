# crawler-seed

Bootstraps the distributed web crawler pipeline by publishing seed URLs to the `discovered-urls` Kafka topic. Other services in the pipeline consume these events to start or extend crawls.

## Architecture overview

```
crawler-seed  ──►  [discovered-urls]  ──►  crawler-worker  ──►  [crawled-urls]  ──►  crawler-parser
                        ▲                                                                    │
                        └────────────────────────────────────────────────────────────────────┘
                                            (newly discovered links)
```

`crawler-seed` is the entry point. You can publish seeds either programmatically via HTTP or run the binary to start an HTTP server that accepts seed submissions.

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
| `seeds`            | *(removed)*          | N/A                       | Seed list removed from `config.yml`. Use the HTTP `POST /seed` endpoint to submit seeds.
| `http_addr`        | `:8080`              | `SEED_HTTP_ADDR`          | Address the HTTP server listens on (e.g. `:8080`, `127.0.0.1:9000`).

# Running

Build and run the service. The process starts an HTTP server (default `:8080`) and exposes `POST /seed` to accept seeds.

```bash
# Build
go build -o crawler-seed ./...

# Run (defaults to listening on :8080)
./crawler-seed

# Override Kafka broker and HTTP address via environment variables
SEED_KAFKA_BROKER=broker:9092 SEED_HTTP_ADDR=":8080" ./crawler-seed
```

## What it does

1. Starts an HTTP server that accepts seed submissions via `POST /seed`.
2. Connects to Kafka and verifies connectivity.
3. For each submitted seed URL, constructs a `DiscoveredURL` event and produces it to the configured `topic_discovered` Kafka topic.

Example event payload:

```json
{
  "url": "https://example.com",
  "depth": 0,
  "source_url": "",
  "enqueued_at": "2026-04-25T10:00:00Z"
}
```

The process remains running to accept further `POST /seed` requests.

## HTTP API

POST /seed
- Accepts JSON body: `{ "url": "https://example.com", "depth": 2 }`
- `url` is required and must be a valid absolute URL.
- `depth` is optional; omitted values use the configured default `depth`.

Example using `curl`:

```bash
curl -X POST http://localhost:8080/seed \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com","depth":2}'
```

Response (202 Accepted):

```json
{ "published": true, "url": "https://example.com", "depth": 2 }
```

## Kafka topics

Topic names are configurable via `config.yml` or environment variables (see Configuration above).

| Config key         | Default           | Direction | Message type    |
|--------------------|-------------------|-----------|-----------------|
| `topic_discovered` | `discovered-urls` | Produce   | `DiscoveredURL` |
