package kafkaconn

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// New creates a Kafka producer client and verifies connectivity.
func New(ctx context.Context, broker string) (*kgo.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}
	return cl, nil
}

// Publish synchronously produces a single record to Kafka.
func Publish(ctx context.Context, cl *kgo.Client, topic string, key, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	return cl.ProduceSync(ctx, rec).FirstErr()
}
