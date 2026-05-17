package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type StreamOption func(*jetstream.StreamConfig)

func EventStreamConfig(scope, aggregateType string, opts ...StreamOption) jetstream.StreamConfig {
	cfg := jetstream.StreamConfig{
		Name:               StreamName(DefaultStreamPattern, scope, aggregateType),
		Description:        fmt.Sprintf("go-event stream for %s.%s events", scope, aggregateType),
		Subjects:           []string{EventTypeFilter(scope, aggregateType)},
		Retention:          jetstream.LimitsPolicy,
		Discard:            jetstream.DiscardOld,
		Storage:            jetstream.FileStorage,
		Replicas:           1,
		MaxAge:             0,
		MaxMsgs:            -1,
		MaxBytes:           -1,
		MaxMsgsPerSubject:  -1,
		AllowDirect:        true,
		AllowAtomicPublish: true,
		Metadata: map[string]string{
			"go-event": "event-stream",
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func WithStreamName(name string) StreamOption {
	return func(cfg *jetstream.StreamConfig) {
		cfg.Name = name
	}
}

func WithStreamPattern(pattern, scope, aggregateType string) StreamOption {
	return func(cfg *jetstream.StreamConfig) {
		cfg.Name = StreamName(pattern, scope, aggregateType)
	}
}

func WithMemoryStorage() StreamOption {
	return func(cfg *jetstream.StreamConfig) {
		cfg.Storage = jetstream.MemoryStorage
	}
}

func WithMaxAge(maxAge time.Duration) StreamOption {
	return func(cfg *jetstream.StreamConfig) {
		cfg.MaxAge = maxAge
	}
}

func ConfigureStream(fn func(*jetstream.StreamConfig)) StreamOption {
	return func(cfg *jetstream.StreamConfig) {
		if fn != nil {
			fn(cfg)
		}
	}
}

func EnsureEventStream(ctx context.Context, js jetstream.JetStream, scope, aggregateType string, opts ...StreamOption) (jetstream.Stream, error) {
	cfg := EventStreamConfig(scope, aggregateType, opts...)
	stream, err := js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create or update event stream %s: %w", cfg.Name, err)
	}
	return stream, nil
}
