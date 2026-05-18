package nats

import (
	"context"
	"fmt"
	"log/slog"

	goevent "github.com/dpotapov/go-event"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type PublisherConfig struct {
	Logger *slog.Logger
}

type Publisher struct {
	js     jetstream.JetStream
	logger *slog.Logger
}

var _ goevent.Publisher = (*Publisher)(nil)

func NewPublisher(nc *gonats.Conn, cfg PublisherConfig) (*Publisher, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is required")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{js: js, logger: logger}, nil
}

func (p *Publisher) Publish(ctx context.Context, events ...goevent.Event) error {
	for _, evt := range events {
		data, err := marshalEvent(evt)
		if err != nil {
			return err
		}
		subj := goevent.EventSubject(evt)
		if err := subj.Validate(); err != nil {
			return err
		}
		if _, err := p.js.Publish(ctx, subj.String(), data); err != nil {
			return fmt.Errorf("publish event %s: %w", subj, err)
		}
		p.logger.DebugContext(ctx, "publish event", "subject", subj)
	}
	return nil
}
