package nats

import (
	"fmt"
	"log/slog"
	"time"

	goevent "github.com/dpotapov/go-event"
	gonats "github.com/nats-io/nats.go"
)

// BusConfig configures the NATS-backed bus helper.
type BusConfig struct {
	Queue                  string
	QueueMaxRetries        int
	RequestTimeout         time.Duration
	Logger                 *slog.Logger
	CursorStore            goevent.CursorStore
	CursorBootPolicy       goevent.CursorBootPolicy
	OnSubscriptionError    func(*goevent.SubscriptionError) bool
	CommandConflictRetries int
}

// NewBus constructs a go-event Bus using NATS publisher, dispatcher,
// command subscriber, and event subscriber implementations.
func NewBus(nc *gonats.Conn, cfg BusConfig) (*goevent.Bus, error) {
	dispatcher, err := NewDispatcher(nc, DispatcherConfig{
		RequestTimeout: cfg.RequestTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create dispatcher: %w", err)
	}
	publisher, err := NewPublisher(nc, PublisherConfig{
		Logger: cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create publisher: %w", err)
	}
	commands, err := NewCommandSubscriber(nc, CommandSubscriberConfig{
		Queue:  cfg.Queue,
		Logger: cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create command subscriber: %w", err)
	}
	events, err := NewSubscriber(nc, SubscriberConfig{
		Logger: cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create event subscriber: %w", err)
	}
	return goevent.NewBus(dispatcher, publisher, commands, events, goevent.BusOptions{
		Queue:                  cfg.Queue,
		QueueMaxRetries:        cfg.QueueMaxRetries,
		Logger:                 cfg.Logger,
		CursorStore:            cfg.CursorStore,
		CursorBootPolicy:       cfg.CursorBootPolicy,
		OnSubscriptionError:    cfg.OnSubscriptionError,
		CommandConflictRetries: cfg.CommandConflictRetries,
	}), nil
}
