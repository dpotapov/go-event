package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	goevent "github.com/dpotapov/go-event"
	gonats "github.com/nats-io/nats.go"
)

type DispatcherConfig struct {
	RequestTimeout time.Duration
}

type Dispatcher struct {
	nc      *gonats.Conn
	timeout time.Duration
}

var _ goevent.Dispatcher = (*Dispatcher)(nil)

func NewDispatcher(nc *gonats.Conn, cfg DispatcherConfig) (*Dispatcher, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is required")
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Dispatcher{nc: nc, timeout: timeout}, nil
}

func (d *Dispatcher) DispatchCommand(ctx context.Context, cmd goevent.Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	return d.request(ctx, CommandSubject(cmd), data, nil)
}

func (d *Dispatcher) DispatchQuery(ctx context.Context, query goevent.Command, result any) error {
	data, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("marshal query: %w", err)
	}
	return d.request(ctx, QuerySubject(query), data, result)
}

func (d *Dispatcher) request(ctx context.Context, subject string, data []byte, result any) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	resp, err := d.nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		if errors.Is(err, gonats.ErrNoResponders) {
			return goevent.ErrNoResponders
		}
		return fmt.Errorf("nats request %s: %w", subject, err)
	}
	var envelope responseEnvelope
	if err := json.Unmarshal(resp.Data, &envelope); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if !envelope.OK {
		if envelope.Error == "" {
			return errors.New("handler failed")
		}
		return errors.New(envelope.Error)
	}
	if result != nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, result); err != nil {
			return fmt.Errorf("decode query result: %w", err)
		}
	}
	return nil
}

type CommandSubscriberConfig struct {
	Catalog *goevent.Catalog
	Queue   string
	Logger  *slog.Logger
}

type CommandSubscriber struct {
	nc      *gonats.Conn
	catalog *goevent.Catalog
	queue   string
	logger  *slog.Logger
}

var _ goevent.CommandSubscriber = (*CommandSubscriber)(nil)

func NewCommandSubscriber(nc *gonats.Conn, cfg CommandSubscriberConfig) (*CommandSubscriber, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is required")
	}
	catalog := cfg.Catalog
	if catalog == nil {
		catalog = goevent.NewCatalog()
	}
	queue := cfg.Queue
	if queue == "" {
		queue = "goevent"
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &CommandSubscriber{nc: nc, catalog: catalog, queue: queue, logger: logger}, nil
}

func (s *CommandSubscriber) SubscribeCommand(ctx context.Context, handler goevent.CommandHandler, cfg goevent.CommandSubscriptionConfig) (goevent.Subscription, error) {
	return s.subscribe(ctx, goevent.KindCommand, goevent.CommandHandlerFunc(func(ctx context.Context, cmd goevent.Command) error {
		return handler.HandleCommand(ctx, cmd)
	}), nil, cfg)
}

func (s *CommandSubscriber) SubscribeQuery(ctx context.Context, handler goevent.QueryHandler, cfg goevent.CommandSubscriptionConfig) (goevent.Subscription, error) {
	return s.subscribe(ctx, goevent.KindQuery, nil, handler, cfg)
}

func (s *CommandSubscriber) subscribe(
	ctx context.Context,
	kind goevent.MessageKind,
	commandHandler goevent.CommandHandler,
	queryHandler goevent.QueryHandler,
	cfg goevent.CommandSubscriptionConfig,
) (goevent.Subscription, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	queue := cfg.Queue
	if queue == "" {
		queue = s.queue
	}
	group := &natsSubscriptions{}
	for _, filter := range commandFilters(kind, cfg) {
		filter := filter
		sub, err := s.nc.QueueSubscribe(filter, queue, func(msg *gonats.Msg) {
			s.handle(ctx, kind, msg, commandHandler, queryHandler)
		})
		if err != nil {
			_ = group.Stop(context.Background())
			return nil, fmt.Errorf("subscribe %s: %w", filter, err)
		}
		group.subs = append(group.subs, sub)
	}
	return group, nil
}

func (s *CommandSubscriber) handle(ctx context.Context, kind goevent.MessageKind, msg *gonats.Msg, commandHandler goevent.CommandHandler, queryHandler goevent.QueryHandler) {
	addr, ok := parseAddress(msg.Subject)
	if !ok || addr.Kind != kind {
		_ = respond(msg, responseEnvelope{OK: false, Error: "invalid subject"})
		return
	}
	cmd, err := s.catalog.DecodeCommand(addr, msg.Data)
	if err != nil {
		s.logger.ErrorContext(ctx, "decode command failed", "subject", msg.Subject, "err", err)
		_ = respond(msg, responseEnvelope{OK: false, Error: err.Error()})
		return
	}
	if commandHandler != nil {
		err = commandHandler.HandleCommand(ctx, cmd)
		if err != nil {
			_ = respond(msg, responseEnvelope{OK: false, Error: err.Error()})
			return
		}
		_ = respond(msg, responseEnvelope{OK: true})
		return
	}
	result, err := queryHandler.HandleQuery(ctx, cmd)
	if err != nil {
		_ = respond(msg, responseEnvelope{OK: false, Error: err.Error()})
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		_ = respond(msg, responseEnvelope{OK: false, Error: fmt.Sprintf("marshal query result: %v", err)})
		return
	}
	_ = respond(msg, responseEnvelope{OK: true, Data: data})
}

type responseEnvelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
}

func respond(msg *gonats.Msg, envelope responseEnvelope) error {
	if msg.Reply == "" {
		return nil
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	return msg.Respond(data)
}

type natsSubscriptions struct {
	subs []*gonats.Subscription
}

func (s *natsSubscriptions) Stop(ctx context.Context) error {
	var firstErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, sub := range s.subs {
			if err := sub.Drain(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}()
	select {
	case <-done:
		return firstErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
