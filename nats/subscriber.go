package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	goevent "github.com/dpotapov/go-event"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type SubscriberConfig struct {
	Catalog       *goevent.Catalog
	StreamPattern string
	Logger        *slog.Logger

	ConsumerConfig        func(*jetstream.ConsumerConfig)
	OrderedConsumerConfig func(*jetstream.OrderedConsumerConfig)
	HeartbeatInterval     time.Duration
}

type Subscriber struct {
	js            jetstream.JetStream
	catalog       *goevent.Catalog
	streamPattern string
	logger        *slog.Logger

	consumerConfig        func(*jetstream.ConsumerConfig)
	orderedConsumerConfig func(*jetstream.OrderedConsumerConfig)
	heartbeatInterval     time.Duration
}

var _ goevent.EventSubscriber = (*Subscriber)(nil)

func NewSubscriber(nc *gonats.Conn, cfg SubscriberConfig) (*Subscriber, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is required")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	catalog := cfg.Catalog
	if catalog == nil {
		catalog = goevent.NewCatalog()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	pattern := cfg.StreamPattern
	if pattern == "" {
		pattern = DefaultStreamPattern
	}
	heartbeat := cfg.HeartbeatInterval
	if heartbeat <= 0 {
		heartbeat = 15 * time.Second
	}
	return &Subscriber{
		js:                    js,
		catalog:               catalog,
		streamPattern:         pattern,
		logger:                logger,
		consumerConfig:        cfg.ConsumerConfig,
		orderedConsumerConfig: cfg.OrderedConsumerConfig,
		heartbeatInterval:     heartbeat,
	}, nil
}

func (s *Subscriber) SubscribeEvents(ctx context.Context, handler goevent.EventHandler, cfg goevent.EventSubscriptionConfig) (goevent.Subscription, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.AggregateScope == "" || len(cfg.AggregateTypes) != 1 {
		return nil, fmt.Errorf("nats event subscription requires one aggregate scope and one aggregate type")
	}
	if cfg.Queue == "" {
		return s.subscribeOrdered(ctx, handler, cfg)
	}
	return s.subscribeQueue(ctx, handler, cfg)
}

func (s *Subscriber) subscribeOrdered(ctx context.Context, handler goevent.EventHandler, cfg goevent.EventSubscriptionConfig) (goevent.Subscription, error) {
	stream := s.streamFor(cfg.AggregateScope, cfg.AggregateTypes[0])
	filters := eventFilters(cfg)
	consumerCfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:    filters,
		InactiveThreshold: 5 * time.Second,
	}
	if len(filters) == 1 {
		consumerCfg.FilterSubjects = []string{filters[0]}
	}
	if cfg.Cursor != nil {
		consumerCfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		consumerCfg.OptStartSeq = cursorToSeq(cfg.Cursor) + 1
	} else if cfg.CursorBootPolicy == goevent.CursorBootAll {
		consumerCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	} else {
		consumerCfg.DeliverPolicy = jetstream.DeliverNewPolicy
	}
	if s.orderedConsumerConfig != nil {
		s.orderedConsumerConfig(&consumerCfg)
	}
	consumer, err := s.js.OrderedConsumer(ctx, stream, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("create ordered consumer: %w", err)
	}
	return s.consume(ctx, consumer, handler, cfg)
}

func (s *Subscriber) subscribeQueue(ctx context.Context, handler goevent.EventHandler, cfg goevent.EventSubscriptionConfig) (goevent.Subscription, error) {
	stream := s.streamFor(cfg.AggregateScope, cfg.AggregateTypes[0])
	filters := eventFilters(cfg)
	group := &subscriptionGroup{}
	for _, filter := range filters {
		name := cfg.ConsumerName
		if name == "" {
			name = cfg.Queue
		}
		if len(filters) > 1 {
			name = consumerName(name, filter)
		} else {
			name = sanitizeName(name)
		}
		consumerCfg := jetstream.ConsumerConfig{
			Name:          name,
			Durable:       name,
			FilterSubject: filter,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			ReplayPolicy:  jetstream.ReplayInstantPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    1,
		}
		switch {
		case cfg.MaxRetries < 0:
			consumerCfg.MaxDeliver = -1
		case cfg.MaxRetries > 0:
			consumerCfg.MaxDeliver = cfg.MaxRetries + 1
		}
		if cfg.Cursor != nil {
			consumerCfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
			consumerCfg.OptStartSeq = cursorToSeq(cfg.Cursor) + 1
		}
		if s.consumerConfig != nil {
			s.consumerConfig(&consumerCfg)
		}
		consumer, err := s.js.CreateOrUpdateConsumer(ctx, stream, consumerCfg)
		if err != nil {
			_ = group.Stop(context.Background())
			return nil, fmt.Errorf("create queue consumer %s: %w", name, err)
		}
		sub, err := s.consume(ctx, consumer, handler, cfg)
		if err != nil {
			_ = group.Stop(context.Background())
			return nil, err
		}
		group.subs = append(group.subs, sub)
	}
	return group, nil
}

func (s *Subscriber) consume(ctx context.Context, consumer jetstream.Consumer, handler goevent.EventHandler, cfg goevent.EventSubscriptionConfig) (goevent.Subscription, error) {
	runCtx, cancel := context.WithCancel(ctx)
	session := &consumeSession{
		sub:     s,
		ctx:     runCtx,
		cancel:  cancel,
		handler: handler,
		cfg:     cfg,
	}
	if info := consumer.CachedInfo(); info != nil && info.NumPending == 0 && cfg.CaughtUp != nil {
		session.caughtUpOnce.Do(cfg.CaughtUp)
	}
	opts := []jetstream.PullConsumeOpt{}
	if cfg.OnError != nil {
		opts = append(opts, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			info := &goevent.SubscriptionError{Queue: cfg.Queue, Transport: true, Err: err}
			if !cfg.OnError(info) {
				session.stop()
			}
		}))
	}
	cc, err := consumer.Consume(session.handleMessage, opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("consume events: %w", err)
	}
	session.setConsumeContext(cc)
	go func() {
		<-runCtx.Done()
		cc.Stop()
	}()
	return session, nil
}

func (s *Subscriber) streamFor(scope, aggregateType string) string {
	return StreamName(s.streamPattern, scope, aggregateType)
}

type subscriptionGroup struct {
	subs []goevent.Subscription
}

func (g *subscriptionGroup) Stop(ctx context.Context) error {
	var firstErr error
	for _, sub := range g.subs {
		if err := sub.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type consumeSession struct {
	sub     *Subscriber
	ctx     context.Context
	cancel  context.CancelFunc
	handler goevent.EventHandler
	cfg     goevent.EventSubscriptionConfig

	caughtUpOnce sync.Once

	mu sync.Mutex
	cc jetstream.ConsumeContext
}

func (s *consumeSession) setConsumeContext(cc jetstream.ConsumeContext) {
	s.mu.Lock()
	s.cc = cc
	s.mu.Unlock()
}

func (s *consumeSession) stop() {
	s.mu.Lock()
	cc := s.cc
	s.mu.Unlock()
	if cc != nil {
		cc.Stop()
	}
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *consumeSession) Stop(ctx context.Context) error {
	s.stop()
	s.mu.Lock()
	cc := s.cc
	s.mu.Unlock()
	if cc == nil {
		return nil
	}
	select {
	case <-cc.Closed():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *consumeSession) handleMessage(msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		s.sub.logger.WarnContext(s.ctx, "event metadata unavailable", "subject", msg.Subject(), "err", err)
		return
	}
	if meta.NumPending == 0 && s.cfg.CaughtUp != nil {
		defer s.caughtUpOnce.Do(s.cfg.CaughtUp)
	}
	logger := s.sub.logger.With("subject", msg.Subject(), "stream_seq", meta.Sequence.Stream, "consumer_seq", meta.Sequence.Consumer)

	var evt goevent.Event
	defer func() {
		if p := recover(); p != nil {
			s.onError(msg, logger, fmt.Errorf("panic: %v", p), evt, meta.NumDelivered)
		}
	}()

	evt, ok := s.decodeEvent(msg, meta, logger)
	if !ok {
		return
	}
	if len(s.cfg.AggregateIDs) > 0 && !slices.Contains(s.cfg.AggregateIDs, evt.AggregateID()) {
		s.ack(msg, logger)
		return
	}
	if s.cfg.Queue != "" {
		done := make(chan struct{})
		defer close(done)
		go s.heartbeat(msg, logger, done)
	}
	handlerCtx := goevent.WithMessageMetadata(s.ctx, goevent.MessageMetadata{
		NumDelivered: meta.NumDelivered,
		Timestamp:    meta.Timestamp,
	})
	if err := s.handler.HandleEvent(handlerCtx, evt, cursorFromSeq(meta.Sequence.Stream)); err != nil {
		s.onError(msg, logger, err, evt, meta.NumDelivered)
		return
	}
	s.ack(msg, logger)
}

func (s *consumeSession) decodeEvent(msg jetstream.Msg, meta *jetstream.MsgMetadata, logger *slog.Logger) (goevent.Event, bool) {
	addr, ok := parseAddress(msg.Subject())
	if !ok || addr.Class != goevent.ClassEvent {
		logger.WarnContext(s.ctx, "invalid event subject")
		s.ack(msg, logger)
		return nil, false
	}
	if len(s.cfg.EventNames) > 0 && !slices.Contains(s.cfg.EventNames, addr.Name) {
		s.ack(msg, logger)
		return nil, false
	}
	evt, err := s.sub.catalog.NewEvent(addr.Scope, addr.AggregateType, addr.Name)
	if err != nil {
		if errors.As(err, new(*goevent.UnknownEventError)) {
			logger.WarnContext(s.ctx, "unknown event type", "err", err)
			s.ack(msg, logger)
			return nil, false
		}
		s.nak(msg, logger, err)
		return nil, false
	}
	if codec, ok := evt.(Codec); ok {
		if err := codec.NATSUnmarshal(msg.Subject(), msg.Data(), meta.Timestamp); err != nil {
			logger.ErrorContext(s.ctx, "decode event failed", "err", err)
			s.ack(msg, logger)
			return nil, false
		}
	} else if err := json.Unmarshal(msg.Data(), evt); err != nil {
		logger.ErrorContext(s.ctx, "decode event failed", "err", err)
		s.ack(msg, logger)
		return nil, false
	}
	return evt, true
}

func (s *consumeSession) heartbeat(msg jetstream.Msg, logger *slog.Logger, done <-chan struct{}) {
	ticker := time.NewTicker(s.sub.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := msg.InProgress(); err != nil {
				logger.WarnContext(s.ctx, "event handler heartbeat failed", "err", err)
			}
		}
	}
}

func (s *consumeSession) ack(msg jetstream.Msg, logger *slog.Logger) {
	if s.cfg.Queue == "" {
		return
	}
	if err := msg.Ack(); err != nil {
		logger.ErrorContext(s.ctx, "ack event failed", "err", err)
	}
}

func (s *consumeSession) nak(msg jetstream.Msg, logger *slog.Logger, err error) {
	logger.ErrorContext(s.ctx, "event handler failed", "err", err)
	if s.cfg.Queue == "" {
		return
	}
	if nakErr := msg.Nak(); nakErr != nil {
		logger.ErrorContext(s.ctx, "nak event failed", "err", nakErr)
	}
}

func (s *consumeSession) onError(msg jetstream.Msg, logger *slog.Logger, err error, evt goevent.Event, numDelivered uint64) {
	s.nak(msg, logger, err)
	if s.cfg.OnError == nil {
		if s.cfg.Queue == "" {
			s.stop()
		}
		return
	}
	if s.cfg.Queue != "" {
		if s.cfg.MaxRetries < 0 {
			return
		}
		if numDelivered <= uint64(s.cfg.MaxRetries) {
			return
		}
	}
	info := &goevent.SubscriptionError{
		Queue:            s.cfg.Queue,
		NumDelivered:     numDelivered,
		RetriesExhausted: s.cfg.Queue != "",
		Err:              err,
	}
	if evt != nil {
		info.Address = goevent.EventAddress(evt)
	}
	if !s.cfg.OnError(info) {
		s.stop()
	}
}
