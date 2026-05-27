package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	goevent "github.com/dpotapov/go-event"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type SubscriberConfig struct {
	StreamPattern string
	Logger        *slog.Logger

	ConsumerConfig        func(*jetstream.ConsumerConfig)
	OrderedConsumerConfig func(*jetstream.OrderedConsumerConfig)
	HeartbeatInterval     time.Duration
}

type Subscriber struct {
	js            jetstream.JetStream
	registry      goevent.DecoderRegistry
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
		registry:              goevent.NewTypeRegistry(),
		streamPattern:         pattern,
		logger:                logger,
		consumerConfig:        cfg.ConsumerConfig,
		orderedConsumerConfig: cfg.OrderedConsumerConfig,
		heartbeatInterval:     heartbeat,
	}, nil
}

func (s *Subscriber) BindRegistry(registry goevent.DecoderRegistry) {
	s.registry = registry
}

func (s *Subscriber) RegisterMessageType(prototype any) error {
	evt, ok := prototype.(goevent.Event)
	if !ok {
		return fmt.Errorf("subscriber message type %T must implement event.Event", prototype)
	}
	return s.registry.RegisterMessageType(evt)
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
	filters := queueConsumerFilters(cfg)
	name := cfg.ConsumerName
	if name == "" {
		name = cfg.Queue
	}
	name = sanitizeName(name)
	consumerCfg := jetstream.ConsumerConfig{
		Name:          name,
		Durable:       name,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    1,
	}
	if len(filters) == 1 {
		consumerCfg.FilterSubject = filters[0]
	} else {
		consumerCfg.FilterSubjects = filters
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
		return nil, fmt.Errorf("create queue consumer %s: %w", name, err)
	}
	return s.consume(ctx, consumer, handler, cfg)
}

func queueConsumerFilters(cfg goevent.EventSubscriptionConfig) []string {
	filters := eventFilters(cfg)
	if len(filters) <= 1 {
		return filters
	}
	if filter, ok := aggregateInstanceQueueFilter(cfg); ok {
		return []string{filter}
	}
	return filters
}

func aggregateInstanceQueueFilter(cfg goevent.EventSubscriptionConfig) (string, bool) {
	kind, id, ok := aggregateInstanceQueueScope(cfg)
	if !ok {
		return "", false
	}
	return strings.Join([]string{
		cfg.AggregateScope,
		string(kind),
		cfg.AggregateTypes[0],
		id,
		">",
	}, "."), true
}

func aggregateInstanceQueueScope(cfg goevent.EventSubscriptionConfig) (goevent.MessageKind, string, bool) {
	if len(cfg.EventFilters) == 0 {
		if len(cfg.AggregateIDs) != 1 {
			return "", "", false
		}
		kind := cfg.Kind
		if kind == "" {
			kind = goevent.KindEvent
		}
		return kind, cfg.AggregateIDs[0], true
	}

	var (
		outKind goevent.MessageKind
		outID   string
	)
	for _, filter := range cfg.EventFilters {
		if len(filter.AggregateIDs) != 1 {
			return "", "", false
		}
		kind := filter.Kind
		if kind == "" {
			kind = goevent.KindEvent
		}
		id := filter.AggregateIDs[0]
		if outKind == "" {
			outKind = kind
			outID = id
			continue
		}
		if outKind != kind || outID != id {
			return "", "", false
		}
	}
	return outKind, outID, outKind != "" && outID != ""
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

func (s *consumeSession) Stop() error {
	s.stop()
	s.mu.Lock()
	cc := s.cc
	s.mu.Unlock()
	if cc == nil {
		return nil
	}
	<-cc.Closed()
	return nil
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
	subj, ok := ParseSubject(msg.Subject())
	if !ok {
		logger.WarnContext(s.ctx, "invalid event subject")
		s.ack(msg, logger)
		return nil, false
	}
	if !subscriptionSubjectMatches(s.cfg, subj) {
		s.ack(msg, logger)
		return nil, false
	}
	evt, err := s.sub.registry.NewEvent(subj.Kind, subj.Scope, subj.AggregateType, subj.Name)
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

func subscriptionSubjectMatches(cfg goevent.EventSubscriptionConfig, subj goevent.Subject) bool {
	if len(cfg.EventFilters) > 0 {
		for _, filter := range cfg.EventFilters {
			if eventFilterMatchesSubject(filter, subj) {
				return true
			}
		}
		return false
	}
	kind := cfg.Kind
	if kind == "" {
		kind = goevent.KindEvent
	}
	if subj.Kind != kind {
		return false
	}
	return len(cfg.EventNames) == 0 || slices.Contains(cfg.EventNames, subj.Name)
}

func eventFilterMatchesSubject(filter goevent.EventSubscriptionFilter, subj goevent.Subject) bool {
	kind := filter.Kind
	if kind == "" {
		kind = goevent.KindEvent
	}
	if subj.Kind != kind {
		return false
	}
	if len(filter.AggregateIDs) > 0 && !slices.Contains(filter.AggregateIDs, subj.AggregateID) {
		return false
	}
	return len(filter.EventNames) == 0 || slices.Contains(filter.EventNames, subj.Name)
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
		info.Subject = goevent.EventSubject(evt)
	}
	if !s.cfg.OnError(info) {
		s.stop()
	}
}
