package testkit

import (
	"context"
	"fmt"

	goevent "github.com/dpotapov/go-event"
)

type StubDispatcher struct {
	Commands []goevent.Command
	Queries  []goevent.Command
	QueryFn  func(context.Context, goevent.Command, any) error
	Err      error
}

func (s *StubDispatcher) DispatchCommand(ctx context.Context, cmd goevent.Command) error {
	if s.Err != nil {
		return s.Err
	}
	s.Commands = append(s.Commands, cmd)
	return nil
}

func (s *StubDispatcher) DispatchQuery(ctx context.Context, query goevent.Command, result any) error {
	if s.Err != nil {
		return s.Err
	}
	s.Queries = append(s.Queries, query)
	if s.QueryFn != nil {
		return s.QueryFn(ctx, query, result)
	}
	return nil
}

type StubPublisher struct {
	Events []goevent.Event
	Err    error
}

func (s *StubPublisher) Publish(ctx context.Context, events ...goevent.Event) error {
	if s.Err != nil {
		return s.Err
	}
	s.Events = append(s.Events, events...)
	return nil
}

type EventSubscription struct {
	Handler goevent.EventHandler
	Config  goevent.EventSubscriptionConfig
}

type StubEventSubscriber struct {
	Subscriptions []EventSubscription
	Err           error
}

func (s *StubEventSubscriber) SubscribeEvents(ctx context.Context, handler goevent.EventHandler, cfg goevent.EventSubscriptionConfig) (goevent.Subscription, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	s.Subscriptions = append(s.Subscriptions, EventSubscription{Handler: handler, Config: cfg})
	if cfg.Queue == "" && cfg.CaughtUp != nil {
		cfg.CaughtUp()
	}
	return goevent.SubscriptionFunc(func() error { return nil }), nil
}

type CommandSubscription struct {
	Handler goevent.CommandHandler
	Config  goevent.CommandSubscriptionConfig
}

type QuerySubscription struct {
	Handler goevent.QueryHandler
	Config  goevent.CommandSubscriptionConfig
}

type StubCommandSubscriber struct {
	CommandSubscriptions []CommandSubscription
	QuerySubscriptions   []QuerySubscription
	Err                  error
}

func (s *StubCommandSubscriber) SubscribeCommand(ctx context.Context, handler goevent.CommandHandler, cfg goevent.CommandSubscriptionConfig) (goevent.Subscription, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	s.CommandSubscriptions = append(s.CommandSubscriptions, CommandSubscription{Handler: handler, Config: cfg})
	return goevent.SubscriptionFunc(func() error { return nil }), nil
}

func (s *StubCommandSubscriber) SubscribeQuery(ctx context.Context, handler goevent.QueryHandler, cfg goevent.CommandSubscriptionConfig) (goevent.Subscription, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	s.QuerySubscriptions = append(s.QuerySubscriptions, QuerySubscription{Handler: handler, Config: cfg})
	return goevent.SubscriptionFunc(func() error { return nil }), nil
}

type BusHarness struct {
	Bus        *goevent.Bus
	Dispatcher *StubDispatcher
	Publisher  *StubPublisher
	Commands   *StubCommandSubscriber
	Events     *StubEventSubscriber
}

func NewBusHarness(queue string, opts goevent.BusOptions) *BusHarness {
	dispatcher := &StubDispatcher{}
	publisher := &StubPublisher{}
	commands := &StubCommandSubscriber{}
	events := &StubEventSubscriber{}
	opts.Queue = queue
	bus := goevent.NewBus(dispatcher, publisher, commands, events, opts)
	return &BusHarness{
		Bus:        bus,
		Dispatcher: dispatcher,
		Publisher:  publisher,
		Commands:   commands,
		Events:     events,
	}
}

func (h *BusHarness) TriggerEvent(ctx context.Context, evt goevent.Event, cursor []byte) error {
	for _, sub := range h.Events.Subscriptions {
		if eventConfigMatches(sub.Config, evt) {
			return sub.Handler.HandleEvent(ctx, evt, cursor)
		}
	}
	return fmt.Errorf("no event subscription for %s", goevent.EventSubject(evt))
}

func (h *BusHarness) TriggerCommand(ctx context.Context, cmd goevent.Command) error {
	for _, sub := range h.Commands.CommandSubscriptions {
		if commandConfigMatches(sub.Config, cmd) {
			return sub.Handler.HandleCommand(ctx, cmd)
		}
	}
	return fmt.Errorf("no command subscription for %s", goevent.CommandSubject(cmd))
}

func (h *BusHarness) TriggerQuery(ctx context.Context, query goevent.Command) (any, error) {
	for _, sub := range h.Commands.QuerySubscriptions {
		if commandConfigMatches(sub.Config, query) {
			return sub.Handler.HandleQuery(ctx, query)
		}
	}
	return nil, fmt.Errorf("no query subscription for %s", goevent.QuerySubject(query))
}

func eventConfigMatches(cfg goevent.EventSubscriptionConfig, evt goevent.Event) bool {
	if cfg.AggregateScope != "" && cfg.AggregateScope != evt.AggregateScope() {
		return false
	}
	if len(cfg.EventFilters) > 0 {
		if len(cfg.AggregateTypes) > 0 && !contains(cfg.AggregateTypes, evt.AggregateType()) {
			return false
		}
		for _, filter := range cfg.EventFilters {
			if eventFilterMatches(filter, evt) {
				return true
			}
		}
		return false
	}
	kind := cfg.Kind
	if kind == "" {
		kind = goevent.KindEvent
	}
	if kind != goevent.EventSubject(evt).Kind {
		return false
	}
	if len(cfg.AggregateTypes) > 0 && !contains(cfg.AggregateTypes, evt.AggregateType()) {
		return false
	}
	if len(cfg.AggregateIDs) > 0 && !contains(cfg.AggregateIDs, evt.AggregateID()) {
		return false
	}
	if len(cfg.EventNames) > 0 && !contains(cfg.EventNames, evt.EventName()) {
		return false
	}
	return true
}

func eventFilterMatches(filter goevent.EventSubscriptionFilter, evt goevent.Event) bool {
	kind := filter.Kind
	if kind == "" {
		kind = goevent.KindEvent
	}
	if kind != goevent.EventSubject(evt).Kind {
		return false
	}
	if len(filter.AggregateIDs) > 0 && !contains(filter.AggregateIDs, evt.AggregateID()) {
		return false
	}
	return len(filter.EventNames) == 0 || contains(filter.EventNames, evt.EventName())
}

func commandConfigMatches(cfg goevent.CommandSubscriptionConfig, cmd goevent.Command) bool {
	if cfg.AggregateScope != "" && cfg.AggregateScope != cmd.AggregateScope() {
		return false
	}
	kind := cfg.Kind
	if kind == "" {
		kind = goevent.KindCommand
	}
	if kind != goevent.CommandSubject(cmd).Kind && kind != goevent.QuerySubject(cmd).Kind {
		return false
	}
	if len(cfg.AggregateTypes) > 0 && !contains(cfg.AggregateTypes, cmd.AggregateType()) {
		return false
	}
	if len(cfg.AggregateIDs) > 0 && !contains(cfg.AggregateIDs, cmd.AggregateID()) {
		return false
	}
	if len(cfg.CommandNames) > 0 && !contains(cfg.CommandNames, cmd.CommandName()) {
		return false
	}
	return true
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
