package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
)

type BusOptions struct {
	Queue                    string
	QueueMaxRetries          int
	Logger                   *slog.Logger
	CursorStore              CursorStore
	CursorBootPolicy         CursorBootPolicy
	OnSubscriptionError      func(*SubscriptionError) bool
	CommandConflictRetries   int
	ExcludeCommandDebugNames map[string]bool
}

type Bus struct {
	dispatcher Dispatcher
	publisher  Publisher
	commands   CommandSubscriber
	events     EventSubscriber
	registry   *registry
	queue      string
	logger     *slog.Logger

	cursorStore              CursorStore
	cursorBootPolicy         CursorBootPolicy
	onSubscriptionError      func(*SubscriptionError) bool
	queueMaxRetries          int
	commandConflictRetries   int
	excludeCommandDebugNames map[string]bool

	mu            sync.Mutex
	running       bool
	closed        bool
	ctx           context.Context
	cancel        context.CancelFunc
	orderedGroups map[orderedKey]*eventGroup
	queueGroups   map[queueKey]*eventGroup
	commandsRegs  []*commandRegistration
	queryRegs     []*queryRegistration
	done          chan error
	doneOnce      sync.Once
	stopErr       error
}

func NewBus(dispatcher Dispatcher, publisher Publisher, commands CommandSubscriber, events EventSubscriber, opts BusOptions) *Bus {
	registry := newRegistry()
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	bindRegistry(commands, registry)
	bindRegistry(events, registry)
	retries := opts.CommandConflictRetries
	if retries == 0 {
		retries = 5
	}
	return &Bus{
		dispatcher:               dispatcher,
		publisher:                publisher,
		commands:                 commands,
		events:                   events,
		registry:                 registry,
		queue:                    opts.Queue,
		logger:                   logger,
		cursorStore:              opts.CursorStore,
		cursorBootPolicy:         opts.CursorBootPolicy,
		onSubscriptionError:      opts.OnSubscriptionError,
		queueMaxRetries:          opts.QueueMaxRetries,
		commandConflictRetries:   retries,
		excludeCommandDebugNames: opts.ExcludeCommandDebugNames,
		orderedGroups:            make(map[orderedKey]*eventGroup),
		queueGroups:              make(map[queueKey]*eventGroup),
		done:                     make(chan error, 1),
	}
}

// registryBinder is optional glue for transport adapters. Bus owns the decoder
// registry populated by typed handlers; adapters that decode incoming bytes bind
// to it so command/event subscriptions see the same registered Go types.
type registryBinder interface {
	BindRegistry(DecoderRegistry)
}

func bindRegistry(target any, registry DecoderRegistry) {
	if binder, ok := target.(registryBinder); ok {
		binder.BindRegistry(registry)
	}
}

func (b *Bus) Connect(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if b.running {
		b.mu.Unlock()
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	b.ctx = runCtx
	b.cancel = cancel
	b.running = true

	ordered := values(b.orderedGroups)
	commands := append([]*commandRegistration(nil), b.commandsRegs...)
	queries := append([]*queryRegistration(nil), b.queryRegs...)
	queues := values(b.queueGroups)
	b.mu.Unlock()

	caughtUp := make(chan struct{}, len(ordered))
	for _, group := range ordered {
		if err := b.startOrderedGroup(runCtx, group, caughtUp); err != nil {
			_ = b.Disconnect()
			return err
		}
	}
	for range ordered {
		select {
		case <-caughtUp:
		case <-runCtx.Done():
			_ = b.Disconnect()
			return runCtx.Err()
		}
	}
	for _, reg := range commands {
		if err := b.startCommand(runCtx, reg); err != nil {
			_ = b.Disconnect()
			return err
		}
	}
	for _, reg := range queries {
		if err := b.startQuery(runCtx, reg); err != nil {
			_ = b.Disconnect()
			return err
		}
	}
	for _, group := range queues {
		if err := b.startQueueGroup(runCtx, group); err != nil {
			_ = b.Disconnect()
			return err
		}
	}
	return nil
}

func (b *Bus) Disconnect() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.running = false
	cancel := b.cancel
	b.cancel = nil

	var subs []Subscription
	for _, group := range b.orderedGroups {
		if group.sub != nil {
			subs = append(subs, group.sub)
			group.sub = nil
		}
	}
	for _, group := range b.queueGroups {
		if group.sub != nil {
			subs = append(subs, group.sub)
			group.sub = nil
		}
	}
	for _, reg := range b.commandsRegs {
		if reg.sub != nil {
			subs = append(subs, reg.sub)
			reg.sub = nil
		}
	}
	for _, reg := range b.queryRegs {
		if reg.sub != nil {
			subs = append(subs, reg.sub)
			reg.sub = nil
		}
	}
	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	var firstErr error
	for _, sub := range subs {
		if err := sub.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	b.signalDone(firstErr)
	return firstErr
}

func (b *Bus) Done() <-chan error {
	return b.done
}

func (b *Bus) RegisterMessageType(prototype any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mustAllowRegistrationLocked()
	return b.registry.RegisterMessageType(prototype)
}

func (b *Bus) mustAllowRegistrationLocked() {
	switch {
	case b.closed:
		panic(ErrClosed)
	case b.running:
		panic("bus is connected")
	}
}

func (b *Bus) DispatchCommand(ctx context.Context, cmd Command) error {
	if b.dispatcher == nil {
		return ErrNoResponders
	}
	subj := CommandSubject(cmd)
	if !b.excludeCommandDebugNames[cmd.CommandName()] {
		b.logger.DebugContext(ctx, "dispatch command", "subject", subj)
	}
	return b.dispatcher.DispatchCommand(ctx, cmd)
}

func (b *Bus) DispatchQuery(ctx context.Context, query Command, result any) error {
	if b.dispatcher == nil {
		return ErrNoResponders
	}
	subj := QuerySubject(query)
	b.logger.DebugContext(ctx, "dispatch query", "subject", subj)
	return b.dispatcher.DispatchQuery(ctx, query, result)
}

func (b *Bus) Publish(ctx context.Context, events ...Event) error {
	if b.publisher == nil {
		return ErrNoResponders
	}
	return b.publisher.Publish(ctx, events...)
}

type HandlerOption func(*handlerOptions)

type handlerOptions struct {
	aggregateIDs []string
}

func WithAggregateIDs(ids ...string) HandlerOption {
	return func(opts *handlerOptions) {
		opts.aggregateIDs = append([]string(nil), ids...)
	}
}

// HandleEvent registers an ordered event handler for T. It must be called before
// Connect; it panics if the bus is already connected or closed, or if T cannot
// be registered as a message type.
func HandleEvent[T Event](b *Bus, handler func(context.Context, T) error, opts ...HandlerOption) {
	prototype, err := newTypedValue[T]()
	if err != nil {
		panic(err)
	}
	if err := b.RegisterMessageType(prototype); err != nil {
		panic(err)
	}
	kind := messageKind(prototype, KindEvent)
	options := collectHandlerOptions(opts)
	wrapped := EventHandlerFunc(func(ctx context.Context, evt Event, cursor []byte) error {
		typed, ok := evt.(T)
		if !ok {
			return fmt.Errorf("event type mismatch: expected %s, got %T", reflect.TypeOf(prototype), evt)
		}
		return handler(ctx, typed)
	})
	b.addOrderedHandler(kind, prototype.AggregateScope(), prototype.AggregateType(), prototype.EventName(), options.aggregateIDs, wrapped)
}

// HandleQueueEvent registers a queue event handler for T. It must be called
// before Connect; it panics if the bus is already connected or closed, if T
// cannot be registered as a message type, or if the bus has no queue configured.
func HandleQueueEvent[T Event](b *Bus, handler func(context.Context, T) error, opts ...HandlerOption) {
	prototype, err := newTypedValue[T]()
	if err != nil {
		panic(err)
	}
	if err := b.RegisterMessageType(prototype); err != nil {
		panic(err)
	}
	kind := messageKind(prototype, KindEvent)
	options := collectHandlerOptions(opts)
	wrapped := EventHandlerFunc(func(ctx context.Context, evt Event, cursor []byte) error {
		typed, ok := evt.(T)
		if !ok {
			return fmt.Errorf("event type mismatch: expected %s, got %T", reflect.TypeOf(prototype), evt)
		}
		return handler(ctx, typed)
	})
	b.addQueueHandler(kind, prototype.AggregateScope(), prototype.AggregateType(), prototype.EventName(), options.aggregateIDs, wrapped)
}

// HandleCommand registers a command handler for T. It must be called before
// Connect; it panics if the bus is already connected or closed, or if T cannot
// be registered as a message type.
func HandleCommand[T Command](b *Bus, handler func(context.Context, T) error, opts ...HandlerOption) {
	prototype, err := newTypedValue[T]()
	if err != nil {
		panic(err)
	}
	if err := b.RegisterMessageType(prototype); err != nil {
		panic(err)
	}
	kind := messageKind(prototype, KindCommand)
	options := collectHandlerOptions(opts)
	wrapped := CommandHandlerFunc(func(ctx context.Context, cmd Command) error {
		typed, ok := cmd.(T)
		if !ok {
			return fmt.Errorf("command type mismatch: expected %s, got %T", reflect.TypeOf(prototype), cmd)
		}
		return handler(ctx, typed)
	})
	b.addCommandHandler(kind, prototype.AggregateScope(), prototype.AggregateType(), prototype.CommandName(), options.aggregateIDs, wrapped)
}

// HandleQuery registers a query handler for T. It must be called before
// Connect; it panics if the bus is already connected or closed, or if T cannot
// be registered as a message type.
func HandleQuery[T Command, R any](b *Bus, handler func(context.Context, T) (R, error)) {
	prototype, err := newTypedValue[T]()
	if err != nil {
		panic(err)
	}
	kind := messageKind(prototype, KindQuery)
	b.mu.Lock()
	b.mustAllowRegistrationLocked()
	b.mu.Unlock()
	if err := b.registry.registerCommand(prototype, kind); err != nil {
		panic(err)
	}
	wrapped := QueryHandlerFunc(func(ctx context.Context, query Command) (any, error) {
		typed, ok := query.(T)
		if !ok {
			return nil, fmt.Errorf("query type mismatch: expected %s, got %T", reflect.TypeOf(prototype), query)
		}
		return handler(ctx, typed)
	})
	b.addQueryHandler(kind, prototype.AggregateScope(), prototype.AggregateType(), prototype.CommandName(), wrapped)
}

type orderedKey struct {
	scope string
	kind  MessageKind
	agg   string
}

type queueKey struct {
	scope string
	agg   string
	queue string
}

type eventGroup struct {
	ordered  bool
	scope    string
	kind     MessageKind
	agg      string
	queue    string
	handlers map[uint64]registeredEventHandler
	sub      Subscription
}

type registeredEventHandler struct {
	eventKind    MessageKind
	eventName    string
	aggregateIDs []string
	handler      EventHandler
}

func (b *Bus) addOrderedHandler(kind MessageKind, scope, agg, name string, ids []string, handler EventHandler) {
	key := orderedKey{scope: scope, kind: kind, agg: agg}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mustAllowRegistrationLocked()
	group := b.orderedGroups[key]
	if group == nil {
		group = &eventGroup{ordered: true, scope: scope, kind: kind, agg: agg, handlers: make(map[uint64]registeredEventHandler)}
		b.orderedGroups[key] = group
	}
	group.handlers[uint64(len(group.handlers)+1)] = registeredEventHandler{eventKind: kind, eventName: name, aggregateIDs: ids, handler: handler}
}

func (b *Bus) addQueueHandler(kind MessageKind, scope, agg, name string, ids []string, handler EventHandler) {
	if b.queue == "" {
		panic("queue event handler requires BusOptions.Queue")
	}
	key := queueKey{scope: scope, agg: agg, queue: b.queue}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mustAllowRegistrationLocked()
	group := b.queueGroups[key]
	if group == nil {
		group = &eventGroup{scope: scope, kind: kind, agg: agg, queue: b.queue, handlers: make(map[uint64]registeredEventHandler)}
		b.queueGroups[key] = group
	}
	group.handlers[uint64(len(group.handlers)+1)] = registeredEventHandler{eventKind: kind, eventName: name, aggregateIDs: ids, handler: handler}
}

func (b *Bus) startOrderedGroup(ctx context.Context, group *eventGroup, caughtUp chan<- struct{}) error {
	if b.events == nil {
		return ErrNoResponders
	}
	stream := group.scope + "." + string(group.kind) + "." + group.agg
	var cursor []byte
	if b.cursorStore != nil {
		var err error
		cursor, err = b.cursorStore.LoadCursor(ctx, stream)
		if err != nil {
			return fmt.Errorf("load cursor %s: %w", stream, err)
		}
	}
	handler := EventHandlerFunc(func(ctx context.Context, evt Event, cursor []byte) error {
		return b.dispatchEventGroup(ctx, group, evt, cursor)
	})
	if b.cursorStore != nil {
		next := handler
		handler = EventHandlerFunc(func(ctx context.Context, evt Event, cursor []byte) error {
			if err := next.HandleEvent(ctx, evt, cursor); err != nil {
				return err
			}
			return b.cursorStore.SaveCursor(ctx, stream, cursor)
		})
	}
	eventFilters := groupEventFilters(group)
	sub, err := b.events.SubscribeEvents(ctx, handler, EventSubscriptionConfig{
		AggregateScope:   group.scope,
		AggregateTypes:   []string{group.agg},
		EventFilters:     eventFilters,
		Cursor:           cursor,
		CursorBootPolicy: b.cursorBootPolicy,
		CaughtUp: func() {
			if caughtUp != nil {
				caughtUp <- struct{}{}
			}
		},
		OnError: b.makeOnError(false),
	})
	if err != nil {
		return fmt.Errorf("start ordered event subscription %s.%s: %w", group.scope, group.agg, err)
	}
	b.mu.Lock()
	if !b.running || len(group.handlers) == 0 || group.sub != nil {
		b.mu.Unlock()
		return sub.Stop()
	}
	group.sub = sub
	b.mu.Unlock()
	return nil
}

func (b *Bus) startQueueGroup(ctx context.Context, group *eventGroup) error {
	if b.events == nil {
		return ErrNoResponders
	}
	eventFilters := groupEventFilters(group)
	consumerName := group.queue
	sub, err := b.events.SubscribeEvents(ctx, EventHandlerFunc(func(ctx context.Context, evt Event, cursor []byte) error {
		return b.dispatchEventGroup(ctx, group, evt, cursor)
	}), EventSubscriptionConfig{
		AggregateScope: group.scope,
		AggregateTypes: []string{group.agg},
		EventFilters:   eventFilters,
		Queue:          group.queue,
		ConsumerName:   consumerName,
		MaxRetries:     b.queueMaxRetries,
		OnError:        b.makeOnError(true),
	})
	if err != nil {
		return fmt.Errorf("start queue event subscription %s: %w", consumerName, err)
	}
	b.mu.Lock()
	if !b.running || len(group.handlers) == 0 || group.sub != nil {
		b.mu.Unlock()
		return sub.Stop()
	}
	group.sub = sub
	b.mu.Unlock()
	return nil
}

func (b *Bus) dispatchEventGroup(ctx context.Context, group *eventGroup, evt Event, cursor []byte) error {
	b.mu.Lock()
	handlers := make([]registeredEventHandler, 0, len(group.handlers))
	for _, h := range group.handlers {
		handlers = append(handlers, h)
	}
	b.mu.Unlock()
	var firstErr error
	evtKind := EventSubject(evt).Kind
	for _, h := range handlers {
		if h.eventKind != "" && h.eventKind != evtKind {
			continue
		}
		if h.eventName != "" && h.eventName != evt.EventName() {
			continue
		}
		if len(h.aggregateIDs) > 0 && !slices.Contains(h.aggregateIDs, evt.AggregateID()) {
			continue
		}
		if err := h.handler.HandleEvent(ctx, evt, cursor); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type commandRegistration struct {
	cfg     CommandSubscriptionConfig
	handler CommandHandler
	sub     Subscription
}

type queryRegistration struct {
	cfg     CommandSubscriptionConfig
	handler QueryHandler
	sub     Subscription
}

func (b *Bus) addCommandHandler(kind MessageKind, scope, agg, name string, ids []string, handler CommandHandler) {
	reg := &commandRegistration{
		cfg: CommandSubscriptionConfig{
			AggregateScope: scope,
			Kind:           kind,
			AggregateTypes: []string{agg},
			AggregateIDs:   ids,
			CommandNames:   []string{name},
			Queue:          b.queue,
		},
		handler: b.withCommandConflictRetry(b.withCommandLogging(handler)),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mustAllowRegistrationLocked()
	b.commandsRegs = append(b.commandsRegs, reg)
}

func (b *Bus) addQueryHandler(kind MessageKind, scope, agg, name string, handler QueryHandler) {
	reg := &queryRegistration{
		cfg: CommandSubscriptionConfig{
			AggregateScope: scope,
			Kind:           kind,
			AggregateTypes: []string{agg},
			CommandNames:   []string{name},
			Queue:          b.queue,
		},
		handler: b.withQueryLogging(handler),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mustAllowRegistrationLocked()
	b.queryRegs = append(b.queryRegs, reg)
}

func (b *Bus) startCommand(ctx context.Context, reg *commandRegistration) error {
	if b.commands == nil {
		return ErrNoResponders
	}
	sub, err := b.commands.SubscribeCommand(ctx, reg.handler, reg.cfg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	reg.sub = sub
	b.mu.Unlock()
	return nil
}

func (b *Bus) startQuery(ctx context.Context, reg *queryRegistration) error {
	if b.commands == nil {
		return ErrNoResponders
	}
	sub, err := b.commands.SubscribeQuery(ctx, reg.handler, reg.cfg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	reg.sub = sub
	b.mu.Unlock()
	return nil
}

func (b *Bus) withCommandLogging(next CommandHandler) CommandHandler {
	return CommandHandlerFunc(func(ctx context.Context, cmd Command) error {
		if !b.excludeCommandDebugNames[cmd.CommandName()] {
			b.logger.DebugContext(ctx, "handle command", "subject", CommandSubject(cmd))
		}
		err := next.HandleCommand(ctx, cmd)
		if err != nil {
			b.logger.ErrorContext(ctx, "command failed", "subject", CommandSubject(cmd), "err", err)
		}
		return err
	})
}

func (b *Bus) withQueryLogging(next QueryHandler) QueryHandler {
	return QueryHandlerFunc(func(ctx context.Context, query Command) (any, error) {
		b.logger.DebugContext(ctx, "handle query", "subject", QuerySubject(query))
		result, err := next.HandleQuery(ctx, query)
		if err != nil {
			b.logger.ErrorContext(ctx, "query failed", "subject", QuerySubject(query), "err", err)
		}
		return result, err
	})
}

func (b *Bus) withCommandConflictRetry(next CommandHandler) CommandHandler {
	return CommandHandlerFunc(func(ctx context.Context, cmd Command) error {
		var lastErr error
		for attempt := 0; attempt < b.commandConflictRetries; attempt++ {
			err := next.HandleCommand(ctx, cmd)
			if err == nil {
				return nil
			}
			if !errors.Is(err, ErrConflict) {
				return err
			}
			lastErr = err
		}
		return lastErr
	})
}

func (b *Bus) makeOnError(isQueue bool) func(*SubscriptionError) bool {
	return func(err *SubscriptionError) bool {
		if err.Transport && b.onSubscriptionError == nil {
			b.logger.Warn("transport subscription error", "err", err)
			return true
		}
		if !isQueue && b.onSubscriptionError == nil {
			b.logger.Warn("ordered handler error", "err", err)
			return true
		}
		if b.onSubscriptionError != nil && b.onSubscriptionError(err) {
			return true
		}
		b.mu.Lock()
		b.stopErr = err
		b.mu.Unlock()
		go func() { _ = b.Disconnect() }()
		return false
	}
}

func (b *Bus) signalDone(err error) {
	b.doneOnce.Do(func() {
		b.mu.Lock()
		if b.stopErr != nil {
			err = b.stopErr
		}
		b.mu.Unlock()
		if err != nil {
			b.done <- err
		}
		close(b.done)
	})
}

func collectHandlerOptions(opts []HandlerOption) handlerOptions {
	var out handlerOptions
	for _, opt := range opts {
		opt(&out)
	}
	return out
}

func values[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

type eventFilterSpec struct {
	kind MessageKind
	id   string
	name string
}

func groupEventFilters(group *eventGroup) []EventSubscriptionFilter {
	specs := make([]eventFilterSpec, 0, len(group.handlers))
	for _, h := range group.handlers {
		if len(h.aggregateIDs) == 0 {
			specs = append(specs, eventFilterSpec{kind: h.eventKind, name: h.eventName})
			continue
		}
		for _, id := range h.aggregateIDs {
			specs = append(specs, eventFilterSpec{kind: h.eventKind, id: id, name: h.eventName})
		}
	}
	specs = compactEventFilterSpecs(specs)
	filters := make([]EventSubscriptionFilter, 0, len(specs))
	for _, spec := range specs {
		filter := EventSubscriptionFilter{Kind: spec.kind}
		if spec.id != "" {
			filter.AggregateIDs = []string{spec.id}
		}
		if spec.name != "" {
			filter.EventNames = []string{spec.name}
		}
		filters = append(filters, filter)
	}
	return filters
}

func compactEventFilterSpecs(filters []eventFilterSpec) []eventFilterSpec {
	seen := make(map[eventFilterSpec]struct{}, len(filters))
	deduped := make([]eventFilterSpec, 0, len(filters))
	for _, filter := range filters {
		if _, ok := seen[filter]; ok {
			continue
		}
		seen[filter] = struct{}{}
		deduped = append(deduped, filter)
	}
	out := make([]eventFilterSpec, 0, len(deduped))
	for _, filter := range deduped {
		covered := false
		for _, candidate := range deduped {
			if candidate == filter {
				continue
			}
			if eventFilterSpecCovers(candidate, filter) {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, filter)
		}
	}
	slices.SortFunc(out, func(a, b eventFilterSpec) int {
		if a.kind != b.kind {
			return strings.Compare(string(a.kind), string(b.kind))
		}
		if a.id != b.id {
			return strings.Compare(a.id, b.id)
		}
		return strings.Compare(a.name, b.name)
	})
	return out
}

func eventFilterSpecCovers(candidate, filter eventFilterSpec) bool {
	return candidate.kind == filter.kind &&
		(candidate.id == "" || candidate.id == filter.id) &&
		(candidate.name == "" || candidate.name == filter.name)
}
