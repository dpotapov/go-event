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
	"sync/atomic"
)

type BusOptions struct {
	Catalog                  *Catalog
	Queue                    string
	Logger                   *slog.Logger
	CursorStore              CursorStore
	CursorBootPolicy         CursorBootPolicy
	OnSubscriptionError      func(*SubscriptionError) bool
	CommandConflictRetries   int
	ExcludeCommandDebugNames map[string]bool
}

type Bus struct {
	dispatcher Dispatcher
	commands   CommandSubscriber
	events     EventSubscriber
	catalog    *Catalog
	queue      string
	logger     *slog.Logger

	cursorStore              CursorStore
	cursorBootPolicy         CursorBootPolicy
	onSubscriptionError      func(*SubscriptionError) bool
	commandConflictRetries   int
	excludeCommandDebugNames map[string]bool

	mu            sync.Mutex
	running       bool
	closed        bool
	ctx           context.Context
	cancel        context.CancelFunc
	nextID        atomic.Uint64
	orderedGroups map[orderedKey]*eventGroup
	queueGroups   map[queueKey]*eventGroup
	commandsRegs  []*commandRegistration
	queryRegs     []*queryRegistration
	done          chan error
	doneOnce      sync.Once
	stopErr       error
}

func NewBus(dispatcher Dispatcher, commands CommandSubscriber, events EventSubscriber, opts BusOptions) *Bus {
	catalog := opts.Catalog
	if catalog == nil {
		catalog = NewCatalog()
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	retries := opts.CommandConflictRetries
	if retries == 0 {
		retries = 5
	}
	return &Bus{
		dispatcher:               dispatcher,
		commands:                 commands,
		events:                   events,
		catalog:                  catalog,
		queue:                    opts.Queue,
		logger:                   logger,
		cursorStore:              opts.CursorStore,
		cursorBootPolicy:         opts.CursorBootPolicy,
		onSubscriptionError:      opts.OnSubscriptionError,
		commandConflictRetries:   retries,
		excludeCommandDebugNames: opts.ExcludeCommandDebugNames,
		orderedGroups:            make(map[orderedKey]*eventGroup),
		queueGroups:              make(map[queueKey]*eventGroup),
		done:                     make(chan error, 1),
	}
}

func (b *Bus) Start(ctx context.Context) error {
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
			_ = b.Stop(context.Background())
			return err
		}
	}
	for range ordered {
		select {
		case <-caughtUp:
		case <-runCtx.Done():
			_ = b.Stop(context.Background())
			return runCtx.Err()
		}
	}
	for _, reg := range commands {
		if err := b.startCommand(runCtx, reg); err != nil {
			_ = b.Stop(context.Background())
			return err
		}
	}
	for _, reg := range queries {
		if err := b.startQuery(runCtx, reg); err != nil {
			_ = b.Stop(context.Background())
			return err
		}
	}
	for _, group := range queues {
		if err := b.startQueueGroup(runCtx, group); err != nil {
			_ = b.Stop(context.Background())
			return err
		}
	}
	return nil
}

func (b *Bus) Stop(ctx context.Context) error {
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
		if err := sub.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	b.signalDone(firstErr)
	return firstErr
}

func (b *Bus) Done() <-chan error {
	return b.done
}

func (b *Bus) Catalog() *Catalog {
	return b.catalog
}

func (b *Bus) DispatchCommand(ctx context.Context, cmd Command) error {
	if b.dispatcher == nil {
		return ErrNoResponders
	}
	addr := CommandAddress(cmd)
	if !b.excludeCommandDebugNames[cmd.CommandName()] {
		b.logger.DebugContext(ctx, "dispatch command", "subject", addr.Subject())
	}
	return b.dispatcher.DispatchCommand(ctx, cmd)
}

func (b *Bus) DispatchQuery(ctx context.Context, query Command, result any) error {
	if b.dispatcher == nil {
		return ErrNoResponders
	}
	addr := QueryAddress(query)
	b.logger.DebugContext(ctx, "dispatch query", "subject", addr.Subject())
	return b.dispatcher.DispatchQuery(ctx, query, result)
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

func EventHandlerOf[T Event](b *Bus, handler func(context.Context, T) error, opts ...HandlerOption) (Subscription, error) {
	prototype, err := newTypedValue[T]()
	if err != nil {
		return nil, err
	}
	if err := b.catalog.RegisterEvent(prototype); err != nil {
		return nil, err
	}
	options := collectHandlerOptions(opts)
	wrapped := EventHandlerFunc(func(ctx context.Context, evt Event, cursor []byte) error {
		typed, ok := evt.(T)
		if !ok {
			return fmt.Errorf("event type mismatch: expected %s, got %T", reflect.TypeOf(prototype), evt)
		}
		return handler(ctx, typed)
	})
	return b.addOrderedHandler(prototype.AggregateScope(), prototype.AggregateType(), prototype.EventName(), options.aggregateIDs, wrapped)
}

func QueueEventHandlerOf[T Event](b *Bus, handler func(context.Context, T) error, opts ...HandlerOption) (Subscription, error) {
	prototype, err := newTypedValue[T]()
	if err != nil {
		return nil, err
	}
	if err := b.catalog.RegisterEvent(prototype); err != nil {
		return nil, err
	}
	options := collectHandlerOptions(opts)
	wrapped := EventHandlerFunc(func(ctx context.Context, evt Event, cursor []byte) error {
		typed, ok := evt.(T)
		if !ok {
			return fmt.Errorf("event type mismatch: expected %s, got %T", reflect.TypeOf(prototype), evt)
		}
		return handler(ctx, typed)
	})
	return b.addQueueHandler(prototype.AggregateScope(), prototype.AggregateType(), prototype.EventName(), options.aggregateIDs, wrapped)
}

func CommandHandlerOf[T Command](b *Bus, handler func(context.Context, T) error, opts ...HandlerOption) (Subscription, error) {
	prototype, err := newTypedValue[T]()
	if err != nil {
		return nil, err
	}
	if err := b.catalog.RegisterCommand(prototype); err != nil {
		return nil, err
	}
	options := collectHandlerOptions(opts)
	wrapped := CommandHandlerFunc(func(ctx context.Context, cmd Command) error {
		typed, ok := cmd.(T)
		if !ok {
			return fmt.Errorf("command type mismatch: expected %s, got %T", reflect.TypeOf(prototype), cmd)
		}
		return handler(ctx, typed)
	})
	return b.addCommandHandler(prototype.AggregateScope(), prototype.AggregateType(), prototype.CommandName(), options.aggregateIDs, wrapped)
}

func QueryHandlerOf[T Command, R any](b *Bus, handler func(context.Context, T) (R, error)) (Subscription, error) {
	prototype, err := newTypedValue[T]()
	if err != nil {
		return nil, err
	}
	if err := b.catalog.RegisterCommand(prototype); err != nil {
		return nil, err
	}
	wrapped := QueryHandlerFunc(func(ctx context.Context, query Command) (any, error) {
		typed, ok := query.(T)
		if !ok {
			return nil, fmt.Errorf("query type mismatch: expected %s, got %T", reflect.TypeOf(prototype), query)
		}
		return handler(ctx, typed)
	})
	return b.addQueryHandler(prototype.AggregateScope(), prototype.AggregateType(), prototype.CommandName(), wrapped)
}

type orderedKey struct {
	scope string
	agg   string
}

type queueKey struct {
	scope string
	agg   string
	id    string
	name  string
	queue string
}

type eventGroup struct {
	ordered  bool
	scope    string
	agg      string
	id       string
	name     string
	queue    string
	handlers map[uint64]registeredEventHandler
	sub      Subscription
}

type registeredEventHandler struct {
	id           uint64
	eventName    string
	aggregateIDs []string
	handler      EventHandler
}

type eventRegistration struct {
	b       *Bus
	id      uint64
	ordered bool
	okey    orderedKey
	qkey    queueKey
}

func (r *eventRegistration) Stop(ctx context.Context) error {
	return r.b.removeEventHandler(ctx, r)
}

func (b *Bus) addOrderedHandler(scope, agg, name string, ids []string, handler EventHandler) (Subscription, error) {
	id := b.nextID.Add(1)
	key := orderedKey{scope: scope, agg: agg}
	b.mu.Lock()
	group := b.orderedGroups[key]
	if group == nil {
		group = &eventGroup{ordered: true, scope: scope, agg: agg, handlers: make(map[uint64]registeredEventHandler)}
		b.orderedGroups[key] = group
	}
	group.handlers[id] = registeredEventHandler{id: id, eventName: name, aggregateIDs: ids, handler: handler}
	shouldStart := b.running && group.sub == nil
	ctx := b.ctx
	b.mu.Unlock()
	if shouldStart {
		caughtUp := make(chan struct{}, 1)
		if err := b.startOrderedGroup(ctx, group, caughtUp); err != nil {
			_ = b.removeEventHandler(context.Background(), &eventRegistration{b: b, id: id, ordered: true, okey: key})
			return nil, err
		}
	}
	return &eventRegistration{b: b, id: id, ordered: true, okey: key}, nil
}

func (b *Bus) addQueueHandler(scope, agg, name string, ids []string, handler EventHandler) (Subscription, error) {
	if b.queue == "" {
		return nil, fmt.Errorf("queue event handler requires BusOptions.Queue")
	}
	if len(ids) == 0 {
		ids = []string{""}
	}
	var regs []Subscription
	for _, idFilter := range ids {
		reg, err := b.addOneQueueHandler(scope, agg, idFilter, name, handler)
		if err != nil {
			for _, r := range regs {
				_ = r.Stop(context.Background())
			}
			return nil, err
		}
		regs = append(regs, reg)
	}
	return multiRegistration(regs), nil
}

func (b *Bus) addOneQueueHandler(scope, agg, idFilter, name string, handler EventHandler) (Subscription, error) {
	id := b.nextID.Add(1)
	key := queueKey{scope: scope, agg: agg, id: idFilter, name: name, queue: b.queue}
	b.mu.Lock()
	group := b.queueGroups[key]
	if group == nil {
		group = &eventGroup{scope: scope, agg: agg, id: idFilter, name: name, queue: b.queue, handlers: make(map[uint64]registeredEventHandler)}
		b.queueGroups[key] = group
	}
	group.handlers[id] = registeredEventHandler{id: id, eventName: name, handler: handler}
	shouldStart := b.running && group.sub == nil
	ctx := b.ctx
	b.mu.Unlock()
	if shouldStart {
		if err := b.startQueueGroup(ctx, group); err != nil {
			_ = b.removeEventHandler(context.Background(), &eventRegistration{b: b, id: id, qkey: key})
			return nil, err
		}
	}
	return &eventRegistration{b: b, id: id, qkey: key}, nil
}

func (b *Bus) startOrderedGroup(ctx context.Context, group *eventGroup, caughtUp chan<- struct{}) error {
	if b.events == nil {
		return ErrNoResponders
	}
	stream := group.scope + "." + group.agg
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
	sub, err := b.events.SubscribeEvents(ctx, handler, EventSubscriptionConfig{
		AggregateScope:   group.scope,
		AggregateTypes:   []string{group.agg},
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
		return sub.Stop(context.Background())
	}
	group.sub = sub
	b.mu.Unlock()
	return nil
}

func (b *Bus) startQueueGroup(ctx context.Context, group *eventGroup) error {
	if b.events == nil {
		return ErrNoResponders
	}
	ids := []string(nil)
	if group.id != "" {
		ids = []string{group.id}
	}
	consumerName := strings.Join([]string{group.queue, group.scope, group.agg, wildcardName(group.id), group.name}, "_")
	sub, err := b.events.SubscribeEvents(ctx, EventHandlerFunc(func(ctx context.Context, evt Event, cursor []byte) error {
		return b.dispatchEventGroup(ctx, group, evt, cursor)
	}), EventSubscriptionConfig{
		AggregateScope: group.scope,
		AggregateTypes: []string{group.agg},
		AggregateIDs:   ids,
		EventNames:     []string{group.name},
		Queue:          group.queue,
		ConsumerName:   consumerName,
		OnError:        b.makeOnError(true),
	})
	if err != nil {
		return fmt.Errorf("start queue event subscription %s: %w", consumerName, err)
	}
	b.mu.Lock()
	if !b.running || len(group.handlers) == 0 || group.sub != nil {
		b.mu.Unlock()
		return sub.Stop(context.Background())
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
	for _, h := range handlers {
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

func (b *Bus) removeEventHandler(ctx context.Context, reg *eventRegistration) error {
	var sub Subscription
	b.mu.Lock()
	if reg.ordered {
		group := b.orderedGroups[reg.okey]
		if group != nil {
			delete(group.handlers, reg.id)
			if len(group.handlers) == 0 {
				sub = group.sub
				delete(b.orderedGroups, reg.okey)
			}
		}
	} else {
		group := b.queueGroups[reg.qkey]
		if group != nil {
			delete(group.handlers, reg.id)
			if len(group.handlers) == 0 {
				sub = group.sub
				delete(b.queueGroups, reg.qkey)
			}
		}
	}
	b.mu.Unlock()
	if sub != nil {
		return sub.Stop(ctx)
	}
	return nil
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

func (b *Bus) addCommandHandler(scope, agg, name string, ids []string, handler CommandHandler) (Subscription, error) {
	reg := &commandRegistration{
		cfg: CommandSubscriptionConfig{
			AggregateScope: scope,
			AggregateTypes: []string{agg},
			AggregateIDs:   ids,
			CommandNames:   []string{name},
			Queue:          b.queue,
		},
		handler: b.withCommandConflictRetry(b.withCommandLogging(handler)),
	}
	b.mu.Lock()
	b.commandsRegs = append(b.commandsRegs, reg)
	shouldStart := b.running
	ctx := b.ctx
	b.mu.Unlock()
	if shouldStart {
		if err := b.startCommand(ctx, reg); err != nil {
			return nil, err
		}
	}
	return SubscriptionFunc(func(ctx context.Context) error {
		b.mu.Lock()
		if reg.sub == nil {
			b.mu.Unlock()
			return nil
		}
		sub := reg.sub
		reg.sub = nil
		b.mu.Unlock()
		return sub.Stop(ctx)
	}), nil
}

func (b *Bus) addQueryHandler(scope, agg, name string, handler QueryHandler) (Subscription, error) {
	reg := &queryRegistration{
		cfg: CommandSubscriptionConfig{
			AggregateScope: scope,
			AggregateTypes: []string{agg},
			CommandNames:   []string{name},
			Queue:          b.queue,
		},
		handler: b.withQueryLogging(handler),
	}
	b.mu.Lock()
	b.queryRegs = append(b.queryRegs, reg)
	shouldStart := b.running
	ctx := b.ctx
	b.mu.Unlock()
	if shouldStart {
		if err := b.startQuery(ctx, reg); err != nil {
			return nil, err
		}
	}
	return SubscriptionFunc(func(ctx context.Context) error {
		b.mu.Lock()
		if reg.sub == nil {
			b.mu.Unlock()
			return nil
		}
		sub := reg.sub
		reg.sub = nil
		b.mu.Unlock()
		return sub.Stop(ctx)
	}), nil
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
			b.logger.DebugContext(ctx, "handle command", "subject", CommandAddress(cmd).Subject())
		}
		err := next.HandleCommand(ctx, cmd)
		if err != nil {
			b.logger.ErrorContext(ctx, "command failed", "subject", CommandAddress(cmd).Subject(), "err", err)
		}
		return err
	})
}

func (b *Bus) withQueryLogging(next QueryHandler) QueryHandler {
	return QueryHandlerFunc(func(ctx context.Context, query Command) (any, error) {
		b.logger.DebugContext(ctx, "handle query", "subject", QueryAddress(query).Subject())
		result, err := next.HandleQuery(ctx, query)
		if err != nil {
			b.logger.ErrorContext(ctx, "query failed", "subject", QueryAddress(query).Subject(), "err", err)
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
		go func() { _ = b.Stop(context.Background()) }()
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

type multiRegistration []Subscription

func (m multiRegistration) Stop(ctx context.Context) error {
	var firstErr error
	for _, sub := range m {
		if err := sub.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func wildcardName(v string) string {
	if v == "" {
		return "all"
	}
	return v
}
