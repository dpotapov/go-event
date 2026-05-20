package event

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type accountCreated struct {
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
}

func (e *accountCreated) AggregateScope() string { return "billing" }
func (e *accountCreated) AggregateType() string  { return "account" }
func (e *accountCreated) AggregateID() string    { return e.AccountID }
func (e *accountCreated) EventName() string      { return "created" }

type logEmitted struct {
	WorkerID string `json:"worker_id"`
	Level    string `json:"level"`
}

func (e *logEmitted) AggregateScope() string { return "ops" }
func (e *logEmitted) MessageKind() string    { return "log" }
func (e *logEmitted) AggregateType() string  { return "worker" }
func (e *logEmitted) AggregateID() string    { return e.WorkerID }
func (e *logEmitted) EventName() string      { return e.Level }

type renameAccount struct {
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
}

func (c *renameAccount) AggregateScope() string { return "billing" }
func (c *renameAccount) AggregateType() string  { return "account" }
func (c *renameAccount) AggregateID() string    { return c.AccountID }
func (c *renameAccount) CommandName() string    { return "rename" }

type getAccount struct {
	AccountID string `json:"account_id"`
}

func (q *getAccount) AggregateScope() string { return "billing" }
func (q *getAccount) AggregateType() string  { return "account" }
func (q *getAccount) AggregateID() string    { return q.AccountID }
func (q *getAccount) CommandName() string    { return "get" }
func (q *getAccount) ResultType() accountDTO { return accountDTO{} }

type pingWorker struct {
	WorkerID string `json:"worker_id"`
}

func (c *pingWorker) AggregateScope() string { return "ops" }
func (c *pingWorker) MessageKind() string    { return "signal" }
func (c *pingWorker) AggregateType() string  { return "worker" }
func (c *pingWorker) AggregateID() string    { return c.WorkerID }
func (c *pingWorker) CommandName() string    { return "ping" }

type accountDTO struct {
	ID string `json:"id"`
}

type accountAggregate struct {
	ID   string
	Name string
}

func (a *accountAggregate) AggregateScope() string { return "billing" }
func (a *accountAggregate) AggregateType() string  { return "account" }
func (a *accountAggregate) AggregateID() string    { return a.ID }
func (a *accountAggregate) EventTypes() ([]Event, SnapshotEvent) {
	return []Event{&accountCreated{}}, nil
}
func (a *accountAggregate) Apply(evt Event) {
	switch e := evt.(type) {
	case *accountCreated:
		a.ID = e.AccountID
		a.Name = e.Name
	}
}

func TestSubjectValidate(t *testing.T) {
	subj := Subject{
		Scope:         "billing",
		Kind:          KindEvent,
		AggregateType: "account",
		AggregateID:   "acct-1",
		Name:          "created",
	}
	require.NoError(t, subj.Validate())
	require.Equal(t, "billing.event.account.acct-1.created", subj.String())

	subj.AggregateID = "bad.id"
	require.Error(t, subj.Validate())
}

func TestSubjectAllowsCustomMessageKind(t *testing.T) {
	subj := Subject{
		Scope:         "billing",
		Kind:          MessageKind("signal"),
		AggregateType: "account",
		AggregateID:   "acct-1",
		Name:          "refresh",
	}
	require.NoError(t, subj.Validate())
}

func TestSubjectUsesMessageKindOverride(t *testing.T) {
	evt := &logEmitted{WorkerID: "worker-1", Level: "info"}
	require.Equal(t, Subject{
		Scope:         "ops",
		Kind:          MessageKind("log"),
		AggregateType: "worker",
		AggregateID:   "worker-1",
		Name:          "info",
	}, EventSubject(evt))

	cmd := &pingWorker{WorkerID: "worker-1"}
	require.Equal(t, Subject{
		Scope:         "ops",
		Kind:          MessageKind("signal"),
		AggregateType: "worker",
		AggregateID:   "worker-1",
		Name:          "ping",
	}, CommandSubject(cmd))
	require.Equal(t, Subject{
		Scope:         "ops",
		Kind:          MessageKind("signal"),
		AggregateType: "worker",
		AggregateID:   "worker-1",
		Name:          "ping",
	}, QuerySubject(cmd))
}

func TestSubjectLogsAsString(t *testing.T) {
	var out strings.Builder
	logger := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger.DebugContext(t.Context(), "dispatch command", "subject", Subject{
		Scope:         "billing",
		Kind:          KindCommand,
		AggregateType: "account",
		AggregateID:   "acct-1",
		Name:          "rename",
	})

	require.Contains(t, out.String(), "subject=billing.command.account.acct-1.rename")
}

func TestRegistryRegistersAndDecodesTypes(t *testing.T) {
	registry := newRegistry()
	require.NoError(t, RegisterMessageType[*accountCreated](registry))
	require.NoError(t, RegisterMessageType[*renameAccount](registry))
	require.NoError(t, RegisterMessageType[*getAccount](registry))

	evt, err := registry.DecodeEvent(Subject{
		Scope:         "billing",
		Kind:          KindEvent,
		AggregateType: "account",
		Name:          "created",
	}, []byte(`{"account_id":"acct-1","name":"Acme"}`))
	require.NoError(t, err)
	require.Equal(t, "acct-1", evt.AggregateID())

	cmd, err := registry.DecodeCommand(Subject{
		Scope:         "billing",
		Kind:          KindCommand,
		AggregateType: "account",
		Name:          "rename",
	}, []byte(`{"account_id":"acct-1","name":"New"}`))
	require.NoError(t, err)
	require.Equal(t, "acct-1", cmd.AggregateID())

	query, err := registry.DecodeCommand(Subject{
		Scope:         "billing",
		Kind:          KindQuery,
		AggregateType: "account",
		Name:          "get",
	}, []byte(`{"account_id":"acct-1"}`))
	require.NoError(t, err)
	require.Equal(t, "acct-1", query.AggregateID())
}

func TestRegistryDecodesCustomEventKind(t *testing.T) {
	registry := newRegistry()
	require.NoError(t, RegisterMessageType[*logEmitted](registry))

	evt, err := registry.DecodeEvent(Subject{
		Scope:         "ops",
		Kind:          MessageKind("log"),
		AggregateType: "worker",
		Name:          "info",
	}, []byte(`{"worker_id":"worker-1","level":"info"}`))
	require.NoError(t, err)
	require.Equal(t, "worker-1", evt.AggregateID())

	_, err = registry.DecodeEvent(Subject{
		Scope:         "ops",
		Kind:          KindEvent,
		AggregateType: "worker",
		Name:          "info",
	}, []byte(`{"worker_id":"worker-1","level":"info"}`))
	require.Error(t, err)
}

func TestRegistryRejectsUnsupportedMessageType(t *testing.T) {
	registry := newRegistry()

	err := RegisterMessageType[*accountDTO](registry)
	require.ErrorContains(t, err, "must implement event.Event or event.Command")
}

type flakyStore struct {
	loads int
	saves int
}

func (s *flakyStore) Load(ctx context.Context, agg ESAggregate) (uint64, error) {
	s.loads++
	return uint64(s.loads - 1), nil
}

func (s *flakyStore) Save(ctx context.Context, agg AggregateRef, expectedVersion uint64, events ...Event) error {
	s.saves++
	if s.saves == 1 {
		return ErrConflict
	}
	return nil
}

func TestExecuteUsesFreshAggregateOnRetry(t *testing.T) {
	store := &flakyStore{}
	seen := make([]*accountAggregate, 0, 2)

	err := Execute(t.Context(), store,
		func() *accountAggregate {
			return &accountAggregate{ID: "acct-1"}
		},
		func(ctx context.Context, agg *accountAggregate, version uint64, attempt uint64) ([]Event, error) {
			seen = append(seen, agg)
			return []Event{&accountCreated{AccountID: "acct-1", Name: "Acme"}}, nil
		},
		WithExecuteMaxAttempts(2),
	)
	require.NoError(t, err)
	require.Len(t, seen, 2)
	require.NotSame(t, seen[0], seen[1])
	require.Equal(t, 2, store.loads)
	require.Equal(t, 2, store.saves)
}

type recordingDispatcher struct {
	command Command
	query   Command
}

func (d *recordingDispatcher) DispatchCommand(ctx context.Context, cmd Command) error {
	d.command = cmd
	return nil
}

func (d *recordingDispatcher) DispatchQuery(ctx context.Context, query Command, result any) error {
	d.query = query
	*(result.(*accountDTO)) = accountDTO{ID: query.AggregateID()}
	return nil
}

type recordingPublisher struct {
	events []Event
	err    error
}

func (p *recordingPublisher) Publish(ctx context.Context, events ...Event) error {
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, events...)
	return nil
}

type recordingEventSubscriber struct {
	subs  []EventSubscriptionConfig
	h     []EventHandler
	order *[]string
}

func (s *recordingEventSubscriber) SubscribeEvents(ctx context.Context, handler EventHandler, cfg EventSubscriptionConfig) (Subscription, error) {
	if s.order != nil {
		if cfg.Queue == "" {
			*s.order = append(*s.order, "ordered")
		} else {
			*s.order = append(*s.order, "queue")
		}
	}
	s.subs = append(s.subs, cfg)
	s.h = append(s.h, handler)
	if cfg.CaughtUp != nil {
		cfg.CaughtUp()
	}
	return SubscriptionFunc(func() error { return nil }), nil
}

type recordingCommandSubscriber struct {
	commandSubs []CommandSubscriptionConfig
	querySubs   []CommandSubscriptionConfig
	commandH    []CommandHandler
	queryH      []QueryHandler
	order       *[]string
}

func (s *recordingCommandSubscriber) SubscribeCommand(ctx context.Context, handler CommandHandler, cfg CommandSubscriptionConfig) (Subscription, error) {
	if s.order != nil {
		*s.order = append(*s.order, "command")
	}
	s.commandSubs = append(s.commandSubs, cfg)
	s.commandH = append(s.commandH, handler)
	return SubscriptionFunc(func() error { return nil }), nil
}

func (s *recordingCommandSubscriber) SubscribeQuery(ctx context.Context, handler QueryHandler, cfg CommandSubscriptionConfig) (Subscription, error) {
	if s.order != nil {
		*s.order = append(*s.order, "query")
	}
	s.querySubs = append(s.querySubs, cfg)
	s.queryH = append(s.queryH, handler)
	return SubscriptionFunc(func() error { return nil }), nil
}

func TestBusRejectsHandlerRegistrationAfterConnect(t *testing.T) {
	dispatcher := &recordingDispatcher{}
	publisher := &recordingPublisher{}
	commands := &recordingCommandSubscriber{}
	events := &recordingEventSubscriber{}
	bus := NewBus(dispatcher, publisher, commands, events, BusOptions{Queue: "svc"})

	require.NoError(t, bus.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Disconnect()) })

	require.Panics(t, func() {
		HandleEvent(bus, func(ctx context.Context, evt *accountCreated) error {
			return nil
		})
	})
}

func TestBusRejectsHandlerRegistrationAfterDisconnect(t *testing.T) {
	bus := NewBus(nil, nil, nil, nil, BusOptions{})

	require.NoError(t, bus.Disconnect())

	require.Panics(t, func() {
		HandleCommand(bus, func(ctx context.Context, cmd *renameAccount) error {
			return nil
		})
	})
}

func TestBusDispatchAndTypedHandlers(t *testing.T) {
	dispatcher := &recordingDispatcher{}
	publisher := &recordingPublisher{}
	commands := &recordingCommandSubscriber{}
	events := &recordingEventSubscriber{}
	bus := NewBus(dispatcher, publisher, commands, events, BusOptions{Queue: "svc"})

	commandCalled := false
	HandleCommand(bus, func(ctx context.Context, cmd *renameAccount) error {
		commandCalled = true
		require.Equal(t, "acct-1", cmd.AccountID)
		return nil
	})
	HandleQuery(bus, func(ctx context.Context, query *getAccount) (accountDTO, error) {
		return accountDTO{ID: query.AccountID}, nil
	})

	require.NoError(t, bus.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Disconnect()) })

	require.NoError(t, commands.commandH[0].HandleCommand(t.Context(), &renameAccount{AccountID: "acct-1"}))
	require.True(t, commandCalled)

	result, err := commands.queryH[0].HandleQuery(t.Context(), &getAccount{AccountID: "acct-2"})
	require.NoError(t, err)
	require.Equal(t, accountDTO{ID: "acct-2"}, result)

	require.NoError(t, bus.DispatchCommand(t.Context(), &renameAccount{AccountID: "acct-3"}))
	require.Equal(t, "acct-3", dispatcher.command.AggregateID())

	dto, err := Ask[accountDTO](t.Context(), bus, &getAccount{AccountID: "acct-4"})
	require.NoError(t, err)
	require.Equal(t, "acct-4", dto.ID)

	require.NoError(t, bus.Publish(t.Context(), &accountCreated{AccountID: "acct-5", Name: "Acme"}))
	require.Len(t, publisher.events, 1)
	require.Equal(t, "acct-5", publisher.events[0].AggregateID())
}

func TestBusPublishReturnsNoRespondersWithoutPublisher(t *testing.T) {
	bus := NewBus(nil, nil, nil, nil, BusOptions{})
	err := bus.Publish(t.Context(), &accountCreated{AccountID: "acct-1", Name: "Acme"})
	require.ErrorIs(t, err, ErrNoResponders)
}

func TestTypedHandlersRegisterDecodeTypesInBusRegistry(t *testing.T) {
	bus := NewBus(nil, nil, &recordingCommandSubscriber{}, &recordingEventSubscriber{}, BusOptions{Queue: "svc"})

	HandleCommand(bus, func(ctx context.Context, cmd *renameAccount) error {
		return nil
	})
	HandleEvent(bus, func(ctx context.Context, evt *accountCreated) error {
		return nil
	})

	cmd, err := bus.registry.NewCommand(KindCommand, "billing", "account", "rename")
	require.NoError(t, err)
	require.IsType(t, &renameAccount{}, cmd)

	evt, err := bus.registry.NewEvent(KindEvent, "billing", "account", "created")
	require.NoError(t, err)
	require.IsType(t, &accountCreated{}, evt)
}

func TestBusTypedHandlersUseCustomMessageKind(t *testing.T) {
	commands := &recordingCommandSubscriber{}
	events := &recordingEventSubscriber{}
	bus := NewBus(nil, nil, commands, events, BusOptions{Queue: "svc"})

	HandleQueueEvent(bus, func(ctx context.Context, evt *logEmitted) error {
		return nil
	})
	HandleCommand(bus, func(ctx context.Context, cmd *pingWorker) error {
		return nil
	})
	HandleQuery(bus, func(ctx context.Context, query *pingWorker) (accountDTO, error) {
		return accountDTO{ID: query.WorkerID}, nil
	})

	require.NoError(t, bus.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Disconnect()) })

	require.Equal(t, MessageKind("log"), events.subs[0].Kind)
	require.Empty(t, events.subs[0].EventNames)
	require.Equal(t, MessageKind("signal"), commands.commandSubs[0].Kind)
	require.Equal(t, MessageKind("signal"), commands.querySubs[0].Kind)
}

func TestBusConnectStartsOrderedHandlersBeforeCommandsAndQueues(t *testing.T) {
	var order []string
	commands := &recordingCommandSubscriber{order: &order}
	events := &recordingEventSubscriber{order: &order}
	bus := NewBus(nil, nil, commands, events, BusOptions{Queue: "svc"})

	HandleEvent(bus, func(ctx context.Context, evt *accountCreated) error {
		return nil
	})
	HandleCommand(bus, func(ctx context.Context, cmd *renameAccount) error {
		return nil
	})
	HandleQuery(bus, func(ctx context.Context, query *getAccount) (accountDTO, error) {
		return accountDTO{ID: query.AccountID}, nil
	})
	HandleQueueEvent(bus, func(ctx context.Context, evt *accountCreated) error {
		return nil
	})

	require.NoError(t, bus.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Disconnect()) })

	require.Equal(t, []string{"ordered", "command", "query", "queue"}, order)
}

func TestBusStopsOnSubscriptionErrorWhenConfigured(t *testing.T) {
	events := &recordingEventSubscriber{}
	bus := NewBus(nil, nil, nil, events, BusOptions{
		OnSubscriptionError: func(err *SubscriptionError) bool { return false },
	})
	HandleEvent(bus, func(ctx context.Context, evt *accountCreated) error {
		return errors.New("boom")
	})
	require.NoError(t, bus.Connect(t.Context()))

	require.False(t, events.subs[0].OnError(&SubscriptionError{Err: errors.New("boom")}))
	err := <-bus.Done()
	require.ErrorContains(t, err, "boom")
}
