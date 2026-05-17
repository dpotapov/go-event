package event

import (
	"context"
	"errors"
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
func (a *accountAggregate) Apply(evt Event) error {
	switch e := evt.(type) {
	case *accountCreated:
		a.ID = e.AccountID
		a.Name = e.Name
	}
	return nil
}

func TestAddressSubjectRoundTrip(t *testing.T) {
	addr := Address{
		Scope:         "billing",
		Class:         ClassEvent,
		AggregateType: "account",
		AggregateID:   "acct-1",
		Name:          "created",
	}
	require.NoError(t, addr.Validate())
	require.Equal(t, "billing.event.account.acct-1.created", addr.Subject())

	got, ok := ParseSubject(addr.Subject())
	require.True(t, ok)
	require.Equal(t, addr, got)

	_, ok = ParseSubject("billing.event.account.bad.id.created")
	require.False(t, ok)
}

func TestCatalogRegistersAndDecodesTypes(t *testing.T) {
	catalog := NewCatalog()
	require.NoError(t, RegisterEventType[*accountCreated](catalog))
	require.NoError(t, RegisterCommandType[*renameAccount](catalog))

	evt, err := catalog.DecodeEvent(Address{
		Scope:         "billing",
		Class:         ClassEvent,
		AggregateType: "account",
		Name:          "created",
	}, []byte(`{"account_id":"acct-1","name":"Acme"}`))
	require.NoError(t, err)
	require.Equal(t, "acct-1", evt.AggregateID())

	cmd, err := catalog.DecodeCommand(Address{
		Scope:         "billing",
		Class:         ClassCommand,
		AggregateType: "account",
		Name:          "rename",
	}, []byte(`{"account_id":"acct-1","name":"New"}`))
	require.NoError(t, err)
	require.Equal(t, "acct-1", cmd.AggregateID())
}

type flakyStore struct {
	loads int
	saves int
}

func (s *flakyStore) Load(ctx context.Context, aggregate Aggregate) (uint64, error) {
	s.loads++
	return uint64(s.loads - 1), nil
}

func (s *flakyStore) Save(ctx context.Context, aggregate Aggregate, expectedVersion uint64, events ...Event) error {
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

type recordingEventSubscriber struct {
	subs []EventSubscriptionConfig
	h    []EventHandler
}

func (s *recordingEventSubscriber) SubscribeEvents(ctx context.Context, handler EventHandler, cfg EventSubscriptionConfig) (Subscription, error) {
	s.subs = append(s.subs, cfg)
	s.h = append(s.h, handler)
	if cfg.CaughtUp != nil {
		cfg.CaughtUp()
	}
	return SubscriptionFunc(func(context.Context) error { return nil }), nil
}

type recordingCommandSubscriber struct {
	commandSubs []CommandSubscriptionConfig
	querySubs   []CommandSubscriptionConfig
	commandH    []CommandHandler
	queryH      []QueryHandler
}

func (s *recordingCommandSubscriber) SubscribeCommand(ctx context.Context, handler CommandHandler, cfg CommandSubscriptionConfig) (Subscription, error) {
	s.commandSubs = append(s.commandSubs, cfg)
	s.commandH = append(s.commandH, handler)
	return SubscriptionFunc(func(context.Context) error { return nil }), nil
}

func (s *recordingCommandSubscriber) SubscribeQuery(ctx context.Context, handler QueryHandler, cfg CommandSubscriptionConfig) (Subscription, error) {
	s.querySubs = append(s.querySubs, cfg)
	s.queryH = append(s.queryH, handler)
	return SubscriptionFunc(func(context.Context) error { return nil }), nil
}

func TestBusSupportsDynamicHandlerRegistration(t *testing.T) {
	dispatcher := &recordingDispatcher{}
	commands := &recordingCommandSubscriber{}
	events := &recordingEventSubscriber{}
	bus := NewBus(dispatcher, commands, events, BusOptions{Queue: "svc"})

	require.NoError(t, bus.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Stop(context.Background())) })

	received := make(chan string, 1)
	reg, err := EventHandlerOf(bus, func(ctx context.Context, evt *accountCreated) error {
		received <- evt.AccountID
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Len(t, events.subs, 1)
	require.Equal(t, []string{"account"}, events.subs[0].AggregateTypes)

	err = events.h[0].HandleEvent(t.Context(), &accountCreated{AccountID: "acct-1"}, []byte("cursor"))
	require.NoError(t, err)
	require.Equal(t, "acct-1", <-received)
}

func TestBusDispatchAndTypedHandlers(t *testing.T) {
	dispatcher := &recordingDispatcher{}
	commands := &recordingCommandSubscriber{}
	events := &recordingEventSubscriber{}
	bus := NewBus(dispatcher, commands, events, BusOptions{Queue: "svc"})

	commandCalled := false
	_, err := CommandHandlerOf(bus, func(ctx context.Context, cmd *renameAccount) error {
		commandCalled = true
		require.Equal(t, "acct-1", cmd.AccountID)
		return nil
	})
	require.NoError(t, err)
	_, err = QueryHandlerOf(bus, func(ctx context.Context, query *getAccount) (accountDTO, error) {
		return accountDTO{ID: query.AccountID}, nil
	})
	require.NoError(t, err)

	require.NoError(t, bus.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Stop(context.Background())) })

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
}

func TestBusStopsOnSubscriptionErrorWhenConfigured(t *testing.T) {
	events := &recordingEventSubscriber{}
	bus := NewBus(nil, nil, events, BusOptions{
		OnSubscriptionError: func(err *SubscriptionError) bool { return false },
	})
	_, err := EventHandlerOf(bus, func(ctx context.Context, evt *accountCreated) error {
		return errors.New("boom")
	})
	require.NoError(t, err)
	require.NoError(t, bus.Start(t.Context()))

	require.False(t, events.subs[0].OnError(&SubscriptionError{Err: errors.New("boom")}))
	err = <-bus.Done()
	require.ErrorContains(t, err, "boom")
}
