package nats_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	goevent "github.com/dpotapov/go-event"
	goeventnats "github.com/dpotapov/go-event/nats"
	"github.com/dpotapov/go-event/testkit/natstest"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type account struct {
	ID   string
	Name string
}

func (a *account) AggregateScope() string { return "test" }
func (a *account) AggregateType() string  { return "account" }
func (a *account) AggregateID() string    { return a.ID }
func (a *account) Apply(evt goevent.Event) error {
	switch e := evt.(type) {
	case *accountCreated:
		a.Name = e.Name
	case *accountRenamed:
		a.Name = e.Name
	}
	return nil
}

type accountCreated struct {
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
}

func (e *accountCreated) AggregateScope() string { return "test" }
func (e *accountCreated) AggregateType() string  { return "account" }
func (e *accountCreated) AggregateID() string    { return e.AccountID }
func (e *accountCreated) EventName() string      { return "created" }

type accountRenamed struct {
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
}

func (e *accountRenamed) AggregateScope() string { return "test" }
func (e *accountRenamed) AggregateType() string  { return "account" }
func (e *accountRenamed) AggregateID() string    { return e.AccountID }
func (e *accountRenamed) EventName() string      { return "renamed" }

type renameAccount struct {
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
}

func (c *renameAccount) AggregateScope() string { return "test" }
func (c *renameAccount) AggregateType() string  { return "account" }
func (c *renameAccount) AggregateID() string    { return c.AccountID }
func (c *renameAccount) CommandName() string    { return "rename" }

type getAccount struct {
	AccountID string `json:"account_id"`
}

func (q *getAccount) AggregateScope() string { return "test" }
func (q *getAccount) AggregateType() string  { return "account" }
func (q *getAccount) AggregateID() string    { return q.AccountID }
func (q *getAccount) CommandName() string    { return "get" }
func (q *getAccount) ResultType() accountView {
	return accountView{}
}

type accountView struct {
	ID string `json:"id"`
}

func testCatalog(t testing.TB) *goevent.Catalog {
	t.Helper()
	catalog := goevent.NewCatalog()
	require.NoError(t, goevent.RegisterEventType[*accountCreated](catalog))
	require.NoError(t, goevent.RegisterEventType[*accountRenamed](catalog))
	require.NoError(t, goevent.RegisterCommandType[*renameAccount](catalog))
	require.NoError(t, goevent.RegisterCommandType[*getAccount](catalog))
	return catalog
}

func TestEventStoreCASUsesAggregateSubjectFilter(t *testing.T) {
	env := natstest.Run(t)
	catalog := testCatalog(t)
	env.EnsureEventStream(t, "test", "account", goeventnats.WithMemoryStorage())

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{Catalog: catalog})
	require.NoError(t, err)

	agg := &account{ID: "acct-1"}
	require.NoError(t, store.Save(t.Context(), agg, 0, &accountCreated{AccountID: "acct-1", Name: "Acme"}))

	loaded := &account{ID: "acct-1"}
	version, err := store.Load(t.Context(), loaded)
	require.NoError(t, err)
	require.NotZero(t, version)
	require.Equal(t, "Acme", loaded.Name)

	err = store.Save(t.Context(), agg, 0, &accountRenamed{AccountID: "acct-1", Name: "Stale"})
	require.Error(t, err)
	require.True(t, errors.Is(err, goevent.ErrConflict), "expected conflict, got %v", err)

	require.NoError(t, store.Save(t.Context(), agg, version, &accountRenamed{AccountID: "acct-1", Name: "Fresh"}))
	loaded = &account{ID: "acct-1"}
	version, err = store.Load(t.Context(), loaded)
	require.NoError(t, err)
	require.NotZero(t, version)
	require.Equal(t, "Fresh", loaded.Name)
}

func TestEventStoreSavesMultipleEventsAtomically(t *testing.T) {
	env := natstest.Run(t)
	catalog := testCatalog(t)
	env.EnsureEventStream(t, "test", "account", goeventnats.WithMemoryStorage())

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{Catalog: catalog})
	require.NoError(t, err)

	agg := &account{ID: "acct-1"}
	require.NoError(t, store.Save(t.Context(), agg, 0,
		&accountCreated{AccountID: "acct-1", Name: "Acme"},
		&accountRenamed{AccountID: "acct-1", Name: "Fresh"},
	))

	loaded := &account{ID: "acct-1"}
	version, err := store.Load(t.Context(), loaded)
	require.NoError(t, err)
	require.NotZero(t, version)
	require.Equal(t, "Fresh", loaded.Name)
}

func TestNATSBusEndToEnd(t *testing.T) {
	env := natstest.Run(t)
	catalog := testCatalog(t)
	env.EnsureEventStream(t, "test", "account", goeventnats.WithMemoryStorage())

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{Catalog: catalog})
	require.NoError(t, err)
	dispatcher, err := goeventnats.NewDispatcher(env.Conn, goeventnats.DispatcherConfig{RequestTimeout: 2 * time.Second})
	require.NoError(t, err)
	commandSub, err := goeventnats.NewCommandSubscriber(env.Conn, goeventnats.CommandSubscriberConfig{Catalog: catalog, Queue: "svc"})
	require.NoError(t, err)
	eventSub, err := goeventnats.NewSubscriber(env.Conn, goeventnats.SubscriberConfig{Catalog: catalog})
	require.NoError(t, err)

	bus := goevent.NewBus(dispatcher, commandSub, eventSub, goevent.BusOptions{Catalog: catalog, Queue: "svc"})

	commandSeen := make(chan string, 1)
	_, err = goevent.CommandHandlerOf(bus, func(ctx context.Context, cmd *renameAccount) error {
		commandSeen <- cmd.Name
		return nil
	})
	require.NoError(t, err)

	_, err = goevent.QueryHandlerOf(bus, func(ctx context.Context, query *getAccount) (accountView, error) {
		return accountView{ID: query.AccountID}, nil
	})
	require.NoError(t, err)

	orderedSeen := make(chan string, 1)
	_, err = goevent.EventHandlerOf(bus, func(ctx context.Context, evt *accountCreated) error {
		orderedSeen <- evt.AccountID
		return nil
	})
	require.NoError(t, err)

	queueSeen := make(chan string, 1)
	_, err = goevent.QueueEventHandlerOf(bus, func(ctx context.Context, evt *accountCreated) error {
		queueSeen <- evt.AccountID
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, bus.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Stop(context.Background())) })

	require.NoError(t, bus.DispatchCommand(t.Context(), &renameAccount{AccountID: "acct-1", Name: "Fresh"}))
	require.Equal(t, "Fresh", receive(t, commandSeen))

	view, err := goevent.Ask[accountView](t.Context(), bus, &getAccount{AccountID: "acct-1"})
	require.NoError(t, err)
	require.Equal(t, "acct-1", view.ID)

	require.NoError(t, store.Save(t.Context(), &account{ID: "acct-1"}, 0, &accountCreated{AccountID: "acct-1", Name: "Acme"}))
	require.Equal(t, "acct-1", receive(t, orderedSeen))
	require.Equal(t, "acct-1", receive(t, queueSeen))
}

func TestQueueSubscriptionCreatesConsumerPerFilter(t *testing.T) {
	env := natstest.Run(t)
	catalog := testCatalog(t)
	stream := env.EnsureEventStream(t, "test", "account", goeventnats.WithMemoryStorage())
	info, err := stream.Info(t.Context())
	require.NoError(t, err)

	subscriber, err := goeventnats.NewSubscriber(env.Conn, goeventnats.SubscriberConfig{Catalog: catalog})
	require.NoError(t, err)
	sub, err := subscriber.SubscribeEvents(t.Context(), goevent.EventHandlerFunc(func(ctx context.Context, evt goevent.Event, cursor []byte) error {
		return nil
	}), goevent.EventSubscriptionConfig{
		AggregateScope: "test",
		AggregateTypes: []string{"account"},
		EventNames:     []string{"created", "renamed"},
		Queue:          "projection",
		ConsumerName:   "projection_account",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sub.Stop(context.Background())) })

	var names []string
	require.Eventually(t, func() bool {
		names = env.ConsumerNames(t, info.Config.Name)
		return len(names) == 2
	}, 5*time.Second, 50*time.Millisecond)

	for _, name := range names {
		consumer, err := env.JetStream.Consumer(t.Context(), info.Config.Name, name)
		require.NoError(t, err)
		cfg := consumer.CachedInfo().Config
		require.NotEmpty(t, cfg.FilterSubject)
		require.Empty(t, cfg.FilterSubjects)
		require.True(t,
			slices.Contains([]string{"test.event.account.*.created", "test.event.account.*.renamed"}, cfg.FilterSubject),
			"unexpected filter subject %q", cfg.FilterSubject,
		)
	}
}

func BenchmarkRawJetStreamSubscription(b *testing.B) {
	env := natstest.Run(b)
	env.EnsureEventStream(b, "test", "account", goeventnats.WithMemoryStorage())
	var count atomic.Uint64
	consumer, err := env.JetStream.CreateOrUpdateConsumer(b.Context(), goeventnats.StreamName(goeventnats.DefaultStreamPattern, "test", "account"), jetstream.ConsumerConfig{
		Name:          "raw",
		Durable:       "raw",
		FilterSubject: "test.event.account.*.created",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	require.NoError(b, err)
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		_, _ = msg.Metadata()
		var evt accountCreated
		_ = json.Unmarshal(msg.Data(), &evt)
		count.Add(1)
		_ = msg.Ack()
	})
	require.NoError(b, err)
	defer cc.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := env.JetStream.Publish(b.Context(), "test.event.account.acct.created", []byte(`{"account_id":"acct","name":"Acme"}`))
		require.NoError(b, err)
	}
	require.Eventually(b, func() bool { return count.Load() == uint64(b.N) }, 10*time.Second, 10*time.Millisecond)
}

func BenchmarkMessageBusSubscription(b *testing.B) {
	env := natstest.Run(b)
	catalog := testCatalog(b)
	env.EnsureEventStream(b, "test", "account", goeventnats.WithMemoryStorage())
	subscriber, err := goeventnats.NewSubscriber(env.Conn, goeventnats.SubscriberConfig{Catalog: catalog})
	require.NoError(b, err)
	var count atomic.Uint64
	sub, err := subscriber.SubscribeEvents(b.Context(), goevent.EventHandlerFunc(func(ctx context.Context, evt goevent.Event, cursor []byte) error {
		count.Add(1)
		return nil
	}), goevent.EventSubscriptionConfig{
		AggregateScope: "test",
		AggregateTypes: []string{"account"},
		EventNames:     []string{"created"},
		Queue:          "bench",
		ConsumerName:   "bench",
	})
	require.NoError(b, err)
	defer func() { _ = sub.Stop(context.Background()) }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := env.JetStream.Publish(b.Context(), "test.event.account.acct.created", []byte(`{"account_id":"acct","name":"Acme"}`))
		require.NoError(b, err)
	}
	require.Eventually(b, func() bool { return count.Load() == uint64(b.N) }, 10*time.Second, 10*time.Millisecond)
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for value")
		var zero T
		return zero
	}
}
