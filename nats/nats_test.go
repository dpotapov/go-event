package nats_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dpotapov/go-event"
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
func (a *account) EventTypes() ([]event.Event, event.SnapshotEvent) {
	return []event.Event{&accountCreated{}, &accountRenamed{}}, nil
}
func (a *account) Apply(evt event.Event) {
	switch e := evt.(type) {
	case *accountCreated:
		a.Name = e.Name
	case *accountRenamed:
		a.Name = e.Name
	}
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

type accountSnapshot struct {
	event.SnapshotEventBase

	AccountID string `json:"account_id"`
	Name      string `json:"name"`
}

func (e *accountSnapshot) AggregateScope() string { return "test" }
func (e *accountSnapshot) AggregateType() string  { return "account" }
func (e *accountSnapshot) AggregateID() string    { return e.AccountID }

type snapshottedAccount struct {
	ID   string
	Name string
}

func (a *snapshottedAccount) AggregateScope() string { return "test" }
func (a *snapshottedAccount) AggregateType() string  { return "account" }
func (a *snapshottedAccount) AggregateID() string    { return a.ID }
func (a *snapshottedAccount) EventTypes() ([]event.Event, event.SnapshotEvent) {
	return []event.Event{&accountCreated{}, &accountRenamed{}}, &accountSnapshot{}
}
func (a *snapshottedAccount) Apply(evt event.Event) {
	switch e := evt.(type) {
	case *accountCreated:
		a.Name = e.Name
	case *accountRenamed:
		a.Name = e.Name
	case *accountSnapshot:
		a.ID = e.AccountID
		a.Name = e.Name
	}
}
func (a *snapshottedAccount) TakeSnapshot() (event.SnapshotEvent, error) {
	return &accountSnapshot{AccountID: a.ID, Name: a.Name}, nil
}

type logEmitted struct {
	WorkerID string `json:"worker_id"`
	Level    string `json:"level"`
	Message  string `json:"message"`
}

func (e *logEmitted) AggregateScope() string { return "test" }
func (e *logEmitted) MessageKind() string    { return "log" }
func (e *logEmitted) AggregateType() string  { return "worker" }
func (e *logEmitted) AggregateID() string    { return e.WorkerID }
func (e *logEmitted) EventName() string      { return e.Level }

type workerLog struct {
	ID      string
	Message string
}

func (w *workerLog) AggregateScope() string { return "test" }
func (w *workerLog) AggregateType() string  { return "worker" }
func (w *workerLog) AggregateID() string    { return w.ID }
func (w *workerLog) EventTypes() ([]event.Event, event.SnapshotEvent) {
	return []event.Event{&logEmitted{}}, nil
}
func (w *workerLog) Apply(evt event.Event) {
	if e, ok := evt.(*logEmitted); ok {
		w.Message = e.Message
	}
}

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

type accountWithoutEventTypes struct {
	account
}

func (a *accountWithoutEventTypes) EventTypes() ([]event.Event, event.SnapshotEvent) {
	return nil, nil
}

func registerAccountEvents(t testing.TB, registry event.TypeRegistry) {
	t.Helper()
	require.NoError(t, event.RegisterMessageType[*accountCreated](registry))
	require.NoError(t, event.RegisterMessageType[*accountRenamed](registry))
}

func TestSubjectRoundTrip(t *testing.T) {
	subj := event.Subject{
		Scope:         "billing",
		Kind:          event.MessageKind("signal"),
		AggregateType: "account",
		AggregateID:   "acct-1",
		Name:          "refresh",
	}
	require.Equal(t, "billing.signal.account.acct-1.refresh", subj.String())

	parsed, ok := goeventnats.ParseSubject(subj.String())
	require.True(t, ok)
	require.Equal(t, subj, parsed)

	_, ok = goeventnats.ParseSubject("billing.event.account.bad.id.created")
	require.False(t, ok)
}

func TestEventStoreCASUsesAggregateSubjectFilter(t *testing.T) {
	env := natstest.Run(t)
	env.CreateEventStream(t, "test", "account")

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{})
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
	require.True(t, errors.Is(err, event.ErrConflict), "expected conflict, got %v", err)

	require.NoError(t, store.Save(t.Context(), agg, version, &accountRenamed{AccountID: "acct-1", Name: "Fresh"}))
	loaded = &account{ID: "acct-1"}
	version, err = store.Load(t.Context(), loaded)
	require.NoError(t, err)
	require.NotZero(t, version)
	require.Equal(t, "Fresh", loaded.Name)
}

func TestEventStoreSavesMultipleEventsAtomically(t *testing.T) {
	env := natstest.Run(t)
	env.CreateEventStream(t, "test", "account")

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{})
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

func TestAggregateMessageFilterUsesEventKind(t *testing.T) {
	require.Equal(t, "test.event.worker.worker-1.>", goeventnats.AggregateMessageFilter("test", "worker", "worker-1"))
}

func TestEventStoreImplicitSnapshotLoadReplaysTail(t *testing.T) {
	env := natstest.Run(t)
	env.CreateEventStream(t, "test", "account")

	snapshotStore, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{SnapshotEvery: 1})
	require.NoError(t, err)

	agg := &account{ID: "acct-1"}
	require.NoError(t, snapshotStore.Save(t.Context(), agg, 0, &accountCreated{AccountID: "acct-1", Name: "Acme"}))

	stream, err := env.JetStream.Stream(t.Context(), goeventnats.StreamName(goeventnats.DefaultStreamPattern, "test", "account"))
	require.NoError(t, err)
	snap, err := stream.GetLastMsgForSubject(t.Context(), "test.event.account.acct-1.snapshot")
	require.NoError(t, err)
	require.Equal(t, uint64(2), snap.Sequence)

	noSnapshotStore, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{})
	require.NoError(t, err)
	require.NoError(t, noSnapshotStore.Save(t.Context(), agg, snap.Sequence, &accountRenamed{AccountID: "acct-1", Name: "Fresh"}))

	loaded := &account{ID: "acct-1"}
	version, err := snapshotStore.Load(t.Context(), loaded)
	require.NoError(t, err)
	require.Equal(t, uint64(3), version)
	require.Equal(t, "Fresh", loaded.Name)
}

func TestEventStoreExplicitSnapshotLoad(t *testing.T) {
	env := natstest.Run(t)
	env.CreateEventStream(t, "test", "account")

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{SnapshotEvery: 1})
	require.NoError(t, err)

	agg := &snapshottedAccount{ID: "acct-1"}
	require.NoError(t, store.Save(t.Context(), agg, 0, &accountCreated{AccountID: "acct-1", Name: "Acme"}))

	stream, err := env.JetStream.Stream(t.Context(), goeventnats.StreamName(goeventnats.DefaultStreamPattern, "test", "account"))
	require.NoError(t, err)
	snap, err := stream.GetLastMsgForSubject(t.Context(), "test.event.account.acct-1.snapshot")
	require.NoError(t, err)
	require.Equal(t, uint64(2), snap.Sequence)
	var decoded accountSnapshot
	require.NoError(t, json.Unmarshal(snap.Data, &decoded))
	require.Equal(t, "Acme", decoded.Name)

	loaded := &snapshottedAccount{ID: "acct-1"}
	version, err := store.Load(t.Context(), loaded)
	require.NoError(t, err)
	require.Equal(t, snap.Sequence, version)
	require.Equal(t, "Acme", loaded.Name)
}

func TestEventStoreLoadUsesAggregateEventTypes(t *testing.T) {
	env := natstest.Run(t)
	env.CreateEventStream(t, "test", "account")

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{})
	require.NoError(t, err)

	agg := &account{ID: "acct-1"}
	require.NoError(t, store.Save(t.Context(), agg, 0, &accountCreated{AccountID: "acct-1", Name: "Acme"}))

	_, err = store.Load(t.Context(), &accountWithoutEventTypes{account: account{ID: "acct-1"}})
	require.Error(t, err)
	var unknown *event.UnknownEventError
	require.ErrorAs(t, err, &unknown)

	err = event.Execute(t.Context(), store,
		func() *account {
			return &account{ID: "acct-1"}
		},
		func(ctx context.Context, agg *account, version, attempt uint64) ([]event.Event, error) {
			require.Equal(t, "Acme", agg.Name)
			return []event.Event{&accountRenamed{AccountID: agg.ID, Name: "Fresh"}}, nil
		},
	)
	require.NoError(t, err)

	loaded := &account{ID: "acct-1"}
	_, err = store.Load(t.Context(), loaded)
	require.NoError(t, err)
	require.Equal(t, "Fresh", loaded.Name)
}

func TestNATSBusEndToEnd(t *testing.T) {
	env := natstest.Run(t)
	env.CreateEventStream(t, "test", "account")

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{})
	require.NoError(t, err)
	dispatcher, err := goeventnats.NewDispatcher(env.Conn, goeventnats.DispatcherConfig{RequestTimeout: 2 * time.Second})
	require.NoError(t, err)
	publisher, err := goeventnats.NewPublisher(env.Conn, goeventnats.PublisherConfig{})
	require.NoError(t, err)
	commandSub, err := goeventnats.NewCommandSubscriber(env.Conn, goeventnats.CommandSubscriberConfig{Queue: "svc"})
	require.NoError(t, err)
	eventSub, err := goeventnats.NewSubscriber(env.Conn, goeventnats.SubscriberConfig{})
	require.NoError(t, err)

	bus := event.NewBus(dispatcher, publisher, commandSub, eventSub, event.BusOptions{Queue: "svc"})

	commandSeen := make(chan string, 1)
	event.HandleCommand(bus, func(ctx context.Context, cmd *renameAccount) error {
		commandSeen <- cmd.Name
		return nil
	})

	event.HandleQuery(bus, func(ctx context.Context, query *getAccount) (accountView, error) {
		return accountView{ID: query.AccountID}, nil
	})

	orderedSeen := make(chan string, 1)
	event.HandleEvent(bus, func(ctx context.Context, evt *accountCreated) error {
		orderedSeen <- evt.AccountID
		return nil
	})

	queueSeen := make(chan string, 1)
	event.HandleQueueEvent(bus, func(ctx context.Context, evt *accountCreated) error {
		queueSeen <- evt.AccountID
		return nil
	})

	require.NoError(t, bus.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Disconnect()) })

	require.NoError(t, bus.DispatchCommand(t.Context(), &renameAccount{AccountID: "acct-1", Name: "Fresh"}))
	require.Equal(t, "Fresh", receive(t, commandSeen))

	view, err := event.Ask[accountView](t.Context(), bus, &getAccount{AccountID: "acct-1"})
	require.NoError(t, err)
	require.Equal(t, "acct-1", view.ID)

	require.NoError(t, store.Save(t.Context(), &account{ID: "acct-1"}, 0, &accountCreated{AccountID: "acct-1", Name: "Acme"}))
	require.Equal(t, "acct-1", receive(t, orderedSeen))
	require.Equal(t, "acct-1", receive(t, queueSeen))
}

func TestNewBusConstructsNATSPrimitives(t *testing.T) {
	env := natstest.Run(t)
	env.CreateEventStream(t, "test", "account")
	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{})
	require.NoError(t, err)

	bus, err := goeventnats.NewBus(env.Conn, goeventnats.BusConfig{Queue: "svc"})
	require.NoError(t, err)

	commandSeen := make(chan string, 1)
	event.HandleCommand(bus, func(ctx context.Context, cmd *renameAccount) error {
		commandSeen <- cmd.Name
		return nil
	})

	eventSeen := make(chan string, 1)
	event.HandleEvent(bus, func(ctx context.Context, evt *accountCreated) error {
		eventSeen <- evt.AccountID
		return nil
	})

	require.NoError(t, bus.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Disconnect()) })

	require.NoError(t, bus.DispatchCommand(t.Context(), &renameAccount{AccountID: "acct-1", Name: "Fresh"}))
	require.Equal(t, "Fresh", receive(t, commandSeen))

	require.NoError(t, store.Save(t.Context(), &account{ID: "acct-1"}, 0, &accountCreated{AccountID: "acct-1", Name: "Acme"}))
	require.Equal(t, "acct-1", receive(t, eventSeen))
}

func TestNATSBusPublishDeliversCustomEventKind(t *testing.T) {
	env := natstest.Run(t)
	env.CreateMessageStream(t, "test", "log", "worker")

	bus, err := goeventnats.NewBus(env.Conn, goeventnats.BusConfig{Queue: "logger"})
	require.NoError(t, err)

	seen := make(chan string, 1)
	event.HandleQueueEvent(bus, func(ctx context.Context, evt *logEmitted) error {
		seen <- evt.Message
		return nil
	})
	require.NoError(t, bus.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Disconnect()) })

	require.NoError(t, bus.Publish(t.Context(), &logEmitted{
		WorkerID: "worker-1",
		Level:    "info",
		Message:  "started",
	}))
	require.Equal(t, "started", receive(t, seen))
}

func TestQueueSubscriptionCreatesConsumerPerFilter(t *testing.T) {
	env := natstest.Run(t)
	stream := env.CreateEventStream(t, "test", "account")
	info, err := stream.Info(t.Context())
	require.NoError(t, err)

	subscriber, err := goeventnats.NewSubscriber(env.Conn, goeventnats.SubscriberConfig{})
	require.NoError(t, err)
	registerAccountEvents(t, subscriber)
	sub, err := subscriber.SubscribeEvents(t.Context(), event.EventHandlerFunc(func(ctx context.Context, evt event.Event, cursor []byte) error {
		return nil
	}), event.EventSubscriptionConfig{
		AggregateScope: "test",
		AggregateTypes: []string{"account"},
		EventNames:     []string{"created", "renamed"},
		Queue:          "projection",
		ConsumerName:   "projection_account",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sub.Stop()) })

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

func TestNATSBusQueueHandlerSupportsCustomEventKind(t *testing.T) {
	env := natstest.Run(t)
	env.CreateMessageStream(t, "test", "log", "worker")
	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{})
	require.NoError(t, err)
	eventSub, err := goeventnats.NewSubscriber(env.Conn, goeventnats.SubscriberConfig{})
	require.NoError(t, err)
	bus := event.NewBus(nil, nil, nil, eventSub, event.BusOptions{Queue: "logger"})

	seen := make(chan string, 1)
	event.HandleQueueEvent(bus, func(ctx context.Context, evt *logEmitted) error {
		seen <- evt.Message
		return nil
	})
	require.NoError(t, bus.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, bus.Disconnect()) })

	require.NoError(t, store.Save(t.Context(), &workerLog{ID: "worker-1"}, 0, &logEmitted{
		WorkerID: "worker-1",
		Level:    "info",
		Message:  "started",
	}))
	require.Equal(t, "started", receive(t, seen))
}

func BenchmarkRawJetStreamSubscription(b *testing.B) {
	env := natstest.Run(b)
	env.CreateEventStream(b, "test", "account")
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
	env.CreateEventStream(b, "test", "account")
	subscriber, err := goeventnats.NewSubscriber(env.Conn, goeventnats.SubscriberConfig{})
	require.NoError(b, err)
	registerAccountEvents(b, subscriber)
	var count atomic.Uint64
	sub, err := subscriber.SubscribeEvents(b.Context(), event.EventHandlerFunc(func(ctx context.Context, evt event.Event, cursor []byte) error {
		count.Add(1)
		return nil
	}), event.EventSubscriptionConfig{
		AggregateScope: "test",
		AggregateTypes: []string{"account"},
		EventNames:     []string{"created"},
		Queue:          "bench",
		ConsumerName:   "bench",
	})
	require.NoError(b, err)
	defer func() { _ = sub.Stop() }()

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
