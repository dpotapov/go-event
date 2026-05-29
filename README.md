# go-event

`go-event` is a small Go library for message-oriented services and event sourcing.
It defines stable domain contracts for commands, queries, events, event-sourced
aggregates, and transport-neutral subscriptions, with a NATS JetStream backend for
production.

The library keeps the domain space intentionally small, but each concept has a
specific role:

- `Command` is a synchronous request to make a service do something. It is expected
  to complete quickly, usually within a request timeout configured by the message
  bus implementation.
- `Query[R]` is like a command, but returns a typed result. Query handlers should
  read state and report an answer without committing domain changes.
- `Event` records a committed aggregate state change. Events can also be published
  as integration events so other services can react asynchronously.
- `ESAggregate` rebuilds state by applying the event types it explicitly declares.
- `Store` appends and loads aggregate event streams with optimistic
  concurrency. For background on event sourcing, see Martin Fowler's
  [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) and Greg
  Young's [Building an Event Storage](https://cqrs.wordpress.com/documents/building-event-storage/).
- `Reader` streams decoded aggregate history for audit and read-side inspection.
- `Bus` lets services communicate by dispatching commands, invoking queries, and
  subscribing to commands, queries, and events. Event subscriptions have two common
  models: an event handler builds local projections from an ordered event stream,
  while a queue event handler behaves like an asynchronous command distributed among
  workers.

## Install

```sh
go get github.com/dpotapov/go-event
```

## Quick Tutorial: Order Processing Example

```go
import (
    ...
	event "github.com/dpotapov/go-event"
	natsevt "github.com/dpotapov/go-event/nats"
	"github.com/nats-io/nats.go"
)
```

Start with small aggregate identity types. Embedding one in each message promotes
the `AggregateRef` methods and keeps message definitions focused on payload fields.

```go
type OrderID string

// Scope names the service or bounded context that owns this aggregate.
func (OrderID) AggregateScope() string { return "sales" }
func (OrderID) AggregateType() string  { return "order" }
func (id OrderID) AggregateID() string { return string(id) }

type WorkerID int

func (WorkerID) AggregateScope() string { return "ops" }
func (WorkerID) AggregateType() string  { return "worker" }
func (id WorkerID) AggregateID() string { return strconv.Itoa(int(id)) }
```

Define the command, domain event, and an event-like log message:

```go
type OrderCreateCommand struct {
	OrderID    `json:"order_id"`
	CustomerID string `json:"customer_id"`
}

func (*OrderCreateCommand) CommandName() string { return "create" }

type OrderCreatedEvent struct {
	OrderID    `json:"order_id"`
	CustomerID string `json:"customer_id"`
}

func (*OrderCreatedEvent) EventName() string { return "created" }

type WorkerLogMessage struct {
	WorkerID `json:"worker_id"`

	Level   string    `json:"level"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

// MessageKind is optional. Events default to "event", commands to "command",
// and queries to "query". Logs are event-like: they happened and should be
// recorded, not handled as calls to do something.
func (*WorkerLogMessage) MessageKind() string { return "log" }
func (e *WorkerLogMessage) EventName() string { return e.Level }
```

The worker subscribes to `OrderCreateCommand` and stores the resulting
`OrderCreatedEvent` with optimistic concurrency through `event.Execute`.

```go
func startOrderWorker(ctx context.Context, nc *nats.Conn) (*event.Bus, error) {
	es, err := natsevt.NewEventStore(nc, natsevt.EventStoreConfig{})
	if err != nil {
		return nil, fmt.Errorf("create event store: %w", err)
	}

	bus, err := natsevt.NewBus(nc, natsevt.BusConfig{
		Queue: "order-workers",
	})
	if err != nil {
		return nil, fmt.Errorf("create bus: %w", err)
	}
	event.HandleCommand(bus, orderCreateHandler(es, bus, WorkerID(1)))
	if err := bus.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect bus: %w", err)
	}
	return bus, nil
}
```

There are two setup rules in the example above:

- Register handlers before calling `Connect`. The typed helper
  `event.HandleCommand` tells the bus both which subject to subscribe to and
  which Go type to decode when a message arrives.
- Teach each event-sourced aggregate how to replay its history. `event.Execute`
  loads the current aggregate state before running the command handler, so the
  aggregate lists the event types it can apply with `EventTypes`.

Most services can start with `natsevt.NewBus`. Use `event.NewBus` only when you
need lower-level transport configuration, such as custom JetStream consumer
options.

The command handler mutates an event-sourced aggregate. After the order is created,
it publishes a log message directly through the bus because logs do not need
aggregate replay or optimistic concurrency checks. The `newOrder` factory gives
`event.Execute` a fresh aggregate when it retries after a concurrent write.

```go
type Order struct {
	OrderID
	Created bool
}

func (*Order) EventTypes() ([]event.Event, event.SnapshotEvent) {
	return []event.Event{&OrderCreatedEvent{}}, nil
}

func (o *Order) Apply(evt event.Event) {
	switch e := evt.(type) {
	case *OrderCreatedEvent:
		o.OrderID = e.OrderID
		o.Created = true
	}
}

func orderCreateHandler(store event.Store, publisher event.Publisher, workerID WorkerID) func(context.Context, *OrderCreateCommand) error {
	return func(ctx context.Context, cmd *OrderCreateCommand) error {
		newOrder := func() *Order {
			return &Order{OrderID: cmd.OrderID}
		}

		err := event.Execute(ctx, store,
			newOrder,
			func(ctx context.Context, order *Order, version, attempt uint64) ([]event.Event, error) {
				if order.Created {
					return nil, nil
				}
				return []event.Event{&OrderCreatedEvent{
					OrderID:    cmd.OrderID,
					CustomerID: cmd.CustomerID,
				}}, nil
			},
		)
		if err != nil {
			return fmt.Errorf("create order: %w", err)
		}
		return publisher.Publish(ctx, &WorkerLogMessage{
			WorkerID: workerID,
			Level:    "info",
			Message:  "order created",
			Time:     time.Now(),
		})
	}
}
```

Use `Store.Save` for event-sourced aggregate state. Use `Bus.Publish` for simple
messages such as logs where there is no aggregate version to check.

## NATS Subject Convention

The NATS backend uses one five-token subject shape:

```text
<scope>.<kind>.<aggregate-type>.<aggregate-id>.<message-name>
```

Kinds are:

```text
event
command
query
```

Examples:

```text
billing.event.account.acct-123.created
billing.command.account.acct-123.rename
billing.query.account.acct-123.get
```

Concrete tokens must not contain dots, whitespace, `*`, `>`, or path separators.
Stream subjects use one trailing wildcard:

```text
<scope>.event.<aggregate-type>.>
```

The default stream name is:

```text
<SCOPE>_<AGG>_EVENTS
```

## Event Store CAS

The NATS event store uses JetStream per-subject optimistic concurrency. For an
aggregate save it sets:

```text
Nats-Expected-Last-Subject-Sequence
Nats-Expected-Last-Subject-Sequence-Subject
```

The expected subject is the aggregate filter:

```text
<scope>.event.<aggregate-type>.<aggregate-id>.>
```

That means the returned aggregate version is the last JetStream stream sequence for
that aggregate, not a simple count of aggregate events. This is deliberate: it lets
different event names for the same aggregate share one CAS boundary.

When saving multiple events, the NATS backend uses `Nats-Batch-Id`,
`Nats-Batch-Sequence`, and `Nats-Batch-Commit` so the batch commits atomically.

## Event Store Snapshots

The NATS event store can write snapshot events to the same aggregate stream. A
snapshot uses the normal event kind and defaults to this subject:

```text
<scope>.event.<aggregate-type>.<aggregate-id>.snapshot
```

Enable automatic snapshot writes with `EventStoreConfig.SnapshotEvery`. When the
number of aggregate events since the latest snapshot reaches `SnapshotEvery`,
the store writes a new snapshot after a successful save. Snapshot write failures
are logged because the domain events were already committed.

On load, the store reads the latest snapshot with JetStream direct get-last and
then replays only events after that snapshot sequence. `SnapshotEvery` only
controls writes; existing snapshots are used on load even when automatic writes
are disabled.

There are two snapshot styles:

- **Implicit snapshots:** aggregates do not implement `Snapshottable`; the store
  stores the aggregate value itself. On replay, snapshot bytes are decoded
  directly into the aggregate and `Apply` is skipped for that snapshot message.
  This works best with exported JSON fields or an aggregate-level NATS codec.
- **Explicit snapshots:** aggregates implement `Snapshottable` and return a
  domain snapshot event from `TakeSnapshot`. `EventTypes` returns that snapshot
  prototype as its second return value, and `Apply` should restore state
  idempotently from it.

```go
type OrderSnapshot struct {
	event.SnapshotEventBase

	OrderID string `json:"order_id"`
	Created bool   `json:"created"`
}

func (*Order) EventTypes() ([]event.Event, event.SnapshotEvent) {
	return []event.Event{&OrderCreatedEvent{}}, &OrderSnapshot{}
}

func (o *Order) TakeSnapshot() (event.SnapshotEvent, error) {
	return &OrderSnapshot{OrderID: o.OrderID, Created: o.Created}, nil
}
```

Embed `event.SnapshotEventBase` to use the default `snapshot` event name, or
override `EventName` on the snapshot type to choose a different final subject
token.

## Reading Aggregate History

Use `Store.Load` when you need to rebuild aggregate state for command handling.
Use `Reader.Stream` when you need decoded events for audit, debug, or read-side
inspection without mutating an aggregate.

The NATS event store implements both `event.Store` and `event.Reader`:

```go
for rec, err := range store.Stream(ctx, &Order{OrderID: id}, event.ReadOptions{}) {
	if err != nil {
		return fmt.Errorf("stream order history: %w", err)
	}
	// rec.Event, rec.Subject, rec.Data, rec.Version, rec.Timestamp
}
```

When the reference passed to `Stream` also implements `ESAggregate`, events are
decoded into the concrete types declared by `EventTypes`, matching the load
replay contract. When the reference is only an `AggregateRef`, the stream yields
all matching aggregate messages as `event.GenericEvent` values and leaves the
stored payload in `StoredEvent.Data` as opaque bytes:

```go
ref := event.AggregateKey{Scope: "sales", Type: "order", ID: string(id)}
for rec, err := range store.Stream(ctx, ref, event.ReadOptions{}) {
	if err != nil {
		return fmt.Errorf("stream raw order history: %w", err)
	}
	// rec.Event.EventName() names the event; rec.Data contains transport bytes.
}
```

`ReadOptions` controls replay:

- `SkipSnapshotFastPath` — when false (default), streaming starts after the
  latest snapshot for event-sourced aggregates, matching the replay window
  `Load` uses.
- `IncludeSnapshots` — when false (default), snapshot subject messages are
  omitted for event-sourced aggregates. Set true to include explicit snapshot
  events.

Helpers:

```go
events, err := event.Collect(ctx, store, agg, event.ReadOptions{})
rec, ok, err := event.FindFirst(ctx, store, agg, event.ReadOptions{}, func(evt event.Event) bool {
	return evt.EventName() == "created"
})
```

By default, the read path uses an ephemeral JetStream ordered consumer filtered
to one aggregate. When running with scoped NATS permissions, configure
`EventStoreConfig.ConsumerName` to return a deterministic consumer name for the
aggregate:

```go
store, err := natsevt.NewEventStore(nc, natsevt.EventStoreConfig{
	ConsumerName: func(ref event.AggregateRef) string {
		return "orders_" + ref.AggregateID()
	},
})
```

The same consumer name is used for `Load`, snapshot counting during `Save`, and
`Stream`. Calls for the same aggregate/name should be sequential because the
store deletes any stale named consumer before use and deletes it again after use.

## Stream Creation

The library owns the subject convention, but your application owns stream
creation. Use `SetStreamSubjects` to put the right subject filter on any
JetStream stream config:

```go
cfg := jetstream.StreamConfig{
	Name:               "BILLING_ACCOUNT_EVENTS",
	Storage:            jetstream.FileStorage,
	AllowAtomicPublish: true,
}
natsevt.SetStreamSubjects(&cfg, OrderID(""))
stream, err := js.CreateOrUpdateStream(ctx, cfg)
```

Handlers are registered before `Connect`. Ordered event handlers are grouped by
aggregate type behind handler-derived ordered subscription filters and catch up
before command and queue handlers start; queue handlers use precise durable
consumers.

## Queue Consumer Strategy

In the NATS backend, queue handlers on the same event stream share one durable
consumer named after the queue. For queue `projection` on the `test`/`account`
stream, the durable is `projection` with filters such as
`test.event.account.*.created` and `test.log.account.acct-1.info`.
