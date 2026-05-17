# go-event

`go-event` is a small Go library for message-oriented services and event sourcing.
It defines stable domain contracts for commands, queries, events, event-sourced
aggregates, and transport-neutral subscriptions, with a NATS JetStream backend for
production.

The library keeps the domain space intentionally small:

- `Command` asks a service to do something.
- `Query[R]` asks a service to return a typed result without side effects.
- `Event` records a committed aggregate state change.
- `Aggregate` rebuilds state by applying events.
- `EventStore` appends and loads aggregate event streams with optimistic concurrency.
- `Bus` wires dispatchers, command/query subscribers, and event subscribers together
  while allowing handlers to be added before or after `Start`.

## Install

```sh
go get github.com/dpotapov/go-event
```

## NATS Subject Convention

The NATS backend uses one five-token subject shape:

```text
<scope>.<class>.<aggregate-type>.<aggregate-id>.<message-name>
```

Classes are:

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
`EnsureEventStream` enables `AllowAtomicPublish` by default.

## Stream Creation

Use the helper for a suitable default stream, then customize any JetStream field by
mutating the config:

```go
stream, err := goeventnats.EnsureEventStream(ctx, js, "billing", "account",
    goeventnats.ConfigureStream(func(cfg *jetstream.StreamConfig) {
        cfg.Replicas = 3
        cfg.MaxAge = 30 * 24 * time.Hour
        cfg.Metadata["owner"] = "billing"
    }),
)
```

## Bus Example

```go
catalog := event.NewCatalog()
event.MustRegisterEventType[*AccountCreated](catalog)
event.MustRegisterCommandType[*RenameAccount](catalog)

dispatcher, _ := goeventnats.NewDispatcher(nc, goeventnats.DispatcherConfig{})
commands, _ := goeventnats.NewCommandSubscriber(
    nc,
    goeventnats.CommandSubscriberConfig{
        Catalog: catalog,
        Queue:   "accounts",
    },
)
events, _ := goeventnats.NewSubscriber(nc, goeventnats.SubscriberConfig{Catalog: catalog})

bus := event.NewBus(dispatcher, commands, events, event.BusOptions{
    Catalog: catalog,
    Queue:   "accounts",
})

_, _ = event.CommandHandlerOf(bus, func(ctx context.Context, cmd *RenameAccount) error {
    return nil
})

_, _ = event.EventHandlerOf(bus, func(ctx context.Context, evt *AccountCreated) error {
    return nil
})

if err := bus.Start(ctx); err != nil {
    return err
}
defer bus.Stop(context.Background())
```

Handlers can be registered after `Start`. Ordered event handlers are grouped by
aggregate type behind a broad ordered subscription; queue handlers use precise durable
consumers.

## Queue Consumer Strategy

Queue event subscriptions create one durable consumer per concrete filter instead of
forcing unrelated subjects into one broad consumer. This avoids over-delivery and keeps
authorization scopes narrow. If a direct subscription requests multiple event names,
the NATS adapter creates multiple consumers with stable derived names.

## Testing

Fast unit tests can use `testkit.NewBusHarness`, which records subscriptions and lets
tests trigger commands, queries, and events directly.

Real NATS tests can use the embedded JetStream server:

```go
env := natstest.Run(t)
env.EnsureEventStream(t, "billing", "account", goeventnats.WithMemoryStorage())
```

The server listens on a random local port and is cleaned up through `t.Cleanup`.

## Benchmarks

The NATS package includes benchmarks for raw JetStream consumption and the `go-event`
subscription path:

```sh
go test -bench=. -benchmem ./nats
```

Benchmarks are useful for catching overhead regressions, but exact ratios depend on
machine, server settings, and Go/NATS versions.
