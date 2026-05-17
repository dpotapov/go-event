package event

import "context"

// Event describes a committed state change for one aggregate instance.
type Event interface {
	AggregateScope() string
	AggregateType() string
	AggregateID() string
	EventName() string
}

// Command describes an action requested against one aggregate instance.
type Command interface {
	AggregateScope() string
	AggregateType() string
	AggregateID() string
	CommandName() string
}

// Query describes a read-only request. ResultType is only used for generic
// type inference and should return the zero value of R.
type Query[R any] interface {
	Command
	ResultType() R
}

// Aggregate is an event-sourced entity that can rebuild its state from events.
type Aggregate interface {
	AggregateScope() string
	AggregateType() string
	AggregateID() string
	Apply(Event) error
}

// EventStore appends and loads aggregate event streams.
type EventStore interface {
	Save(ctx context.Context, aggregate Aggregate, expectedVersion uint64, events ...Event) error
	Load(ctx context.Context, aggregate Aggregate) (version uint64, err error)
}
