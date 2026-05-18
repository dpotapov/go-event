package event

import "context"

// AggregateRef identifies one aggregate instance. "Ref" means reference: these
// methods name the aggregate without carrying its event-sourced state or behavior.
type AggregateRef interface {
	AggregateScope() string
	AggregateType() string
	AggregateID() string
}

// Event describes a committed state change for one aggregate instance.
type Event interface {
	AggregateRef
	EventName() string
}

// Command describes an action requested against one aggregate instance.
type Command interface {
	AggregateRef
	CommandName() string
}

// Query describes a read-only request. ResultType is only used for generic
// type inference and should return the zero value of R.
type Query[R any] interface {
	Command
	ResultType() R
}

// MessageKindOverride optionally overrides the default kind token used for a
// message. Empty values are ignored and the call site default is used.
type MessageKindOverride interface {
	MessageKind() string
}

// ESAggregate is an event-sourced entity that can rebuild its state from events.
type ESAggregate interface {
	AggregateRef
	EventTypes() []Event
	Apply(Event)
}

// Store appends and loads aggregate event streams.
type Store interface {
	Save(ctx context.Context, ref AggregateRef, expectedVersion uint64, events ...Event) error
	Load(ctx context.Context, agg ESAggregate) (version uint64, err error)
}
