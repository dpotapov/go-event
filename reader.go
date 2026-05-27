package event

import (
	"context"
	"iter"
	"time"
)

// AggregateKey is a concrete aggregate reference for callers that only need to
// identify an aggregate, not replay its state.
type AggregateKey struct {
	Scope string
	Type  string
	ID    string
}

func (k AggregateKey) AggregateScope() string { return k.Scope }
func (k AggregateKey) AggregateType() string  { return k.Type }
func (k AggregateKey) AggregateID() string    { return k.ID }

// GenericEvent identifies a stored event when the caller did not provide an
// event-sourced aggregate with concrete event types.
type GenericEvent struct {
	Subject Subject
}

func (e GenericEvent) AggregateScope() string { return e.Subject.Scope }
func (e GenericEvent) AggregateType() string  { return e.Subject.AggregateType }
func (e GenericEvent) AggregateID() string    { return e.Subject.AggregateID }
func (e GenericEvent) EventName() string      { return e.Subject.Name }

// StoredEvent is one aggregate message read from the event store.
type StoredEvent struct {
	Event     Event
	Subject   Subject
	Data      []byte
	Version   uint64 // store sequence for this message
	Timestamp time.Time
}

// ReadOptions configures aggregate event streaming.
type ReadOptions struct {
	// SkipSnapshotFastPath disables Load-style "start after latest snapshot"
	// optimization for event-sourced aggregates. When false (default), typed
	// Stream calls start at snapshotSeq+1.
	SkipSnapshotFastPath bool

	// IncludeSnapshots controls whether snapshot subject messages are yielded
	// for event-sourced aggregates. Default false: domain events only.
	IncludeSnapshots bool
}

// Reader streams aggregate history without mutating aggregate state. If ref also
// implements ESAggregate, events are decoded into the concrete EventTypes. Plain
// AggregateRef values stream generic events with opaque Data bytes.
type Reader interface {
	Stream(ctx context.Context, ref AggregateRef, opts ReadOptions) iter.Seq2[StoredEvent, error]
}

// Collect reads all events from a stream into a slice.
func Collect(ctx context.Context, r Reader, ref AggregateRef, opts ReadOptions) ([]StoredEvent, error) {
	var out []StoredEvent
	for rec, err := range r.Stream(ctx, ref, opts) {
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

// FindFirst returns the first stored event matching pred.
func FindFirst(ctx context.Context, r Reader, ref AggregateRef, opts ReadOptions, pred func(Event) bool) (StoredEvent, bool, error) {
	for rec, err := range r.Stream(ctx, ref, opts) {
		if err != nil {
			return StoredEvent{}, false, err
		}
		if pred(rec.Event) {
			return rec, true, nil
		}
	}
	return StoredEvent{}, false, nil
}
