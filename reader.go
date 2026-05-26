package event

import (
	"context"
	"iter"
	"time"
)

// StoredEvent is one decoded aggregate message read from the event store.
type StoredEvent struct {
	Event     Event
	Version   uint64 // JetStream stream sequence for this message
	Timestamp time.Time
}

// ReadOptions configures aggregate event streaming.
type ReadOptions struct {
	// SkipSnapshotFastPath disables Load-style "start after latest snapshot"
	// optimization. When false (default), Stream starts at snapshotSeq+1.
	SkipSnapshotFastPath bool

	// IncludeSnapshots controls whether snapshot subject messages are yielded.
	// Default false: domain events only.
	IncludeSnapshots bool
}

// Reader streams decoded aggregate history without mutating aggregate state.
type Reader interface {
	Stream(ctx context.Context, agg ESAggregate, opts ReadOptions) iter.Seq2[StoredEvent, error]
}

// Collect reads all events from a stream into a slice.
func Collect(ctx context.Context, r Reader, agg ESAggregate, opts ReadOptions) ([]StoredEvent, error) {
	var out []StoredEvent
	for rec, err := range r.Stream(ctx, agg, opts) {
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

// FindFirst returns the first stored event matching pred.
func FindFirst(ctx context.Context, r Reader, agg ESAggregate, opts ReadOptions, pred func(Event) bool) (StoredEvent, bool, error) {
	for rec, err := range r.Stream(ctx, agg, opts) {
		if err != nil {
			return StoredEvent{}, false, err
		}
		if pred(rec.Event) {
			return rec, true, nil
		}
	}
	return StoredEvent{}, false, nil
}
