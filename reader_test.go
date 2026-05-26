package event_test

import (
	"context"
	"iter"
	"testing"
	"time"

	"github.com/dpotapov/go-event"
	"github.com/stretchr/testify/require"
)

type stubReader struct {
	events []event.StoredEvent
	err    error
}

func (s stubReader) Stream(_ context.Context, _ event.ESAggregate, _ event.ReadOptions) iter.Seq2[event.StoredEvent, error] {
	return func(yield func(event.StoredEvent, error) bool) {
		for _, rec := range s.events {
			if !yield(rec, nil) {
				return
			}
		}
		if s.err != nil {
			yield(event.StoredEvent{}, s.err)
		}
	}
}

type stubEvent struct {
	event.AggregateRef
	name string
}

func (e stubEvent) EventName() string { return e.name }

func TestCollect(t *testing.T) {
	now := time.Now().UTC()
	reader := stubReader{events: []event.StoredEvent{
		{Event: stubEvent{name: "created"}, Version: 1, Timestamp: now},
		{Event: stubEvent{name: "renamed"}, Version: 2, Timestamp: now.Add(time.Second)},
	}}

	got, err := event.Collect(t.Context(), reader, nil, event.ReadOptions{})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "created", got[0].Event.EventName())
	require.Equal(t, uint64(2), got[1].Version)
}

func TestFindFirst(t *testing.T) {
	reader := stubReader{events: []event.StoredEvent{
		{Event: stubEvent{name: "created"}},
		{Event: stubEvent{name: "renamed"}},
	}}

	rec, ok, err := event.FindFirst(t.Context(), reader, nil, event.ReadOptions{}, func(evt event.Event) bool {
		return evt.EventName() == "renamed"
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "renamed", rec.Event.EventName())

	_, ok, err = event.FindFirst(t.Context(), reader, nil, event.ReadOptions{}, func(event.Event) bool {
		return false
	})
	require.NoError(t, err)
	require.False(t, ok)
}
