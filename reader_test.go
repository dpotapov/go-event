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

func (s stubReader) Stream(_ context.Context, _ event.AggregateRef, _ event.ReadOptions) iter.Seq2[event.StoredEvent, error] {
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

func TestAggregateKey(t *testing.T) {
	ref := event.AggregateKey{Scope: "billing", Type: "account", ID: "acct-1"}

	require.Equal(t, "billing", ref.AggregateScope())
	require.Equal(t, "account", ref.AggregateType())
	require.Equal(t, "acct-1", ref.AggregateID())
}

func TestGenericEvent(t *testing.T) {
	subj := event.Subject{
		Scope:         "billing",
		Kind:          event.KindEvent,
		AggregateType: "account",
		AggregateID:   "acct-1",
		Name:          "created",
	}
	evt := event.GenericEvent{Subject: subj}

	require.Equal(t, "billing", evt.AggregateScope())
	require.Equal(t, "account", evt.AggregateType())
	require.Equal(t, "acct-1", evt.AggregateID())
	require.Equal(t, "created", evt.EventName())
}

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
