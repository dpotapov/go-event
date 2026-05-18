package nats

import (
	goevent "github.com/dpotapov/go-event"
	"github.com/nats-io/nats.go/jetstream"
)

type AggregateInfo interface {
	AggregateScope() string
	AggregateType() string
}

func StreamSubject(agg AggregateInfo) string {
	kind := goevent.KindEvent
	if override, ok := agg.(goevent.MessageKindOverride); ok && override.MessageKind() != "" {
		kind = goevent.MessageKind(override.MessageKind())
	}
	return EventTypeFilter(kind, agg.AggregateScope(), agg.AggregateType())
}

func SetStreamSubjects(cfg *jetstream.StreamConfig, agg AggregateInfo) {
	cfg.Subjects = []string{StreamSubject(agg)}
}
