package nats

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/dpotapov/go-event"
	"github.com/nats-io/nats.go/jetstream"
)

const aggregateConsumerInactiveThreshold = 5 * time.Second

type aggregateReadContext struct {
	stream         jetstream.Stream
	filter         string
	registry       event.DecoderRegistry
	snapshotSuffix string
	snapshottable  bool
}

func (s *EventStore) resolveReadContext(ctx context.Context, agg event.ESAggregate) (aggregateReadContext, error) {
	registry, err := eventRegistryFor(agg)
	if err != nil {
		return aggregateReadContext{}, err
	}
	streamName := s.streamFor(agg.AggregateScope(), agg.AggregateType())
	stream, err := s.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return aggregateReadContext{}, errStreamNotFound
		}
		return aggregateReadContext{}, fmt.Errorf("get stream %s: %w", streamName, err)
	}
	_, snapshotType := agg.EventTypes()
	snapshotSuffix := "." + snapshotEventName(snapshotType)
	_, snapshottable := agg.(event.Snapshottable)
	return aggregateReadContext{
		stream:         stream,
		filter:         AggregateMessageFilter(agg.AggregateScope(), agg.AggregateType(), agg.AggregateID()),
		registry:       registry,
		snapshotSuffix: snapshotSuffix,
		snapshottable:  snapshottable,
	}, nil
}

func (s *EventStore) resolveGenericReadContext(ctx context.Context, ref event.AggregateRef) (aggregateReadContext, error) {
	streamName := s.streamFor(ref.AggregateScope(), ref.AggregateType())
	stream, err := s.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return aggregateReadContext{}, errStreamNotFound
		}
		return aggregateReadContext{}, fmt.Errorf("get stream %s: %w", streamName, err)
	}
	return aggregateReadContext{
		stream: stream,
		filter: AggregateMessageFilter(ref.AggregateScope(), ref.AggregateType(), ref.AggregateID()),
	}, nil
}

var errStreamNotFound = errors.New("event stream not found")

func startSeqAfterSnapshot(
	ctx context.Context,
	stream jetstream.Stream,
	agg event.ESAggregate,
	opts event.ReadOptions,
	applySnapshot func(subject string, data []byte, timestamp time.Time) error,
) (uint64, *jetstream.RawStreamMsg, error) {
	if opts.SkipSnapshotFastPath {
		return 0, nil, nil
	}
	msg, err := stream.GetLastMsgForSubject(ctx, SnapshotSubject(agg))
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return 0, nil, nil
		}
		return 0, nil, fmt.Errorf("get latest snapshot: %w", err)
	}
	if applySnapshot != nil {
		if err := applySnapshot(msg.Subject, msg.Data, msg.Time); err != nil {
			return 0, nil, err
		}
	}
	var raw *jetstream.RawStreamMsg
	if opts.IncludeSnapshots {
		raw = msg
	}
	return msg.Sequence + 1, raw, nil
}

func openAggregateConsumer(ctx context.Context, stream jetstream.Stream, filter string, startSeq uint64) (jetstream.Consumer, error) {
	if startSeq == 0 {
		startSeq = 1
	}
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{filter},
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       startSeq,
		InactiveThreshold: aggregateConsumerInactiveThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("create aggregate consumer: %w", err)
	}
	return consumer, nil
}

func (s *EventStore) forEachAggregateMessage(
	ctx context.Context,
	consumer jetstream.Consumer,
	fn func(subject string, data []byte, timestamp time.Time, version uint64) error,
) (uint64, error) {
	info := consumer.CachedInfo()
	if info == nil || info.NumPending == 0 {
		return 0, nil
	}
	iter, err := consumer.Messages()
	if err != nil {
		return 0, fmt.Errorf("open event iterator: %w", err)
	}
	defer iter.Stop()

	var version uint64
	for {
		select {
		case <-ctx.Done():
			return version, ctx.Err()
		default:
		}

		msg, err := iter.Next()
		if err != nil {
			return version, fmt.Errorf("read event: %w", err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			return version, fmt.Errorf("read event metadata: %w", err)
		}
		version = meta.Sequence.Stream
		if err := fn(msg.Subject(), msg.Data(), meta.Timestamp, version); err != nil {
			return version, err
		}
		if meta.NumPending == 0 {
			return version, nil
		}
	}
}

func (s *EventStore) storedEventFromMessage(
	registry event.DecoderRegistry,
	subject string,
	data []byte,
	timestamp time.Time,
	version uint64,
	snapshotSuffix string,
	snapshottable bool,
	includeSnapshots bool,
) (event.StoredEvent, bool, error) {
	isSnapshot := strings.HasSuffix(subject, snapshotSuffix)
	if isSnapshot && !includeSnapshots {
		return event.StoredEvent{}, false, nil
	}
	if isSnapshot && !snapshottable {
		return event.StoredEvent{}, false, nil
	}
	subj, ok := ParseSubject(subject)
	if !ok {
		return event.StoredEvent{}, false, fmt.Errorf("invalid event subject %q", subject)
	}
	evt, err := s.decodeEvent(registry, subject, data, timestamp)
	if err != nil {
		if !isSnapshot && isUnknownEventError(err) {
			return event.StoredEvent{}, false, nil
		}
		return event.StoredEvent{}, false, err
	}
	return event.StoredEvent{
		Event:     evt,
		Subject:   subj,
		Data:      append([]byte(nil), data...),
		Version:   version,
		Timestamp: timestamp,
	}, true, nil
}

func genericStoredEventFromMessage(subject string, data []byte, timestamp time.Time, version uint64) (event.StoredEvent, error) {
	subj, ok := ParseSubject(subject)
	if !ok {
		return event.StoredEvent{}, fmt.Errorf("invalid event subject %q", subject)
	}
	return event.StoredEvent{
		Event:     event.GenericEvent{Subject: subj},
		Subject:   subj,
		Data:      append([]byte(nil), data...),
		Version:   version,
		Timestamp: timestamp,
	}, nil
}

func storedEventFromRawSnapshot(
	s *EventStore,
	read aggregateReadContext,
	msg *jetstream.RawStreamMsg,
) (event.StoredEvent, error) {
	rec, ok, err := s.storedEventFromMessage(
		read.registry,
		msg.Subject,
		msg.Data,
		msg.Time,
		msg.Sequence,
		read.snapshotSuffix,
		read.snapshottable,
		true,
	)
	if err != nil {
		return event.StoredEvent{}, err
	}
	if !ok {
		return event.StoredEvent{}, fmt.Errorf("decode snapshot %s", msg.Subject)
	}
	return rec, nil
}

var _ event.Reader = (*EventStore)(nil)

// Stream yields aggregate events oldest first.
func (s *EventStore) Stream(ctx context.Context, ref event.AggregateRef, opts event.ReadOptions) iter.Seq2[event.StoredEvent, error] {
	return func(yield func(event.StoredEvent, error) bool) {
		agg, ok := ref.(event.ESAggregate)
		if !ok {
			s.streamGeneric(ctx, ref, yield)
			return
		}
		s.streamTyped(ctx, agg, opts, yield)
	}
}

func (s *EventStore) streamTyped(ctx context.Context, agg event.ESAggregate, opts event.ReadOptions, yield func(event.StoredEvent, error) bool) {
	read, err := s.resolveReadContext(ctx, agg)
	if errors.Is(err, errStreamNotFound) {
		return
	}
	if err != nil {
		yield(event.StoredEvent{}, err)
		return
	}

	startSeq, snapshotMsg, err := startSeqAfterSnapshot(ctx, read.stream, agg, opts, nil)
	if err != nil {
		yield(event.StoredEvent{}, err)
		return
	}

	if snapshotMsg != nil {
		rec, err := storedEventFromRawSnapshot(s, read, snapshotMsg)
		if err != nil {
			yield(event.StoredEvent{}, err)
			return
		}
		if !yield(rec, nil) {
			return
		}
	}

	consumer, err := openAggregateConsumer(ctx, read.stream, read.filter, startSeq)
	if err != nil {
		yield(event.StoredEvent{}, err)
		return
	}

	_, err = s.forEachAggregateMessage(ctx, consumer, func(subject string, data []byte, timestamp time.Time, version uint64) error {
		rec, ok, err := s.storedEventFromMessage(
			read.registry,
			subject,
			data,
			timestamp,
			version,
			read.snapshotSuffix,
			read.snapshottable,
			opts.IncludeSnapshots,
		)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if !yield(rec, nil) {
			return errStopStream
		}
		return nil
	})
	if errors.Is(err, errStopStream) {
		return
	}
	if err != nil {
		yield(event.StoredEvent{}, err)
	}
}

func (s *EventStore) streamGeneric(ctx context.Context, ref event.AggregateRef, yield func(event.StoredEvent, error) bool) {
	read, err := s.resolveGenericReadContext(ctx, ref)
	if errors.Is(err, errStreamNotFound) {
		return
	}
	if err != nil {
		yield(event.StoredEvent{}, err)
		return
	}

	consumer, err := openAggregateConsumer(ctx, read.stream, read.filter, 1)
	if err != nil {
		yield(event.StoredEvent{}, err)
		return
	}

	_, err = s.forEachAggregateMessage(ctx, consumer, func(subject string, data []byte, timestamp time.Time, version uint64) error {
		rec, err := genericStoredEventFromMessage(subject, data, timestamp, version)
		if err != nil {
			return err
		}
		if !yield(rec, nil) {
			return errStopStream
		}
		return nil
	})
	if errors.Is(err, errStopStream) {
		return
	}
	if err != nil {
		yield(event.StoredEvent{}, err)
	}
}

var errStopStream = errors.New("stop stream")
