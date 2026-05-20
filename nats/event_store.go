package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dpotapov/go-event"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

const (
	batchIDHeader     = "Nats-Batch-Id"
	batchSeqHeader    = "Nats-Batch-Sequence"
	batchCommitHeader = "Nats-Batch-Commit"
)

type EventStoreConfig struct {
	StreamPattern string
	Logger        *slog.Logger
	SnapshotEvery uint64
}

type EventStore struct {
	nc            *gonats.Conn
	js            jetstream.JetStream
	streamPattern string
	snapshotEvery uint64
	logger        *slog.Logger
}

var _ event.Store = (*EventStore)(nil)

func NewEventStore(nc *gonats.Conn, cfg EventStoreConfig) (*EventStore, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is required")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	pattern := cfg.StreamPattern
	if pattern == "" {
		pattern = DefaultStreamPattern
	}
	return &EventStore{
		nc:            nc,
		js:            js,
		streamPattern: pattern,
		snapshotEvery: cfg.SnapshotEvery,
		logger:        logger,
	}, nil
}

func (s *EventStore) Save(ctx context.Context, agg event.AggregateRef, expectedVersion uint64, events ...event.Event) error {
	if len(events) == 0 {
		return nil
	}
	if err := validateEvents(agg, events); err != nil {
		return err
	}
	eventsToPublish := events
	snapshot, err := s.snapshotForSave(ctx, agg, expectedVersion, events)
	if err != nil {
		return err
	}
	if snapshot != nil {
		eventsToPublish = append(append([]event.Event{}, events...), snapshot)
	}
	if len(eventsToPublish) == 1 {
		_, err = s.publishOne(ctx, agg, expectedVersion, eventsToPublish[0])
		return err
	}
	_, err = s.publishBatch(ctx, agg, expectedVersion, eventsToPublish)
	return err
}

func (s *EventStore) Load(ctx context.Context, agg event.ESAggregate) (uint64, error) {
	registry, err := eventRegistryFor(agg)
	if err != nil {
		return 0, err
	}
	streamName := s.streamFor(agg.AggregateScope(), agg.AggregateType())
	filter := AggregateMessageFilter(agg.AggregateScope(), agg.AggregateType(), agg.AggregateID())
	_, snapshotType := agg.EventTypes()
	snapshotSuffix := "." + snapshotEventName(snapshotType)
	_, snapshottable := agg.(event.Snapshottable)
	stream, err := s.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("get stream %s: %w", streamName, err)
	}
	startSeq := uint64(0)
	if msg, err := stream.GetLastMsgForSubject(ctx, SnapshotSubject(agg)); err == nil {
		if err := s.applyMessage(registry, msg.Subject, msg.Data, msg.Time, agg, snapshotSuffix, snapshottable); err != nil {
			return 0, err
		}
		startSeq = msg.Sequence + 1
	} else if !errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, fmt.Errorf("get latest snapshot: %w", err)
	}
	version, err := s.loadFrom(ctx, stream, filter, registry, agg, startSeq, snapshotSuffix, snapshottable)
	if err != nil {
		return 0, err
	}
	if version == 0 && startSeq > 0 {
		return startSeq - 1, nil
	}
	return version, nil
}

func (s *EventStore) loadFrom(
	ctx context.Context,
	stream jetstream.Stream,
	filter string,
	registry event.DecoderRegistry,
	agg event.ESAggregate,
	startSeq uint64,
	snapshotSuffix string,
	snapshottable bool,
) (uint64, error) {
	if startSeq == 0 {
		startSeq = 1
	}
	consumerCfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{filter},
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       startSeq,
		InactiveThreshold: 5 * time.Second,
	}
	consumer, err := stream.OrderedConsumer(ctx, consumerCfg)
	if err != nil {
		return 0, fmt.Errorf("create load consumer: %w", err)
	}
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
		msg, err := iter.Next()
		if err != nil {
			return 0, fmt.Errorf("read event: %w", err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			return 0, fmt.Errorf("read event metadata: %w", err)
		}
		version = meta.Sequence.Stream
		if err := s.applyMessage(registry, msg.Subject(), msg.Data(), meta.Timestamp, agg, snapshotSuffix, snapshottable); err != nil {
			return 0, err
		}
		if meta.NumPending == 0 {
			return version, nil
		}
	}
}

func (s *EventStore) publishOne(ctx context.Context, agg event.AggregateRef, expectedVersion uint64, evt event.Event) (uint64, error) {
	data, err := marshalEvent(evt)
	if err != nil {
		return 0, err
	}
	subject := event.EventSubject(evt).String()
	filter := AggregateMessageFilter(agg.AggregateScope(), agg.AggregateType(), agg.AggregateID())
	msg := gonats.NewMsg(subject)
	msg.Data = data
	ack, err := s.js.PublishMsg(ctx, msg,
		jetstream.WithExpectStream(s.streamFor(agg.AggregateScope(), agg.AggregateType())),
		jetstream.WithExpectLastSequenceForSubject(expectedVersion, filter),
	)
	if err != nil {
		return 0, publishError(err, expectedVersion)
	}
	return ack.Sequence, nil
}

func (s *EventStore) publishBatch(ctx context.Context, agg event.AggregateRef, expectedVersion uint64, events []event.Event) (uint64, error) {
	batchID := nuid.Next()
	filter := AggregateMessageFilter(agg.AggregateScope(), agg.AggregateType(), agg.AggregateID())
	stream := s.streamFor(agg.AggregateScope(), agg.AggregateType())

	var version uint64
	for i, evt := range events {
		subj := event.EventSubject(evt)
		data, err := marshalEvent(evt)
		if err != nil {
			return 0, err
		}
		msg := gonats.NewMsg(subj.String())
		msg.Data = data
		msg.Header.Set(batchIDHeader, batchID)
		msg.Header.Set(batchSeqHeader, strconv.Itoa(i+1))
		if i == 0 {
			msg.Header.Set(jetstream.ExpectedStreamHeader, stream)
			msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, filter)
			msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(expectedVersion, 10))
		}
		if i == len(events)-1 {
			msg.Header.Set(batchCommitHeader, "1")
		}
		resp, err := s.nc.RequestMsgWithContext(ctx, msg)
		if err != nil {
			return 0, fmt.Errorf("publish atomic batch: %w", err)
		}
		if len(resp.Data) == 0 {
			continue
		}
		ack, err := parsePubAck(resp.Data, expectedVersion)
		if err != nil {
			return 0, err
		}
		if ack != nil {
			version = ack.Sequence
		}
	}
	return version, nil
}

func (s *EventStore) decodeEvent(registry event.DecoderRegistry, subject string, data []byte, timestamp time.Time) (event.Event, error) {
	subj, ok := ParseSubject(subject)
	if !ok {
		return nil, fmt.Errorf("invalid event subject %q", subject)
	}
	evt, err := registry.NewEvent(subj.Kind, subj.Scope, subj.AggregateType, subj.Name)
	if err != nil {
		return nil, err
	}
	if codec, ok := evt.(Codec); ok {
		if err := codec.NATSUnmarshal(subject, data, timestamp); err != nil {
			return nil, fmt.Errorf("decode event %s: %w", subject, err)
		}
		return evt, nil
	}
	if err := json.Unmarshal(data, evt); err != nil {
		return nil, fmt.Errorf("decode event %s: %w", subject, err)
	}
	return evt, nil
}

func (s *EventStore) applyMessage(
	registry event.DecoderRegistry,
	subject string,
	data []byte,
	timestamp time.Time,
	agg event.ESAggregate,
	snapshotSuffix string,
	snapshottable bool,
) error {
	if !snapshottable && strings.HasSuffix(subject, snapshotSuffix) {
		if codec, ok := agg.(Codec); ok {
			if err := codec.NATSUnmarshal(subject, data, timestamp); err != nil {
				return fmt.Errorf("decode snapshot %s: %w", subject, err)
			}
			return nil
		}
		if err := json.Unmarshal(data, agg); err != nil {
			return fmt.Errorf("decode snapshot %s: %w", subject, err)
		}
		return nil
	}
	evt, err := s.decodeEvent(registry, subject, data, timestamp)
	if err != nil {
		return err
	}
	agg.Apply(evt)
	return nil
}

func (s *EventStore) snapshotForSave(ctx context.Context, agg event.AggregateRef, expectedVersion uint64, events []event.Event) (event.Event, error) {
	if s.snapshotEvery == 0 {
		return nil, nil
	}
	snapshotAgg, ok := agg.(event.ESAggregate)
	if !ok {
		return nil, nil
	}
	streamName := s.streamFor(agg.AggregateScope(), agg.AggregateType())
	stream, err := s.js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("get stream %s: %w", streamName, err)
	}
	lastSnapshotVersion, err := s.lastSnapshotVersion(ctx, stream, snapshotAgg)
	if err != nil {
		return nil, fmt.Errorf("get latest snapshot: %w", err)
	}
	nextVersion := expectedVersion + uint64(len(events))
	if lastSnapshotVersion != 0 && nextVersion-lastSnapshotVersion < s.snapshotEvery {
		return nil, nil
	}
	for _, evt := range events {
		snapshotAgg.Apply(evt)
	}
	return s.buildSnapshotEvent(snapshotAgg)
}

func (s *EventStore) lastSnapshotVersion(ctx context.Context, stream jetstream.Stream, agg event.ESAggregate) (uint64, error) {
	msg, err := stream.GetLastMsgForSubject(ctx, SnapshotSubject(agg))
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return msg.Sequence, nil
}

func (s *EventStore) buildSnapshotEvent(agg event.ESAggregate) (event.Event, error) {
	if snapshottable, ok := agg.(event.Snapshottable); ok {
		evt, err := snapshottable.TakeSnapshot()
		if err != nil {
			return nil, fmt.Errorf("take snapshot: %w", err)
		}
		_, snapshotType := agg.EventTypes()
		if snapshotType == nil {
			return nil, fmt.Errorf("snapshot type is required")
		}
		if evt.EventName() != snapshotType.EventName() {
			return nil, fmt.Errorf("snapshot event name %q does not match registered name %q", evt.EventName(), snapshotType.EventName())
		}
		return evt, nil
	}
	_, snapshotType := agg.EventTypes()
	return implicitSnapshotEvent{AggregateRef: agg, name: snapshotEventName(snapshotType)}, nil
}

func (s *EventStore) streamFor(scope, aggregateType string) string {
	return StreamName(s.streamPattern, scope, aggregateType)
}

func snapshotEventName(snapshotType event.SnapshotEvent) string {
	if snapshotType == nil || snapshotType.EventName() == "" {
		return event.DefaultSnapshotEventName
	}
	return snapshotType.EventName()
}

type implicitSnapshotEvent struct {
	event.AggregateRef
	name string
}

func (e implicitSnapshotEvent) EventName() string { return e.name }

func marshalEvent(evt event.Event) ([]byte, error) {
	if codec, ok := evt.(Codec); ok {
		data, err := codec.NATSMarshal()
		if err != nil {
			return nil, fmt.Errorf("marshal event %s: %w", event.EventSubject(evt), err)
		}
		return data, nil
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal event %s: %w", event.EventSubject(evt), err)
	}
	return data, nil
}

func eventRegistryFor(agg event.ESAggregate) (event.DecoderRegistry, error) {
	registry := event.NewTypeRegistry()
	events, snapshotType := agg.EventTypes()
	for i, evt := range events {
		if err := registry.RegisterMessageType(evt); err != nil {
			return nil, fmt.Errorf("register event type %d: %w", i, err)
		}
	}
	if snapshotType != nil {
		if err := registry.RegisterMessageType(snapshotType); err != nil {
			return nil, fmt.Errorf("register snapshot type: %w", err)
		}
	}
	return registry, nil
}

func validateEvents(agg event.AggregateRef, events []event.Event) error {
	for i, evt := range events {
		if evt.AggregateScope() != agg.AggregateScope() ||
			evt.AggregateType() != agg.AggregateType() ||
			evt.AggregateID() != agg.AggregateID() {
			return fmt.Errorf("event %d does not belong to aggregate %s.%s.%s", i,
				agg.AggregateScope(), agg.AggregateType(), agg.AggregateID())
		}
		if err := event.EventSubject(evt).Validate(); err != nil {
			return err
		}
	}
	return nil
}

func publishError(err error, expected uint64) error {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiPublishError(apiErr.ErrorCode, apiErr.Description, expected)
	}
	return fmt.Errorf("publish event: %w", err)
}

type pubAckResponse struct {
	Stream   string    `json:"stream"`
	Sequence uint64    `json:"seq"`
	Error    *apiError `json:"error,omitempty"`
}

type apiError struct {
	Code        int    `json:"code"`
	ErrorCode   int    `json:"err_code"`
	Description string `json:"description"`
}

func parsePubAck(data []byte, expected uint64) (*pubAckResponse, error) {
	var ack pubAckResponse
	if err := json.Unmarshal(data, &ack); err != nil {
		return nil, fmt.Errorf("parse publish ack: %w", err)
	}
	if ack.Error == nil {
		return &ack, nil
	}
	return nil, apiPublishError(jetstream.ErrorCode(ack.Error.ErrorCode), ack.Error.Description, expected)
}

func apiPublishError(code jetstream.ErrorCode, description string, expected uint64) error {
	if code == jetstream.JSErrCodeStreamWrongLastSequence || strings.Contains(description, "wrong last sequence") {
		actual := uint64(0)
		_, _ = fmt.Sscanf(description, "wrong last sequence: %d", &actual)
		return &event.ConflictError{Expected: expected, Actual: actual}
	}
	if description == "" {
		description = fmt.Sprintf("jetstream publish failed with code %d", code)
	}
	return fmt.Errorf("publish event: %s", description)
}
