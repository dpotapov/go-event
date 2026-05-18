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

	goevent "github.com/dpotapov/go-event"
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
}

type EventStore struct {
	nc            *gonats.Conn
	js            jetstream.JetStream
	streamPattern string
	logger        *slog.Logger
}

var _ goevent.Store = (*EventStore)(nil)

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
		logger:        logger,
	}, nil
}

func (s *EventStore) Save(ctx context.Context, agg goevent.AggregateRef, expectedVersion uint64, events ...goevent.Event) error {
	if len(events) == 0 {
		return nil
	}
	if err := validateEvents(agg, events); err != nil {
		return err
	}
	if len(events) == 1 {
		return s.publishOne(ctx, agg, expectedVersion, events[0])
	}
	return s.publishBatch(ctx, agg, expectedVersion, events)
}

func (s *EventStore) Load(ctx context.Context, agg goevent.ESAggregate) (uint64, error) {
	registry, err := eventRegistryFor(agg)
	if err != nil {
		return 0, err
	}
	streamName := s.streamFor(agg.AggregateScope(), agg.AggregateType())
	filter := AggregateMessageFilter(agg.AggregateScope(), agg.AggregateType(), agg.AggregateID())
	stream, err := s.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("get stream %s: %w", streamName, err)
	}
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{filter},
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: 5 * time.Second,
	})
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
		evt, err := s.decodeEvent(registry, msg.Subject(), msg.Data(), meta.Timestamp)
		if err != nil {
			return 0, err
		}
		agg.Apply(evt)
		if meta.NumPending == 0 {
			return version, nil
		}
	}
}

func (s *EventStore) publishOne(ctx context.Context, agg goevent.AggregateRef, expectedVersion uint64, evt goevent.Event) error {
	data, err := marshalEvent(evt)
	if err != nil {
		return err
	}
	subject := goevent.EventSubject(evt).String()
	filter := AggregateMessageFilter(agg.AggregateScope(), agg.AggregateType(), agg.AggregateID())
	msg := gonats.NewMsg(subject)
	msg.Data = data
	_, err = s.js.PublishMsg(ctx, msg,
		jetstream.WithExpectStream(s.streamFor(agg.AggregateScope(), agg.AggregateType())),
		jetstream.WithExpectLastSequenceForSubject(expectedVersion, filter),
	)
	if err != nil {
		return publishError(err, expectedVersion)
	}
	return nil
}

func (s *EventStore) publishBatch(ctx context.Context, agg goevent.AggregateRef, expectedVersion uint64, events []goevent.Event) error {
	batchID := nuid.Next()
	filter := AggregateMessageFilter(agg.AggregateScope(), agg.AggregateType(), agg.AggregateID())
	stream := s.streamFor(agg.AggregateScope(), agg.AggregateType())

	for i, evt := range events {
		subj := goevent.EventSubject(evt)
		data, err := marshalEvent(evt)
		if err != nil {
			return err
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
			return fmt.Errorf("publish atomic batch: %w", err)
		}
		if len(resp.Data) == 0 {
			continue
		}
		if err := parsePubAck(resp.Data, expectedVersion); err != nil {
			return err
		}
	}
	return nil
}

func (s *EventStore) decodeEvent(registry goevent.DecoderRegistry, subject string, data []byte, timestamp time.Time) (goevent.Event, error) {
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

func (s *EventStore) streamFor(scope, aggregateType string) string {
	return StreamName(s.streamPattern, scope, aggregateType)
}

func marshalEvent(evt goevent.Event) ([]byte, error) {
	if codec, ok := evt.(Codec); ok {
		data, err := codec.NATSMarshal()
		if err != nil {
			return nil, fmt.Errorf("marshal event %s: %w", goevent.EventSubject(evt), err)
		}
		return data, nil
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal event %s: %w", goevent.EventSubject(evt), err)
	}
	return data, nil
}

func eventRegistryFor(agg goevent.ESAggregate) (goevent.DecoderRegistry, error) {
	registry := goevent.NewTypeRegistry()
	for i, evt := range agg.EventTypes() {
		if err := registry.RegisterMessageType(evt); err != nil {
			return nil, fmt.Errorf("register event type %d: %w", i, err)
		}
	}
	return registry, nil
}

func validateEvents(agg goevent.AggregateRef, events []goevent.Event) error {
	for i, evt := range events {
		if evt.AggregateScope() != agg.AggregateScope() ||
			evt.AggregateType() != agg.AggregateType() ||
			evt.AggregateID() != agg.AggregateID() {
			return fmt.Errorf("event %d does not belong to aggregate %s.%s.%s", i,
				agg.AggregateScope(), agg.AggregateType(), agg.AggregateID())
		}
		if err := goevent.EventSubject(evt).Validate(); err != nil {
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

func parsePubAck(data []byte, expected uint64) error {
	var ack pubAckResponse
	if err := json.Unmarshal(data, &ack); err != nil {
		return fmt.Errorf("parse publish ack: %w", err)
	}
	if ack.Error == nil {
		return nil
	}
	return apiPublishError(jetstream.ErrorCode(ack.Error.ErrorCode), ack.Error.Description, expected)
}

func apiPublishError(code jetstream.ErrorCode, description string, expected uint64) error {
	if code == jetstream.JSErrCodeStreamWrongLastSequence || strings.Contains(description, "wrong last sequence") {
		actual := uint64(0)
		_, _ = fmt.Sscanf(description, "wrong last sequence: %d", &actual)
		return &goevent.ConflictError{Expected: expected, Actual: actual}
	}
	if description == "" {
		description = fmt.Sprintf("jetstream publish failed with code %d", code)
	}
	return fmt.Errorf("publish event: %s", description)
}
