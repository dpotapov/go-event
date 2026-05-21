package nats_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dpotapov/go-event"
	goeventnats "github.com/dpotapov/go-event/nats"
	"github.com/dpotapov/go-event/testkit/natstest"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

const (
	benchAggregateScope = "test"
	benchAggregateType  = "account"
)

func init() {
	testing.Init()
	// ~30s for `go test -bench=. ./...` on a laptop (400ms benchtime); override with `-benchtime`.
	if f := flag.Lookup("test.benchtime"); f != nil && f.Value.String() == "1s" {
		_ = f.Value.Set("400ms")
	}
}

var benchRunID atomic.Uint64

func benchNextRunID() uint64 {
	return benchRunID.Add(1)
}

type benchSeedCache struct {
	once  sync.Once
	aggID string
	err   error
}

var benchSeedRegistry sync.Map

func benchSeedOnce(key string, seed func(aggID string) error) (string, error) {
	v, _ := benchSeedRegistry.LoadOrStore(key, &benchSeedCache{})
	cache := v.(*benchSeedCache)
	cache.once.Do(func() {
		cache.aggID = fmt.Sprintf("bench-%s-%d", key, benchNextRunID())
		cache.err = seed(cache.aggID)
	})
	return cache.aggID, cache.err
}

func benchStreamName() string {
	return goeventnats.StreamName(goeventnats.DefaultStreamPattern, benchAggregateScope, benchAggregateType)
}

func benchFilter(aggID string) string {
	return goeventnats.AggregateMessageFilter(benchAggregateScope, benchAggregateType, aggID)
}

func benchTotalEvents() int {
	if testing.Short() {
		return 50
	}
	return 500
}

func benchTailCases(total int) []int {
	if testing.Short() {
		return []int{0, 10}
	}
	// Sparse points still show snapshot vs tail replay trade-off.
	return []int{0, 10, 100, total - 1}
}

func benchContentionAggregates() int {
	if testing.Short() {
		return 5
	}
	return 20
}

func benchContentionEventsPerAggregate() int {
	if testing.Short() {
		return 5
	}
	return 10
}

func benchSeedAccountEvents(ctx context.Context, js jetstream.JetStream, aggID string, count int) error {
	if count < 1 {
		return nil
	}
	stream := benchStreamName()
	for i := 0; i < count; i++ {
		subject, data, err := benchAccountEventPayload(aggID, i)
		if err != nil {
			return err
		}
		if _, err := js.Publish(ctx, subject, data, jetstream.WithExpectStream(stream)); err != nil {
			return fmt.Errorf("publish event %d: %w", i, err)
		}
	}
	return nil
}

func benchAccountEventPayload(aggID string, index int) (string, []byte, error) {
	if index == 0 {
		evt := &accountCreated{AccountID: aggID, Name: "initial"}
		data, err := json.Marshal(evt)
		if err != nil {
			return "", nil, err
		}
		return event.EventSubject(evt).String(), data, nil
	}
	evt := &accountRenamed{AccountID: aggID, Name: fmt.Sprintf("rename-%d", index)}
	data, err := json.Marshal(evt)
	if err != nil {
		return "", nil, err
	}
	return event.EventSubject(evt).String(), data, nil
}

func benchAccountNameAfterEvents(count int) string {
	if count < 1 {
		return ""
	}
	if count == 1 {
		return "initial"
	}
	return fmt.Sprintf("rename-%d", count-1)
}

func benchPublishAccountSnapshot(ctx context.Context, js jetstream.JetStream, aggID string, eventsApplied int) error {
	snap := &accountSnapshot{AccountID: aggID, Name: benchAccountNameAfterEvents(eventsApplied)}
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	subject := goeventnats.SnapshotSubject(&snapshottedAccount{ID: aggID})
	if _, err := js.Publish(ctx, subject, data, jetstream.WithExpectStream(benchStreamName())); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	return nil
}

func benchSeedAccountWithSnapshotTail(ctx context.Context, js jetstream.JetStream, aggID string, total, tail int) error {
	if tail < 0 || tail > total {
		return fmt.Errorf("invalid tail %d for total %d", tail, total)
	}
	head := total - tail
	if err := benchSeedAccountEvents(ctx, js, aggID, head); err != nil {
		return err
	}
	if head > 0 {
		if err := benchPublishAccountSnapshot(ctx, js, aggID, head); err != nil {
			return err
		}
	}
	for i := head; i < total; i++ {
		subject, data, err := benchAccountEventPayload(aggID, i)
		if err != nil {
			return err
		}
		if _, err := js.Publish(ctx, subject, data, jetstream.WithExpectStream(benchStreamName())); err != nil {
			return fmt.Errorf("publish tail event %d: %w", i, err)
		}
	}
	return nil
}

func benchRawReplayAccount(ctx context.Context, stream jetstream.Stream, aggID string, startSeq uint64) (*account, uint64, error) {
	if startSeq == 0 {
		startSeq = 1
	}
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{benchFilter(aggID)},
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       startSeq,
		InactiveThreshold: 5 * time.Second,
	})
	if err != nil {
		return nil, 0, err
	}
	info := consumer.CachedInfo()
	if info == nil || info.NumPending == 0 {
		return &account{ID: aggID}, 0, nil
	}
	iter, err := consumer.Messages()
	if err != nil {
		return nil, 0, err
	}
	defer iter.Stop()

	agg := &account{ID: aggID}
	var version uint64
	for {
		msg, err := iter.Next()
		if err != nil {
			return nil, 0, err
		}
		meta, err := msg.Metadata()
		if err != nil {
			return nil, 0, err
		}
		version = meta.Sequence.Stream
		if err := benchApplyRawAccountEvent(agg, msg.Subject(), msg.Data()); err != nil {
			return nil, 0, err
		}
		if meta.NumPending == 0 {
			return agg, version, nil
		}
	}
}

func benchApplyRawAccountEvent(agg *account, subject string, data []byte) error {
	parts := strings.Split(subject, ".")
	if len(parts) != 5 {
		return fmt.Errorf("invalid subject %q", subject)
	}
	switch parts[4] {
	case "created":
		var evt accountCreated
		if err := json.Unmarshal(data, &evt); err != nil {
			return err
		}
		agg.Apply(&evt)
	case "renamed":
		var evt accountRenamed
		if err := json.Unmarshal(data, &evt); err != nil {
			return err
		}
		agg.Apply(&evt)
	default:
		return fmt.Errorf("unexpected event name %q", parts[4])
	}
	return nil
}

func benchRawPublishRename(ctx context.Context, js jetstream.JetStream, aggID string, expectedVersion uint64, seq int) error {
	evt := &accountRenamed{AccountID: aggID, Name: fmt.Sprintf("bench-rename-%d", seq)}
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	msg := gonats.NewMsg(event.EventSubject(evt).String())
	msg.Data = data
	_, err = js.PublishMsg(ctx, msg,
		jetstream.WithExpectStream(benchStreamName()),
		jetstream.WithExpectLastSequenceForSubject(expectedVersion, benchFilter(aggID)),
	)
	return err
}

func benchContentionEvent(aggID string, seq int) event.Event {
	return &accountRenamed{AccountID: aggID, Name: fmt.Sprintf("contention-rename-%d", seq)}
}

func benchSaveContentionVersion(ctx context.Context, stream jetstream.Stream, aggID string) (uint64, error) {
	msg, err := stream.GetLastMsgForSubject(ctx, event.EventSubject(benchContentionEvent(aggID, 0)).String())
	if err != nil {
		return 0, err
	}
	return msg.Sequence, nil
}

type benchSaveContentionStats struct {
	saveNanos    atomic.Int64
	versionNanos atomic.Int64
}

func benchSaveContentionAggregate(ctx context.Context, store *goeventnats.EventStore, stream jetstream.Stream, aggID string, eventsPerAggregate int, stats *benchSaveContentionStats) error {
	agg := &account{ID: aggID}
	var version uint64
	for i := 0; i < eventsPerAggregate; i++ {
		started := time.Now()
		err := store.Save(ctx, agg, version, benchContentionEvent(aggID, i))
		stats.saveNanos.Add(time.Since(started).Nanoseconds())
		if err != nil {
			return fmt.Errorf("save aggregate %s event %d: %w", aggID, i, err)
		}
		if i == eventsPerAggregate-1 {
			return nil
		}
		started = time.Now()
		nextVersion, err := benchSaveContentionVersion(ctx, stream, aggID)
		stats.versionNanos.Add(time.Since(started).Nanoseconds())
		if err != nil {
			return fmt.Errorf("get aggregate %s version after event %d: %w", aggID, i, err)
		}
		version = nextVersion
	}
	return nil
}

func benchSaveContentionConcurrent(ctx context.Context, store *goeventnats.EventStore, stream jetstream.Stream, prefix string, aggregates, eventsPerAggregate int, stats *benchSaveContentionStats) error {
	errs := make(chan error, aggregates)
	var wg sync.WaitGroup
	wg.Add(aggregates)
	for i := 0; i < aggregates; i++ {
		aggID := fmt.Sprintf("%s-agg-%03d", prefix, i)
		go func() {
			defer wg.Done()
			errs <- benchSaveContentionAggregate(ctx, store, stream, aggID, eventsPerAggregate, stats)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// BenchmarkEventStore runs JetStream event-store benchmarks on one embedded NATS server.
// Default -benchtime (~1s) targets ~30s total for this benchmark tree plus subscription benches.
func BenchmarkEventStore(b *testing.B) {
	ctx := b.Context()
	env := natstest.Run(b)
	env.CreateEventStream(b, benchAggregateScope, benchAggregateType)

	store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{})
	require.NoError(b, err)

	stream, err := env.JetStream.Stream(ctx, benchStreamName())
	require.NoError(b, err)

	// SaveContention last: it appends many unrelated aggregates to the shared stream.
	b.Run("SaveAppend", func(b *testing.B) {
		benchEventStoreSaveAppend(b, ctx, env, store, stream)
	})
	b.Run("LoadReplay", func(b *testing.B) {
		benchEventStoreLoadReplay(b, ctx, env, store, stream)
	})
	b.Run("LoadSnapshotTail", func(b *testing.B) {
		benchEventStoreLoadSnapshotTail(b, ctx, env, store)
	})
	b.Run("SaveSnapshotTax", func(b *testing.B) {
		benchEventStoreSaveSnapshotTax(b, ctx, env)
	})
	b.Run("SaveContention", func(b *testing.B) {
		benchEventStoreSaveContention(b, ctx, store, stream)
	})
}

func benchEventStoreSaveAppend(b *testing.B, ctx context.Context, env *natstest.Server, store *goeventnats.EventStore, stream jetstream.Stream) {
	for _, impl := range []string{"goevent", "raw"} {
		b.Run(impl, func(b *testing.B) {
			aggID := fmt.Sprintf("save-append-%s-%d", impl, benchNextRunID())
			require.NoError(b, benchSeedAccountEvents(ctx, env.JetStream, aggID, 1))

			var version uint64
			if impl == "goevent" {
				agg := &account{ID: aggID}
				var err error
				version, err = store.Load(ctx, agg)
				require.NoError(b, err)
			} else {
				var err error
				_, version, err = benchRawReplayAccount(ctx, stream, aggID, 1)
				require.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if impl == "goevent" {
					agg := &account{ID: aggID}
					if err := store.Save(ctx, agg, version, &accountRenamed{
						AccountID: aggID,
						Name:      fmt.Sprintf("bench-rename-%d", i),
					}); err != nil {
						b.Fatal(err)
					}
				} else if err := benchRawPublishRename(ctx, env.JetStream, aggID, version, i); err != nil {
					b.Fatal(err)
				}
				version++
			}
		})
	}
}

func benchEventStoreSaveContention(b *testing.B, ctx context.Context, store *goeventnats.EventStore, stream jetstream.Stream) {
	aggregates := benchContentionAggregates()
	eventsPerAggregate := benchContentionEventsPerAggregate()

	cases := []struct {
		label string
		run   func(context.Context, *goeventnats.EventStore, jetstream.Stream, string, int, int, *benchSaveContentionStats) error
	}{
		{label: "concurrent", run: benchSaveContentionConcurrent},
	}
	for _, tc := range cases {
		b.Run(tc.label, func(b *testing.B) {
			stats := &benchSaveContentionStats{}
			b.ResetTimer()
			started := time.Now()
			runID := benchNextRunID()
			for i := 0; i < b.N; i++ {
				prefix := fmt.Sprintf("contention-%s-%d-%d", tc.label, runID, i)
				if err := tc.run(ctx, store, stream, prefix, aggregates, eventsPerAggregate, stats); err != nil {
					b.Fatal(err)
				}
			}
			elapsed := time.Since(started)
			b.StopTimer()
			totalSaves := b.N * aggregates * eventsPerAggregate
			totalVersionReads := b.N * aggregates * (eventsPerAggregate - 1)
			b.ReportMetric(float64(aggregates), "aggregates/op")
			b.ReportMetric(float64(eventsPerAggregate), "events/aggregate")
			b.ReportMetric(float64(aggregates*eventsPerAggregate), "saves/workload")
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(totalSaves), "wall_ns/save")
			b.ReportMetric(float64(stats.saveNanos.Load())/float64(totalSaves), "save_ns/call")
			if totalVersionReads > 0 {
				b.ReportMetric(float64(stats.versionNanos.Load())/float64(totalVersionReads), "version_ns/read")
			}
		})
	}
}

func benchEventStoreLoadReplay(b *testing.B, ctx context.Context, env *natstest.Server, store *goeventnats.EventStore, stream jetstream.Stream) {
	total := benchTotalEvents()
	for _, impl := range []string{"goevent", "raw"} {
		b.Run(fmt.Sprintf("events=%d/%s", total, impl), func(b *testing.B) {
			aggID, err := benchSeedOnce(fmt.Sprintf("load-replay-%s-%d", impl, total), func(aggID string) error {
				return benchSeedAccountEvents(ctx, env.JetStream, aggID, total)
			})
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if impl == "goevent" {
					loaded := &account{ID: aggID}
					if _, err := store.Load(ctx, loaded); err != nil {
						b.Fatal(err)
					}
					continue
				}
				if _, _, err := benchRawReplayAccount(ctx, stream, aggID, 1); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(total), "events/rep")
		})
	}
}

func benchEventStoreLoadSnapshotTail(b *testing.B, ctx context.Context, env *natstest.Server, store *goeventnats.EventStore) {
	total := benchTotalEvents()
	for _, tail := range benchTailCases(total) {
		b.Run(fmt.Sprintf("total=%d/tail=%d", total, tail), func(b *testing.B) {
			aggID, err := benchSeedOnce(fmt.Sprintf("load-tail-%d-%d", total, tail), func(aggID string) error {
				return benchSeedAccountWithSnapshotTail(ctx, env.JetStream, aggID, total, tail)
			})
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				loaded := &snapshottedAccount{ID: aggID}
				if _, err := store.Load(ctx, loaded); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(tail), "tail/rep")
			b.ReportMetric(float64(total), "total/rep")
		})
	}
}

func benchEventStoreSaveSnapshotTax(b *testing.B, ctx context.Context, env *natstest.Server) {
	cases := []struct {
		label         string
		snapshotEvery uint64
		seedEvents    int
	}{
		{label: "off", snapshotEvery: 0, seedEvents: 1},
		{label: "not_due", snapshotEvery: 1_000_000, seedEvents: 50},
		{label: "due", snapshotEvery: 10, seedEvents: 9},
	}
	for _, tc := range cases {
		b.Run(tc.label, func(b *testing.B) {
			store, err := goeventnats.NewEventStore(env.Conn, goeventnats.EventStoreConfig{
				SnapshotEvery: tc.snapshotEvery,
			})
			require.NoError(b, err)

			b.ReportMetric(float64(tc.seedEvents), "seed/rep")
			if tc.snapshotEvery > 0 {
				b.ReportMetric(float64(tc.snapshotEvery), "snapshot_every/rep")
			}

			if tc.label == "due" {
				benchSnapshotTaxDue(b, ctx, env.JetStream, store, tc.seedEvents)
				return
			}

			aggID, err := benchSeedOnce("snapshot-tax-"+tc.label, func(aggID string) error {
				agg := &snapshottedAccount{ID: aggID}
				if tc.seedEvents == 1 {
					return store.Save(ctx, agg, 0, &accountCreated{AccountID: aggID, Name: "initial"})
				}
				return benchSeedAccountEvents(ctx, env.JetStream, aggID, tc.seedEvents)
			})
			require.NoError(b, err)
			agg := &snapshottedAccount{ID: aggID}
			version, err := store.Load(ctx, &snapshottedAccount{ID: aggID})
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.Save(ctx, agg, version, &accountRenamed{
					AccountID: aggID,
					Name:      fmt.Sprintf("tax-rename-%d", i),
				}); err != nil {
					b.Fatal(err)
				}
				version++
			}
		})
	}
}

type benchSnapshotTaxSlot struct {
	aggID   string
	version uint64
}

func benchSnapshotTaxDue(b *testing.B, ctx context.Context, js jetstream.JetStream, store *goeventnats.EventStore, seedEvents int) {
	const pool = 8
	slots := make([]benchSnapshotTaxSlot, pool)
	refresh := func() {
		for j := range slots {
			aggID := fmt.Sprintf("snapshot-tax-due-%d-%d", benchNextRunID(), j)
			require.NoError(b, benchSeedAccountEvents(ctx, js, aggID, seedEvents))
			version, err := store.Load(ctx, &snapshottedAccount{ID: aggID})
			require.NoError(b, err)
			slots[j] = benchSnapshotTaxSlot{aggID: aggID, version: version}
		}
	}
	var refreshOnce sync.Once
	refreshOnce.Do(refresh)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%pool == 0 {
			b.StopTimer()
			refresh()
			b.StartTimer()
		}
		slot := slots[i%pool]
		agg := &snapshottedAccount{ID: slot.aggID}
		if err := store.Save(ctx, agg, slot.version, &accountRenamed{
			AccountID: slot.aggID,
			Name:      fmt.Sprintf("tax-due-rename-%d", i),
		}); err != nil {
			b.Fatal(err)
		}
	}
}
