package natstest

import (
	"context"
	"fmt"
	"testing"
	"time"

	goevent "github.com/dpotapov/go-event"
	goeventnats "github.com/dpotapov/go-event/nats"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Server struct {
	Server    *server.Server
	Conn      *gonats.Conn
	JetStream jetstream.JetStream
	URL       string
}

type Option func(*server.Options)

func WithServerOptions(fn func(*server.Options)) Option {
	return func(opts *server.Options) {
		if fn != nil {
			fn(opts)
		}
	}
}

func Run(t testing.TB, opts ...Option) *Server {
	t.Helper()
	options := natsserver.DefaultTestOptions
	options.Port = -1
	options.JetStream = true
	options.StoreDir = t.TempDir()
	for _, opt := range opts {
		opt(&options)
	}
	ns := natsserver.RunServer(&options)
	if ns == nil {
		t.Fatal("start nats server")
	}
	nc, err := gonats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		ns.WaitForShutdown()
		t.Fatalf("connect nats: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
		t.Fatalf("create jetstream: %v", err)
	}
	out := &Server{Server: ns, Conn: nc, JetStream: js, URL: ns.ClientURL()}
	t.Cleanup(out.Close)
	return out
}

func (s *Server) Close() {
	if s.Conn != nil {
		s.Conn.Close()
	}
	if s.Server != nil {
		s.Server.Shutdown()
		s.Server.WaitForShutdown()
	}
}

type streamInfo struct {
	scope         string
	kind          string
	aggregateType string
}

func (s streamInfo) AggregateScope() string { return s.scope }
func (s streamInfo) AggregateType() string  { return s.aggregateType }
func (s streamInfo) MessageKind() string    { return s.kind }

func (s *Server) CreateEventStream(t testing.TB, scope, aggregateType string) jetstream.Stream {
	t.Helper()
	return s.CreateMessageStream(t, scope, string(goevent.KindEvent), aggregateType)
}

func (s *Server) CreateMessageStream(t testing.TB, scope, kind, aggregateType string) jetstream.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := jetstream.StreamConfig{
		Name:               goeventnats.StreamName(goeventnats.DefaultStreamPattern, scope, aggregateType),
		Storage:            jetstream.MemoryStorage,
		Retention:          jetstream.LimitsPolicy,
		AllowDirect:        true,
		AllowAtomicPublish: true,
	}
	goeventnats.SetStreamSubjects(&cfg, streamInfo{scope: scope, kind: kind, aggregateType: aggregateType})
	stream, err := s.JetStream.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		t.Fatalf("ensure message stream: %v", err)
	}
	return stream
}

func (s *Server) ConsumerNames(t testing.TB, streamName string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := s.JetStream.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("get stream %s: %v", streamName, err)
	}
	names := make([]string, 0)
	lister := stream.ConsumerNames(ctx)
	for name := range lister.Name() {
		names = append(names, name)
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("list consumers for %s: %v", streamName, err)
	}
	return names
}

func (s *Server) String() string {
	return fmt.Sprintf("NATS(%s)", s.URL)
}
