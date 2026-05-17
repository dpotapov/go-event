package natstest

import (
	"context"
	"fmt"
	"testing"
	"time"

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

func (s *Server) EnsureEventStream(t testing.TB, scope, aggregateType string, opts ...goeventnats.StreamOption) jetstream.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := goeventnats.EnsureEventStream(ctx, s.JetStream, scope, aggregateType, opts...)
	if err != nil {
		t.Fatalf("ensure event stream: %v", err)
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
