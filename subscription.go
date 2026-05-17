package event

import (
	"context"
	"fmt"
)

type Subscription interface {
	Stop(context.Context) error
}

type SubscriptionFunc func(context.Context) error

func (f SubscriptionFunc) Stop(ctx context.Context) error {
	if f == nil {
		return nil
	}
	return f(ctx)
}

type EventSubscriber interface {
	SubscribeEvents(context.Context, EventHandler, EventSubscriptionConfig) (Subscription, error)
}

type CommandSubscriber interface {
	SubscribeCommand(context.Context, CommandHandler, CommandSubscriptionConfig) (Subscription, error)
	SubscribeQuery(context.Context, QueryHandler, CommandSubscriptionConfig) (Subscription, error)
}

type Dispatcher interface {
	DispatchCommand(context.Context, Command) error
	DispatchQuery(context.Context, Command, any) error
}

type EventHandler interface {
	HandleEvent(context.Context, Event, []byte) error
}

type EventHandlerFunc func(context.Context, Event, []byte) error

func (f EventHandlerFunc) HandleEvent(ctx context.Context, evt Event, cursor []byte) error {
	return f(ctx, evt, cursor)
}

type CommandHandler interface {
	HandleCommand(context.Context, Command) error
}

type CommandHandlerFunc func(context.Context, Command) error

func (f CommandHandlerFunc) HandleCommand(ctx context.Context, cmd Command) error {
	return f(ctx, cmd)
}

type QueryHandler interface {
	HandleQuery(context.Context, Command) (any, error)
}

type QueryHandlerFunc func(context.Context, Command) (any, error)

func (f QueryHandlerFunc) HandleQuery(ctx context.Context, query Command) (any, error) {
	return f(ctx, query)
}

type CursorBootPolicy int

const (
	CursorBootNew CursorBootPolicy = iota
	CursorBootAll
)

type CursorStore interface {
	LoadCursor(context.Context, string) ([]byte, error)
	SaveCursor(context.Context, string, []byte) error
}

type EventSubscriptionConfig struct {
	AggregateScope string
	AggregateTypes []string
	AggregateIDs   []string
	EventNames     []string

	Queue        string
	ConsumerName string
	MaxRetries   int

	Cursor           []byte
	CursorBootPolicy CursorBootPolicy
	CaughtUp         func()
	OnError          func(*SubscriptionError) bool
}

func (c EventSubscriptionConfig) Validate() error {
	if len(c.AggregateTypes) > 0 && c.AggregateScope == "" {
		return fmt.Errorf("invalid event subscription: aggregate scope is required when aggregate types are set")
	}
	for i, v := range c.AggregateTypes {
		if err := ValidateToken(fmt.Sprintf("aggregate type[%d]", i), v); err != nil {
			return err
		}
	}
	if len(c.AggregateIDs) > 0 && len(c.AggregateTypes) != 1 {
		return fmt.Errorf("invalid event subscription: aggregate IDs require exactly one aggregate type")
	}
	for i, v := range c.AggregateIDs {
		if err := ValidateToken(fmt.Sprintf("aggregate id[%d]", i), v); err != nil {
			return err
		}
	}
	if len(c.EventNames) > 0 && len(c.AggregateTypes) != 1 {
		return fmt.Errorf("invalid event subscription: event names require exactly one aggregate type")
	}
	for i, v := range c.EventNames {
		if err := ValidateToken(fmt.Sprintf("event name[%d]", i), v); err != nil {
			return err
		}
	}
	if c.AggregateScope != "" {
		if err := ValidateToken("scope", c.AggregateScope); err != nil {
			return err
		}
	}
	return nil
}

type CommandSubscriptionConfig struct {
	AggregateScope string
	AggregateTypes []string
	AggregateIDs   []string
	CommandNames   []string
	Queue          string
}

func (c CommandSubscriptionConfig) Validate() error {
	if len(c.AggregateTypes) > 0 && c.AggregateScope == "" {
		return fmt.Errorf("invalid command subscription: aggregate scope is required when aggregate types are set")
	}
	for i, v := range c.AggregateTypes {
		if err := ValidateToken(fmt.Sprintf("aggregate type[%d]", i), v); err != nil {
			return err
		}
	}
	if len(c.AggregateIDs) > 0 && len(c.AggregateTypes) != 1 {
		return fmt.Errorf("invalid command subscription: aggregate IDs require exactly one aggregate type")
	}
	for i, v := range c.AggregateIDs {
		if err := ValidateToken(fmt.Sprintf("aggregate id[%d]", i), v); err != nil {
			return err
		}
	}
	if len(c.CommandNames) > 0 && len(c.AggregateTypes) != 1 {
		return fmt.Errorf("invalid command subscription: command names require exactly one aggregate type")
	}
	for i, v := range c.CommandNames {
		if err := ValidateToken(fmt.Sprintf("command name[%d]", i), v); err != nil {
			return err
		}
	}
	if c.AggregateScope != "" {
		if err := ValidateToken("scope", c.AggregateScope); err != nil {
			return err
		}
	}
	return nil
}

func Ask[R any](ctx context.Context, dispatcher Dispatcher, query Query[R]) (R, error) {
	var result R
	err := dispatcher.DispatchQuery(ctx, query, &result)
	return result, err
}
