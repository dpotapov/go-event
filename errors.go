package event

import (
	"errors"
	"fmt"
)

var (
	ErrConflict          = errors.New("event version conflict")
	ErrNoResponders      = errors.New("no responders available")
	ErrInvalidResultType = errors.New("invalid query result type")
	ErrClosed            = errors.New("bus is closed")
)

type ConflictError struct {
	Expected uint64
	Actual   uint64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("event version conflict: expected %d, got %d", e.Expected, e.Actual)
}

func (e *ConflictError) Is(target error) bool {
	return target == ErrConflict
}

type UnknownEventError struct {
	Scope         string
	AggregateType string
	Name          string
}

func (e *UnknownEventError) Error() string {
	return fmt.Sprintf("unknown event type: %s.%s.%s", e.Scope, e.AggregateType, e.Name)
}

func (e *UnknownEventError) Is(target error) bool {
	_, ok := target.(*UnknownEventError)
	return ok
}

type UnknownCommandError struct {
	Scope         string
	AggregateType string
	Name          string
}

func (e *UnknownCommandError) Error() string {
	return fmt.Sprintf("unknown command type: %s.%s.%s", e.Scope, e.AggregateType, e.Name)
}

func (e *UnknownCommandError) Is(target error) bool {
	_, ok := target.(*UnknownCommandError)
	return ok
}

type SubscriptionError struct {
	Subject          Subject
	Queue            string
	NumDelivered     uint64
	RetriesExhausted bool
	Transport        bool
	Err              error
}

func (e *SubscriptionError) Error() string {
	if e == nil || e.Err == nil {
		return "subscription error"
	}
	return e.Err.Error()
}

func (e *SubscriptionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
