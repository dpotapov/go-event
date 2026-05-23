package event

import (
	"encoding/json"
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

// ResponseError represents a structured error received in a CQRS response.
// It contains the raw JSON payload which can be interpreted by higher layers.
type ResponseError json.RawMessage

// Error implements the error interface.
func (e ResponseError) Error() string {
	var obj struct {
		Detail  string `json:"detail"`
		Message string `json:"message"`
	}
	if json.Unmarshal(e, &obj) == nil {
		if obj.Detail != "" {
			return obj.Detail
		}
		if obj.Message != "" {
			return obj.Message
		}
	}
	return "operation failed"
}

// MarshalJSON implements json.Marshaler, allowing the error to be re-serialized.
func (e ResponseError) MarshalJSON() ([]byte, error) {
	return e, nil
}

// UnmarshalInto allows callers to extract the underlying JSON into a typed struct.
func (e ResponseError) UnmarshalInto(v any) error {
	return json.Unmarshal(e, v)
}
