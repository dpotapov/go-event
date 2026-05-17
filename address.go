package event

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

type MessageClass string

const (
	ClassEvent   MessageClass = "event"
	ClassCommand MessageClass = "command"
	ClassQuery   MessageClass = "query"
)

var ErrInvalidName = errors.New("invalid message name")

// Address is the transport-neutral location of a command, query, or event.
type Address struct {
	Scope         string
	Class         MessageClass
	AggregateType string
	AggregateID   string
	Name          string
}

func EventAddress(evt Event) Address {
	return Address{
		Scope:         evt.AggregateScope(),
		Class:         ClassEvent,
		AggregateType: evt.AggregateType(),
		AggregateID:   evt.AggregateID(),
		Name:          evt.EventName(),
	}
}

func CommandAddress(cmd Command) Address {
	return Address{
		Scope:         cmd.AggregateScope(),
		Class:         ClassCommand,
		AggregateType: cmd.AggregateType(),
		AggregateID:   cmd.AggregateID(),
		Name:          cmd.CommandName(),
	}
}

func QueryAddress(query Command) Address {
	return Address{
		Scope:         query.AggregateScope(),
		Class:         ClassQuery,
		AggregateType: query.AggregateType(),
		AggregateID:   query.AggregateID(),
		Name:          query.CommandName(),
	}
}

func (a Address) Subject() string {
	return strings.Join([]string{
		a.Scope,
		string(a.Class),
		a.AggregateType,
		a.AggregateID,
		a.Name,
	}, ".")
}

func (a Address) Validate() error {
	if err := ValidateToken("scope", a.Scope); err != nil {
		return err
	}
	switch a.Class {
	case ClassEvent, ClassCommand, ClassQuery:
	default:
		return fmt.Errorf("%w: class %q", ErrInvalidName, a.Class)
	}
	if err := ValidateToken("aggregate type", a.AggregateType); err != nil {
		return err
	}
	if err := ValidateToken("aggregate id", a.AggregateID); err != nil {
		return err
	}
	if err := ValidateToken("name", a.Name); err != nil {
		return err
	}
	return nil
}

func ParseSubject(subject string) (Address, bool) {
	parts := strings.Split(subject, ".")
	if len(parts) != 5 {
		return Address{}, false
	}
	addr := Address{
		Scope:         parts[0],
		Class:         MessageClass(parts[1]),
		AggregateType: parts[2],
		AggregateID:   parts[3],
		Name:          parts[4],
	}
	return addr, addr.Validate() == nil
}

// ValidateToken checks one concrete subject token. Wildcards are intentionally
// rejected here; subscription filters use separate helpers in transport packages.
func ValidateToken(label, token string) error {
	if token == "" {
		return fmt.Errorf("%w: %s is empty", ErrInvalidName, label)
	}
	for _, r := range token {
		if r == '.' || r == '*' || r == '>' || r == '/' || r == '\\' || unicode.IsSpace(r) || !unicode.IsPrint(r) {
			return fmt.Errorf("%w: %s %q contains reserved character %q", ErrInvalidName, label, token, r)
		}
	}
	return nil
}
