package event

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

type MessageKind string

const (
	KindEvent   MessageKind = "event"
	KindCommand MessageKind = "command"
	KindQuery   MessageKind = "query"
)

var ErrInvalidName = errors.New("invalid message name")

// Address is the transport-neutral location of a command, query, or event.
type Address struct {
	Scope         string
	Kind          MessageKind
	AggregateType string
	AggregateID   string
	Name          string
}

func EventAddress(evt Event) Address {
	return Address{
		Scope:         evt.AggregateScope(),
		Kind:          KindEvent,
		AggregateType: evt.AggregateType(),
		AggregateID:   evt.AggregateID(),
		Name:          evt.EventName(),
	}
}

func CommandAddress(cmd Command) Address {
	return Address{
		Scope:         cmd.AggregateScope(),
		Kind:          KindCommand,
		AggregateType: cmd.AggregateType(),
		AggregateID:   cmd.AggregateID(),
		Name:          cmd.CommandName(),
	}
}

func QueryAddress(query Command) Address {
	return Address{
		Scope:         query.AggregateScope(),
		Kind:          KindQuery,
		AggregateType: query.AggregateType(),
		AggregateID:   query.AggregateID(),
		Name:          query.CommandName(),
	}
}

func (a Address) Subject() string {
	return strings.Join([]string{
		a.Scope,
		string(a.Kind),
		a.AggregateType,
		a.AggregateID,
		a.Name,
	}, ".")
}

func (a Address) Validate() error {
	if err := ValidateToken("scope", a.Scope); err != nil {
		return err
	}
	switch a.Kind {
	case KindEvent, KindCommand, KindQuery:
	default:
		return fmt.Errorf("%w: kind %q", ErrInvalidName, a.Kind)
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
		Kind:          MessageKind(parts[1]),
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
