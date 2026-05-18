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

// Subject is the transport-neutral delivery subject of a command, query, or event.
type Subject struct {
	Scope         string
	Kind          MessageKind
	AggregateType string
	AggregateID   string
	Name          string
}

func EventSubject(evt Event) Subject {
	return Subject{
		Scope:         evt.AggregateScope(),
		Kind:          messageKind(evt, KindEvent),
		AggregateType: evt.AggregateType(),
		AggregateID:   evt.AggregateID(),
		Name:          evt.EventName(),
	}
}

func CommandSubject(cmd Command) Subject {
	return Subject{
		Scope:         cmd.AggregateScope(),
		Kind:          messageKind(cmd, KindCommand),
		AggregateType: cmd.AggregateType(),
		AggregateID:   cmd.AggregateID(),
		Name:          cmd.CommandName(),
	}
}

func QuerySubject(query Command) Subject {
	return Subject{
		Scope:         query.AggregateScope(),
		Kind:          messageKind(query, KindQuery),
		AggregateType: query.AggregateType(),
		AggregateID:   query.AggregateID(),
		Name:          query.CommandName(),
	}
}

func messageKind(msg any, defaultKind MessageKind) MessageKind {
	if provider, ok := msg.(MessageKindOverride); ok {
		if kind := provider.MessageKind(); kind != "" {
			return MessageKind(kind)
		}
	}
	return defaultKind
}

func (s Subject) String() string {
	return strings.Join([]string{
		s.Scope,
		string(s.Kind),
		s.AggregateType,
		s.AggregateID,
		s.Name,
	}, ".")
}

func (s Subject) Validate() error {
	if err := ValidateToken("scope", s.Scope); err != nil {
		return err
	}
	if err := ValidateToken("kind", string(s.Kind)); err != nil {
		return err
	}
	if err := ValidateToken("aggregate type", s.AggregateType); err != nil {
		return err
	}
	if err := ValidateToken("aggregate id", s.AggregateID); err != nil {
		return err
	}
	if err := ValidateToken("name", s.Name); err != nil {
		return err
	}
	return nil
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
