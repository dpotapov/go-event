package event

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

type typeKey struct {
	scope string
	kind  MessageKind
	agg   string
	name  string
}

// TypeRegistry is the public typed-registration target implemented by Bus and
// transport adapters that need to decode stored or incoming messages.
type TypeRegistry interface {
	RegisterMessageType(any) error
}

type DecoderRegistry interface {
	TypeRegistry
	NewEvent(kind MessageKind, scope, aggregateType, name string) (Event, error)
	DecodeEvent(Subject, []byte) (Event, error)
	NewCommand(kind MessageKind, scope, aggregateType, name string) (Command, error)
	DecodeCommand(Subject, []byte) (Command, error)
}

type registry struct {
	mu       sync.RWMutex
	events   map[typeKey]registeredType[Event]
	commands map[typeKey]registeredType[Command]
}

type registeredType[T any] struct {
	typ     reflect.Type
	factory func() T
}

func NewTypeRegistry() DecoderRegistry {
	return newRegistry()
}

func newRegistry() *registry {
	return &registry{
		events:   make(map[typeKey]registeredType[Event]),
		commands: make(map[typeKey]registeredType[Command]),
	}
}

func RegisterMessageType[T any](target TypeRegistry) error {
	prototype, err := newTypedValue[T]()
	if err != nil {
		return err
	}
	return target.RegisterMessageType(prototype)
}

func MustRegisterMessageType[T any](target TypeRegistry) {
	if err := RegisterMessageType[T](target); err != nil {
		panic(err)
	}
}

func (c *registry) RegisterMessageType(prototype any) error {
	if evt, ok := prototype.(Event); ok {
		return c.registerEvent(evt, messageKind(evt, KindEvent))
	}
	if cmd, ok := prototype.(Command); ok {
		kind := messageKind(cmd, KindCommand)
		if err := c.registerCommand(cmd, kind); err != nil {
			return err
		}
		if _, ok := prototype.(MessageKindOverride); !ok && kind == KindCommand {
			return c.registerCommand(cmd, KindQuery)
		}
		return nil
	}
	return fmt.Errorf("message type %T must implement event.Event or event.Command", prototype)
}

func (c *registry) registerEvent(prototype Event, kind MessageKind) error {
	key := typeKey{scope: prototype.AggregateScope(), kind: kind, agg: prototype.AggregateType(), name: prototype.EventName()}
	if err := validateTypeKey(key, true); err != nil {
		return err
	}
	factory, typ, err := factoryFor[Event](prototype)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return registerType(c.events, key, registeredType[Event]{typ: typ, factory: factory})
}

func (c *registry) registerCommand(prototype Command, kind MessageKind) error {
	key := typeKey{scope: prototype.AggregateScope(), kind: kind, agg: prototype.AggregateType(), name: prototype.CommandName()}
	if err := validateTypeKey(key, false); err != nil {
		return err
	}
	factory, typ, err := factoryFor(prototype)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return registerType(c.commands, key, registeredType[Command]{typ: typ, factory: factory})
}

func (c *registry) NewEvent(kind MessageKind, scope, aggregateType, name string) (Event, error) {
	key := typeKey{scope: scope, kind: kind, agg: aggregateType, name: name}
	c.mu.RLock()
	reg, ok := c.events[key]
	if !ok {
		reg, ok = c.events[typeKey{scope: scope, kind: kind, agg: aggregateType}]
	}
	c.mu.RUnlock()
	if !ok {
		return nil, &UnknownEventError{Scope: scope, AggregateType: aggregateType, Name: name}
	}
	return reg.factory(), nil
}

func (c *registry) NewCommand(kind MessageKind, scope, aggregateType, name string) (Command, error) {
	key := typeKey{scope: scope, kind: kind, agg: aggregateType, name: name}
	c.mu.RLock()
	reg, ok := c.commands[key]
	c.mu.RUnlock()
	if !ok {
		return nil, &UnknownCommandError{Scope: scope, AggregateType: aggregateType, Name: name}
	}
	return reg.factory(), nil
}

func (c *registry) DecodeEvent(subj Subject, data []byte) (Event, error) {
	evt, err := c.NewEvent(subj.Kind, subj.Scope, subj.AggregateType, subj.Name)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, evt); err != nil {
		return nil, fmt.Errorf("decode event %s: %w", subj, err)
	}
	return evt, nil
}

func (c *registry) DecodeCommand(subj Subject, data []byte) (Command, error) {
	cmd, err := c.NewCommand(subj.Kind, subj.Scope, subj.AggregateType, subj.Name)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, cmd); err != nil {
		return nil, fmt.Errorf("decode command %s: %w", subj, err)
	}
	return cmd, nil
}

func newTypedValue[T any]() (T, error) {
	var zero T
	typ := reflect.TypeOf((*T)(nil)).Elem()
	if typ.Kind() == reflect.Interface {
		return zero, fmt.Errorf("typed registration requires a concrete type, got %s", typ)
	}
	if typ.Kind() == reflect.Pointer {
		return reflect.New(typ.Elem()).Interface().(T), nil
	}
	return reflect.Zero(typ).Interface().(T), nil
}

func factoryFor[T any](prototype T) (func() T, reflect.Type, error) {
	typ := reflect.TypeOf(prototype)
	if typ == nil {
		return nil, nil, fmt.Errorf("prototype is nil")
	}
	factoryType := typ
	if typ.Kind() == reflect.Pointer {
		return func() T { return reflect.New(typ.Elem()).Interface().(T) }, typ, nil
	}
	ptr := reflect.PointerTo(typ)
	if ptr.Implements(reflect.TypeOf((*T)(nil)).Elem()) {
		factoryType = ptr
		return func() T { return reflect.New(typ).Interface().(T) }, factoryType, nil
	}
	return func() T { return reflect.Zero(typ).Interface().(T) }, factoryType, nil
}

func registerType[T any](registry map[typeKey]registeredType[T], key typeKey, reg registeredType[T]) error {
	if existing, ok := registry[key]; ok {
		if existing.typ != reg.typ {
			return fmt.Errorf("type %s.%s.%s.%s already registered as %s, got %s",
				key.scope, key.kind, key.agg, key.name, existing.typ, reg.typ)
		}
		return nil
	}
	registry[key] = reg
	return nil
}

// Empty event names are allowed for wildcard event prototypes. This supports
// dynamic event names, for example log level encoded as the final subject token.
func validateTypeKey(key typeKey, allowEmptyName bool) error {
	if err := ValidateToken("scope", key.scope); err != nil {
		return err
	}
	if err := ValidateToken("kind", string(key.kind)); err != nil {
		return err
	}
	if err := ValidateToken("aggregate type", key.agg); err != nil {
		return err
	}
	if key.name == "" {
		if allowEmptyName {
			return nil
		}
		return fmt.Errorf("%w: name is empty", ErrInvalidName)
	}
	if err := ValidateToken("name", key.name); err != nil {
		return err
	}
	return nil
}
