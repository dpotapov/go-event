package event

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

type typeKey struct {
	scope string
	agg   string
	name  string
}

// Catalog maps domain names to concrete command and event types.
type Catalog struct {
	mu       sync.RWMutex
	events   map[typeKey]registeredType[Event]
	commands map[typeKey]registeredType[Command]
}

type registeredType[T any] struct {
	typ     reflect.Type
	factory func() T
}

func NewCatalog() *Catalog {
	return &Catalog{
		events:   make(map[typeKey]registeredType[Event]),
		commands: make(map[typeKey]registeredType[Command]),
	}
}

func RegisterEventType[T Event](catalog *Catalog) error {
	prototype, err := newTypedValue[T]()
	if err != nil {
		return err
	}
	return catalog.RegisterEvent(prototype)
}

func MustRegisterEventType[T Event](catalog *Catalog) {
	if err := RegisterEventType[T](catalog); err != nil {
		panic(err)
	}
}

func RegisterCommandType[T Command](catalog *Catalog) error {
	prototype, err := newTypedValue[T]()
	if err != nil {
		return err
	}
	return catalog.RegisterCommand(prototype)
}

func MustRegisterCommandType[T Command](catalog *Catalog) {
	if err := RegisterCommandType[T](catalog); err != nil {
		panic(err)
	}
}

func (c *Catalog) RegisterEvent(prototype Event) error {
	if c == nil {
		return fmt.Errorf("register event: catalog is nil")
	}
	key := typeKey{scope: prototype.AggregateScope(), agg: prototype.AggregateType(), name: prototype.EventName()}
	if err := validateTypeKey(key); err != nil {
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

func (c *Catalog) RegisterCommand(prototype Command) error {
	if c == nil {
		return fmt.Errorf("register command: catalog is nil")
	}
	key := typeKey{scope: prototype.AggregateScope(), agg: prototype.AggregateType(), name: prototype.CommandName()}
	if err := validateTypeKey(key); err != nil {
		return err
	}
	factory, typ, err := factoryFor[Command](prototype)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return registerType(c.commands, key, registeredType[Command]{typ: typ, factory: factory})
}

func (c *Catalog) NewEvent(scope, aggregateType, name string) (Event, error) {
	if c == nil {
		return nil, fmt.Errorf("new event: catalog is nil")
	}
	key := typeKey{scope: scope, agg: aggregateType, name: name}
	c.mu.RLock()
	reg, ok := c.events[key]
	c.mu.RUnlock()
	if !ok {
		return nil, &UnknownEventError{Scope: scope, AggregateType: aggregateType, Name: name}
	}
	return reg.factory(), nil
}

func (c *Catalog) NewCommand(scope, aggregateType, name string) (Command, error) {
	if c == nil {
		return nil, fmt.Errorf("new command: catalog is nil")
	}
	key := typeKey{scope: scope, agg: aggregateType, name: name}
	c.mu.RLock()
	reg, ok := c.commands[key]
	c.mu.RUnlock()
	if !ok {
		return nil, &UnknownCommandError{Scope: scope, AggregateType: aggregateType, Name: name}
	}
	return reg.factory(), nil
}

func (c *Catalog) DecodeEvent(addr Address, data []byte) (Event, error) {
	evt, err := c.NewEvent(addr.Scope, addr.AggregateType, addr.Name)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, evt); err != nil {
		return nil, fmt.Errorf("decode event %s: %w", addr.Subject(), err)
	}
	return evt, nil
}

func (c *Catalog) DecodeCommand(addr Address, data []byte) (Command, error) {
	cmd, err := c.NewCommand(addr.Scope, addr.AggregateType, addr.Name)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, cmd); err != nil {
		return nil, fmt.Errorf("decode command %s: %w", addr.Subject(), err)
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
			return fmt.Errorf("type %s.%s.%s already registered as %s, got %s",
				key.scope, key.agg, key.name, existing.typ, reg.typ)
		}
		return nil
	}
	registry[key] = reg
	return nil
}

func validateTypeKey(key typeKey) error {
	if err := ValidateToken("scope", key.scope); err != nil {
		return err
	}
	if err := ValidateToken("aggregate type", key.agg); err != nil {
		return err
	}
	if err := ValidateToken("name", key.name); err != nil {
		return err
	}
	return nil
}
