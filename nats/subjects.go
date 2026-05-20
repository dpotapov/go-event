package nats

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"

	"github.com/dpotapov/go-event"
)

const DefaultStreamPattern = "<SCOPE>_<AGG>_EVENTS"

func StreamName(pattern, scope, aggregateType string) string {
	if pattern == "" {
		pattern = DefaultStreamPattern
	}
	replacer := strings.NewReplacer(
		"<SCOPE>", strings.ToUpper(scope),
		"<AGG>", strings.ToUpper(aggregateType),
	)
	return replacer.Replace(pattern)
}

func AggregateMessageFilter(scope, aggregateType, aggregateID string) string {
	return strings.Join([]string{scope, string(event.KindEvent), aggregateType, aggregateID, ">"}, ".")
}

func SnapshotSubject(agg event.ESAggregate) string {
	_, snapshotType := agg.EventTypes()
	return strings.Join([]string{agg.AggregateScope(), string(event.KindEvent), agg.AggregateType(), agg.AggregateID(), snapshotEventName(snapshotType)}, ".")
}

func EventTypeFilter(kind event.MessageKind, scope, aggregateType string) string {
	return strings.Join([]string{scope, string(kind), aggregateType, ">"}, ".")
}

func CommandSubject(cmd event.Command) string {
	return event.CommandSubject(cmd).String()
}

func QuerySubject(query event.Command) string {
	return event.QuerySubject(query).String()
}

func eventFilters(cfg event.EventSubscriptionConfig) []string {
	kind := cfg.Kind
	if kind == "" {
		kind = event.KindEvent
	}
	types := wildcardIfEmpty(cfg.AggregateTypes)
	ids := wildcardIfEmpty(cfg.AggregateIDs)
	names := wildcardIfEmpty(cfg.EventNames)

	filters := make([]string, 0, len(types)*len(ids)*len(names))
	for _, typ := range types {
		for _, id := range ids {
			for _, name := range names {
				filters = append(filters, strings.Join([]string{
					cfg.AggregateScope,
					string(kind),
					typ,
					id,
					name,
				}, "."))
			}
		}
	}
	return filters
}

func commandFilters(kind event.MessageKind, cfg event.CommandSubscriptionConfig) []string {
	if cfg.Kind != "" {
		kind = cfg.Kind
	}
	scope := cfg.AggregateScope
	if scope == "" {
		scope = "*"
	}
	types := wildcardIfEmpty(cfg.AggregateTypes)
	ids := wildcardIfEmpty(cfg.AggregateIDs)
	names := wildcardIfEmpty(cfg.CommandNames)

	filters := make([]string, 0, len(types)*len(ids)*len(names))
	for _, typ := range types {
		for _, id := range ids {
			for _, name := range names {
				filters = append(filters, strings.Join([]string{
					scope,
					string(kind),
					typ,
					id,
					name,
				}, "."))
			}
		}
	}
	return filters
}

func wildcardIfEmpty(values []string) []string {
	if len(values) == 0 {
		return []string{"*"}
	}
	return values
}

func ParseSubject(subject string) (event.Subject, bool) {
	parts := strings.Split(subject, ".")
	if len(parts) != 5 {
		return event.Subject{}, false
	}
	subj := event.Subject{
		Scope:         parts[0],
		Kind:          event.MessageKind(parts[1]),
		AggregateType: parts[2],
		AggregateID:   parts[3],
		Name:          parts[4],
	}
	return subj, subj.Validate() == nil
}

func consumerName(base, filter string) string {
	if base == "" {
		base = "goevent"
	}
	name := sanitizeName(base)
	if filter == "" {
		return name
	}
	sum := sha1.Sum([]byte(filter))
	return fmt.Sprintf("%s_%s", name, hex.EncodeToString(sum[:])[:12])
}

func sanitizeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		if r == '.' || r == '*' || r == '>' || r == '/' || r == '\\' || unicode.IsSpace(r) || !unicode.IsPrint(r) {
			b.WriteByte('_')
			continue
		}
		b.WriteRune(r)
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "goevent"
	}
	if len(out) > 48 {
		sum := sha1.Sum([]byte(out))
		return out[:35] + "_" + hex.EncodeToString(sum[:])[:12]
	}
	return out
}
