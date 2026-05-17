package nats

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"

	goevent "github.com/dpotapov/go-event"
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

func EventSubject(scope, aggregateType, aggregateID, eventName string) string {
	return goevent.Address{
		Scope:         scope,
		Kind:          goevent.KindEvent,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Name:          eventName,
	}.Subject()
}

func AggregateEventFilter(scope, aggregateType, aggregateID string) string {
	return strings.Join([]string{scope, string(goevent.KindEvent), aggregateType, aggregateID, ">"}, ".")
}

func EventTypeFilter(scope, aggregateType string) string {
	return strings.Join([]string{scope, string(goevent.KindEvent), aggregateType, ">"}, ".")
}

func CommandSubject(cmd goevent.Command) string {
	return goevent.CommandAddress(cmd).Subject()
}

func QuerySubject(query goevent.Command) string {
	return goevent.QueryAddress(query).Subject()
}

func eventFilters(cfg goevent.EventSubscriptionConfig) []string {
	types := wildcardIfEmpty(cfg.AggregateTypes)
	ids := wildcardIfEmpty(cfg.AggregateIDs)
	names := wildcardIfEmpty(cfg.EventNames)

	filters := make([]string, 0, len(types)*len(ids)*len(names))
	for _, typ := range types {
		for _, id := range ids {
			for _, name := range names {
				filters = append(filters, strings.Join([]string{
					cfg.AggregateScope,
					string(goevent.KindEvent),
					typ,
					id,
					name,
				}, "."))
			}
		}
	}
	return filters
}

func commandFilters(kind goevent.MessageKind, cfg goevent.CommandSubscriptionConfig) []string {
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

func parseAddress(subject string) (goevent.Address, bool) {
	return goevent.ParseSubject(subject)
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
