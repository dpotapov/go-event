package nats

import (
	"crypto/sha1"
	"encoding/hex"
	"slices"
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
	if len(cfg.EventFilters) > 0 {
		return compactSubjectFilters(eventFilterSubjects(cfg))
	}
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
	return compactSubjectFilters(filters)
}

func eventFilterSubjects(cfg event.EventSubscriptionConfig) []string {
	typ := cfg.AggregateTypes[0]
	filters := make([]string, 0, len(cfg.EventFilters))
	for _, filter := range cfg.EventFilters {
		kind := filter.Kind
		if kind == "" {
			kind = event.KindEvent
		}
		ids := wildcardIfEmpty(filter.AggregateIDs)
		names := wildcardIfEmpty(filter.EventNames)
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

func compactSubjectFilters(filters []string) []string {
	seen := make(map[string]struct{}, len(filters))
	deduped := make([]string, 0, len(filters))
	for _, filter := range filters {
		if _, ok := seen[filter]; ok {
			continue
		}
		seen[filter] = struct{}{}
		deduped = append(deduped, filter)
	}
	out := make([]string, 0, len(deduped))
	for _, filter := range deduped {
		covered := false
		for _, candidate := range deduped {
			if candidate == filter {
				continue
			}
			if subjectFilterCovers(candidate, filter) {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, filter)
		}
	}
	slices.Sort(out)
	return out
}

func subjectFilterCovers(candidate, filter string) bool {
	candidateParts := strings.Split(candidate, ".")
	filterParts := strings.Split(filter, ".")
	if len(candidateParts) != len(filterParts) {
		return false
	}
	for i := range candidateParts {
		if candidateParts[i] != "*" && candidateParts[i] != filterParts[i] {
			return false
		}
	}
	return true
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
