package headers

import (
	"math"
	"strings"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// normalizeReason canonicalises an x-death reason atom by folding separators to
// a hyphen. RabbitMQ emits the delivery-limit reason with an UNDERSCORE
// ("delivery_limit", confirmed identical on 3.13 and 4.x — RMQ-01 / gate G1),
// while warren's public API documents the hyphenated "delivery-limit". Folding
// here keeps the stored reason and any caller-supplied lookup key in agreement
// regardless of which spelling the broker or the caller uses. strings.ReplaceAll
// returns the input unchanged (no allocation) when there is no underscore, so
// the common reasons (rejected/expired/maxlen) stay alloc-free.
func normalizeReason(reason string) string {
	return strings.ReplaceAll(reason, "_", "-")
}

// XDeathResult holds the parsed x-death header for a given delivery queue.
type XDeathResult struct {
	// Count is the sum of x-death counts for reason ∈ {rejected, delivery-limit}
	// matching the delivery's current queue.
	Count int

	// Reasons holds unique reasons in declaration order (all reasons, not just counted ones).
	Reasons []string

	// byReason maps reason → total count across all entries for the current queue.
	byReason map[string]int
}

// CountByReason returns the total x-death count for a specific reason. The
// lookup key is normalised (RMQ-01), so "delivery-limit" and "delivery_limit"
// resolve to the same entry regardless of which spelling the caller passes.
func (r *XDeathResult) CountByReason(reason string) int {
	return r.byReason[normalizeReason(reason)]
}

// ParseXDeath parses the AMQP x-death header from tbl for the given queue name.
// It returns the death counts and reasons. Malformed or absent headers return a
// zero result rather than an error — the header is broker-written and may be
// absent on first delivery.
func ParseXDeath(tbl amqp091.Table, queue string) XDeathResult {
	// byReason is allocated lazily on the first matching entry (see below).
	// Lens-09 (PC-08): the common no-DLX delivery hits one of the early returns
	// below, where a map allocation would be pure waste on 100% of deliveries.
	var result XDeathResult
	if tbl == nil {
		return result
	}
	raw, ok := tbl["x-death"]
	if !ok {
		return result
	}
	entries, ok := raw.([]any)
	if !ok {
		return result
	}

	for _, e := range entries {
		entry, ok := e.(amqp091.Table)
		if !ok {
			continue
		}
		q, _ := entry["queue"].(string)
		if q != queue {
			continue
		}
		reason, _ := entry["reason"].(string)
		reason = normalizeReason(reason)
		count, _ := entry["count"].(int64)
		if count < 0 {
			count = 0
		}
		// Cap individual count to math.MaxInt to prevent int overflow on 32-bit
		// platforms (math.MaxInt = 2^31-1 there; int64 can exceed it).
		// On 64-bit platforms math.MaxInt == math.MaxInt64, so count (int64) can
		// never exceed it and this branch is dead code by design — it protects
		// 32-bit builds without penalising 64-bit runtime performance.
		if count > math.MaxInt { //nolint:staticcheck // intentional 32-bit guard; dead on 64-bit
			count = math.MaxInt
		}

		// Lazily allocate byReason on the first entry matching this queue, so a
		// delivery with no matching x-death entry never allocates the map.
		if result.byReason == nil {
			result.byReason = make(map[string]int)
		}

		if _, seen := result.byReason[reason]; !seen {
			result.Reasons = append(result.Reasons, reason)
		}
		// Saturate the accumulated sum at math.MaxInt so that multiple entries for
		// the same reason cannot overflow int. Each per-entry count is already
		// capped at math.MaxInt above, so on 64-bit (int == int64 == MaxInt64) the
		// sum of two near-MaxInt entries genuinely overflows — this guard is LIVE
		// on every platform, not dead code: a hostile broker sending two
		// MaxInt64-count entries for the same (queue, reason) would otherwise wrap
		// DeathCount() negative and defeat the MaxRedeliveries poison-pill check.
		// Compare before adding rather than relying on signed-overflow wraparound.
		// result.byReason[reason] ∈ [0, MaxInt], so MaxInt-it never underflows.
		if int(count) > math.MaxInt-result.byReason[reason] {
			result.byReason[reason] = math.MaxInt
		} else {
			result.byReason[reason] += int(count)
		}

		if reason == "rejected" || reason == "delivery-limit" {
			if int(count) > math.MaxInt-result.Count {
				result.Count = math.MaxInt
			} else {
				result.Count += int(count)
			}
		}
	}
	return result
}
