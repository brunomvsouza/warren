package headers

import (
	"math"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

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

// CountByReason returns the total x-death count for a specific reason.
func (r *XDeathResult) CountByReason(reason string) int {
	return r.byReason[reason]
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
		// Saturate the accumulated sum so that multiple entries for the same
		// reason cannot overflow int on 32-bit platforms (math.MaxInt = 2^31-1
		// there). Any value exceeding MaxInt still exceeds any practical
		// MaxRedeliveries ceiling, so saturation is semantically correct.
		// On 64-bit: int == int64; the sum of two int64 values each ≤ MaxInt64/2
		// cannot wrap, so the < 0 guard below is dead code on 64-bit by design.
		newByReason := result.byReason[reason] + int(count)
		if newByReason < 0 { //nolint:staticcheck // 32-bit saturation guard; dead on 64-bit
			newByReason = math.MaxInt
		}
		result.byReason[reason] = newByReason

		if reason == "rejected" || reason == "delivery-limit" {
			newCount := result.Count + int(count)
			if newCount < 0 { //nolint:staticcheck // 32-bit saturation guard; dead on 64-bit
				newCount = math.MaxInt
			}
			result.Count = newCount
		}
	}
	return result
}
