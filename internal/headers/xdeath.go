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
	result := XDeathResult{byReason: make(map[string]int)}
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
		// Cap to math.MaxInt to prevent int overflow on 32-bit platforms.
		// x-death counts legitimately exceeding this bound are treated as MaxInt,
		// which still exceeds any practical MaxRedeliveries ceiling.
		if count > math.MaxInt {
			count = math.MaxInt
		}

		if _, seen := result.byReason[reason]; !seen {
			result.Reasons = append(result.Reasons, reason)
		}
		result.byReason[reason] += int(count)

		if reason == "rejected" || reason == "delivery-limit" {
			result.Count += int(count)
		}
	}
	return result
}
