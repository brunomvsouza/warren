package headers_test

import (
	"math"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"

	"github.com/brunomvsouza/warren/internal/headers"
)

func makeEntry(queue, reason string, count int64) amqp091.Table {
	return amqp091.Table{
		"queue":  queue,
		"reason": reason,
		"count":  count,
	}
}

func TestParseXDeath_Absent(t *testing.T) {
	result := headers.ParseXDeath(nil, "myqueue")
	assert.Equal(t, 0, result.Count)
	assert.Empty(t, result.Reasons)
}

func TestParseXDeath_EmptySlice(t *testing.T) {
	tbl := amqp091.Table{
		"x-death": []any{},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 0, result.Count)
	assert.Empty(t, result.Reasons)
}

func TestParseXDeath_SingleRejectedEntry(t *testing.T) {
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "rejected", 3),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 3, result.Count)
	assert.Equal(t, []string{"rejected"}, result.Reasons)
}

func TestParseXDeath_FilterExpired(t *testing.T) {
	// expired + rejected for same queue: DeathCount returns only rejected count
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "expired", 100),
			makeEntry("myqueue", "rejected", 2),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 2, result.Count)
}

func TestParseXDeath_MultipleReasons(t *testing.T) {
	// RMQ-01 / gate G1: the broker emits the delivery-limit reason with an
	// UNDERSCORE ("delivery_limit", confirmed on RabbitMQ 3.13 and 4.x). The
	// parser must normalise it to the canonical hyphenated form so it both sums
	// into DeathCount and surfaces under the documented "delivery-limit" spelling.
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "expired", 100),
			makeEntry("myqueue", "rejected", 2),
			makeEntry("myqueue", "delivery_limit", 5),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	// DeathCount only sums rejected + delivery-limit
	assert.Equal(t, 7, result.Count)
	// DeathReasons returns unique reasons in declaration order, canonicalised.
	assert.Equal(t, []string{"expired", "rejected", "delivery-limit"}, result.Reasons)
}

func TestParseXDeath_DeathCountByReason(t *testing.T) {
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "expired", 100),
			makeEntry("myqueue", "rejected", 2),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 100, result.CountByReason("expired"))
	assert.Equal(t, 2, result.CountByReason("rejected"))
	assert.Equal(t, 0, result.CountByReason("delivery-limit"))
}

func TestParseXDeath_FiltersDifferentQueue(t *testing.T) {
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("otherqueue", "rejected", 10),
			makeEntry("myqueue", "rejected", 3),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	// Only counts entries matching the current queue
	assert.Equal(t, 3, result.Count)
}

func TestParseXDeath_WrongShape(t *testing.T) {
	tbl := amqp091.Table{
		"x-death": "not a slice",
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 0, result.Count)
	assert.Empty(t, result.Reasons)
}

func TestParseXDeath_DeliveryLimit(t *testing.T) {
	// RMQ-01 / gate G1: drive the real broker atom ("delivery_limit", underscore).
	// It must count toward DeathCount and be reported as the canonical
	// "delivery-limit", and a lookup by EITHER spelling must resolve.
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "delivery_limit", 4),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 4, result.Count)
	assert.Equal(t, []string{"delivery-limit"}, result.Reasons)
	assert.Equal(t, 4, result.CountByReason("delivery-limit"), "canonical hyphen lookup must resolve")
	assert.Equal(t, 4, result.CountByReason("delivery_limit"), "raw broker underscore lookup must resolve")
}

func TestParseXDeath_FilterMaxlen(t *testing.T) {
	// maxlen (broker overflow policy) must be excluded from DeathCount, same as expired.
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "maxlen", 50),
			makeEntry("myqueue", "rejected", 3),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 3, result.Count)
	assert.Equal(t, 50, result.CountByReason("maxlen"))
	assert.Equal(t, []string{"maxlen", "rejected"}, result.Reasons)
}

func TestParseXDeath_WrongEntryType(t *testing.T) {
	// Entries that are not amqp091.Table must be silently skipped.
	tbl := amqp091.Table{
		"x-death": []any{
			"not-a-table",
			makeEntry("myqueue", "rejected", 2),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 2, result.Count)
}

func TestParseXDeath_KeyAbsentNonNilTable(t *testing.T) {
	// Non-nil table that contains other AMQP properties but no x-death key.
	// This is the normal shape on a first-delivery message.
	tbl := amqp091.Table{"content-type": "application/json"}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 0, result.Count)
	assert.Empty(t, result.Reasons)
}

func TestParseXDeath_NegativeCountClamped(t *testing.T) {
	// A misbehaving or compromised broker may send a negative count.
	// It must be clamped to zero so DeathCount never goes negative.
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "rejected", -999),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 0, result.Count)
	assert.Equal(t, 0, result.CountByReason("rejected"))
}

func TestParseXDeath_NoAllocOnNoDLXPath(t *testing.T) {
	// Lens-09 (PC-08): the byReason map must be allocated lazily, *after* the
	// early returns. Every delivery without a DLX hop (the common case) hits one
	// of these no-x-death paths, so allocating a map there would put a heap
	// allocation on the hot path of 100% of deliveries. Guard it with
	// AllocsPerRun == 0. Tables are built outside the measured closure so only
	// ParseXDeath's own allocations are counted.
	const queue = "myqueue"

	t.Run("nil table", func(t *testing.T) {
		allocs := testing.AllocsPerRun(100, func() {
			_ = headers.ParseXDeath(nil, queue)
		})
		assert.Zero(t, allocs, "nil-table path must not allocate the byReason map")
	})

	t.Run("table without x-death key", func(t *testing.T) {
		tbl := amqp091.Table{"content-type": "application/json"}
		allocs := testing.AllocsPerRun(100, func() {
			_ = headers.ParseXDeath(tbl, queue)
		})
		assert.Zero(t, allocs, "x-death-absent path must not allocate the byReason map")
	})

	t.Run("x-death wrong shape", func(t *testing.T) {
		tbl := amqp091.Table{"x-death": "not a slice"}
		allocs := testing.AllocsPerRun(100, func() {
			_ = headers.ParseXDeath(tbl, queue)
		})
		assert.Zero(t, allocs, "x-death-wrong-shape path must not allocate the byReason map")
	})
}

func TestParseXDeath_AllEntriesNonMatchingQueue_NoAlloc(t *testing.T) {
	// Lens-09 (PC-08) edge: x-death is present and well-shaped, but NO entry
	// matches the delivery's queue. The loop runs fully yet every entry hits the
	// `q != queue` continue before the lazy alloc, so byReason must stay nil —
	// zero allocations and a zero result. This is the path the lazy-alloc change
	// newly made allocation-free (a matching entry would allocate).
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("otherqueue", "rejected", 10),
			makeEntry("anotherqueue", "delivery-limit", 5),
		},
	}
	allocs := testing.AllocsPerRun(100, func() {
		_ = headers.ParseXDeath(tbl, "myqueue")
	})
	assert.Zero(t, allocs, "all-non-matching-queue path must not allocate the byReason map")

	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 0, result.Count)
	assert.Empty(t, result.Reasons)
	assert.Equal(t, 0, result.CountByReason("rejected"))
}

func TestParseXDeath_MatchingEntryAllocatesAndCounts(t *testing.T) {
	// Counterpart to the no-alloc guards: a matching entry MUST allocate byReason
	// (and grow Reasons), so the lazy-alloc optimization cannot silently drop
	// counts on the DLX path. Locks the contract from the allocating side.
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "rejected", 3),
		},
	}
	allocs := testing.AllocsPerRun(100, func() {
		_ = headers.ParseXDeath(tbl, "myqueue")
	})
	assert.GreaterOrEqual(t, allocs, float64(1), "a matching entry must allocate the byReason map")

	// Correctness is preserved alongside the allocation.
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 3, result.Count)
	assert.Equal(t, 3, result.CountByReason("rejected"))
	assert.Equal(t, []string{"rejected"}, result.Reasons)
}

func TestParseXDeath_CountByReason_NilMapSafe(t *testing.T) {
	// After the lazy-allocation change, the no-DLX path leaves byReason nil.
	// CountByReason must still return 0 (reading a nil map is safe in Go).
	result := headers.ParseXDeath(nil, "myqueue")
	assert.Equal(t, 0, result.CountByReason("rejected"))
}

func TestParseXDeath_DuplicateReasonAccumulates(t *testing.T) {
	// Multiple entries with the same (queue, reason) pair accumulate their counts.
	// The broker appends a new entry on each DLX hop rather than updating existing ones.
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "rejected", 2),
			makeEntry("myqueue", "rejected", 3),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 5, result.Count)
	assert.Equal(t, 5, result.CountByReason("rejected"))
	assert.Equal(t, []string{"rejected"}, result.Reasons, "duplicate reason must appear only once")
}

// TestParseXDeath_AccumulationSaturatesAtMaxInt locks the load-bearing overflow
// guard: a hostile/buggy broker sending two MaxInt64-count entries for the same
// (queue, reason) must saturate at math.MaxInt, never wrap negative. A negative
// DeathCount() would defeat the `DeathCount() >= MaxRedeliveries` poison-pill
// check (it would read as "below the limit" forever → unbounded redelivery). This
// guard is LIVE on 64-bit (int == int64), not dead code.
func TestParseXDeath_AccumulationSaturatesAtMaxInt(t *testing.T) {
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "delivery-limit", math.MaxInt64),
			makeEntry("myqueue", "delivery-limit", math.MaxInt64),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, math.MaxInt, result.Count, "accumulated count must saturate at MaxInt, not overflow negative")
	assert.GreaterOrEqual(t, result.Count, 0, "DeathCount must never be negative")
	assert.Equal(t, math.MaxInt, result.CountByReason("delivery-limit"))
}
