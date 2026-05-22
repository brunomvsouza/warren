package headers_test

import (
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
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "expired", 100),
			makeEntry("myqueue", "rejected", 2),
			makeEntry("myqueue", "delivery-limit", 5),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	// DeathCount only sums rejected + delivery-limit
	assert.Equal(t, 7, result.Count)
	// DeathReasons returns unique reasons in declaration order
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
	tbl := amqp091.Table{
		"x-death": []any{
			makeEntry("myqueue", "delivery-limit", 4),
		},
	}
	result := headers.ParseXDeath(tbl, "myqueue")
	assert.Equal(t, 4, result.Count)
	assert.Equal(t, []string{"delivery-limit"}, result.Reasons)
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
