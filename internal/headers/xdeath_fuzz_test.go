package headers_test

import (
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/internal/headers"
)

func FuzzXDeathParser(f *testing.F) {
	// Seed corpus: valid entry shapes.
	f.Add("myqueue", "rejected", int64(3))
	f.Add("myqueue", "expired", int64(0))
	f.Add("", "delivery-limit", int64(1))
	f.Add("myqueue", "delivery_limit", int64(2)) // RMQ-01: real broker spelling (underscore)
	f.Add("myqueue", "", int64(-1))
	f.Add("myqueue", "maxlen", int64(50))

	f.Fuzz(func(t *testing.T, queue, reason string, count int64) {
		// Well-formed single entry.
		tbl := amqp091.Table{
			"x-death": []any{
				amqp091.Table{
					"queue":  queue,
					"reason": reason,
					"count":  count,
				},
			},
		}
		result := headers.ParseXDeath(tbl, "myqueue")
		if result.Count < 0 {
			t.Fatalf("ParseXDeath returned negative Count %d for count=%d", result.Count, count)
		}
		_ = result.Reasons
		_ = result.CountByReason(reason)

		// Mixed: one wrong-type entry followed by a valid one.
		mixed := amqp091.Table{
			"x-death": []any{
				"not-a-table",
				amqp091.Table{"queue": "myqueue", "reason": reason, "count": count},
			},
		}
		_ = headers.ParseXDeath(mixed, "myqueue")

		// Wrong top-level type (not []any).
		wrong := amqp091.Table{"x-death": reason}
		_ = headers.ParseXDeath(wrong, "myqueue")
	})
}
