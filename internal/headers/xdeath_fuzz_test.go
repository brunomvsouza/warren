package headers_test

import (
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/internal/headers"
)

func FuzzXDeathParser(f *testing.F) {
	// Seed corpus: normal entry, wrong type, empty, missing queue.
	f.Add("myqueue", "rejected", int64(3))
	f.Add("myqueue", "expired", int64(0))
	f.Add("", "delivery-limit", int64(1))
	f.Add("myqueue", "", int64(-1))

	f.Fuzz(func(t *testing.T, queue, reason string, count int64) {
		tbl := amqp091.Table{
			"x-death": []any{
				amqp091.Table{
					"queue":  queue,
					"reason": reason,
					"count":  count,
				},
			},
		}
		// Must not panic; result must be consistent.
		result := headers.ParseXDeath(tbl, "myqueue")
		_ = result.Count
		_ = result.Reasons
		_ = result.CountByReason(reason)
	})
}
