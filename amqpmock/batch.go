package amqpmock

import "github.com/brunomvsouza/warren"

// BatchFixture is the keyed-literal input to [NewBatch]. It is a generic type
// alias for [warren.BatchFixture]; see [DeliveryFixture] for why the fixture
// path lives in the root package (SPEC §10 decision 9, GA-09).
type BatchFixture[M any] = warren.BatchFixture[M]

// NewBatch fabricates a *warren.Batch[M] from f for tests. It re-exports
// [warren.NewBatchFixture]; like [NewDelivery], the batch is not bound to a
// live channel, so its Ack/Nack do not reach a broker.
func NewBatch[M any](f BatchFixture[M]) *warren.Batch[M] {
	return warren.NewBatchFixture(f)
}
