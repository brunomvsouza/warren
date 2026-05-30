package amqpmock

import "github.com/brunomvsouza/warren"

// DeliveryFixture is the keyed-literal input to [NewDelivery]. It is a generic
// type alias for [warren.DeliveryFixture]: the gomock-free fixture lives in the
// root package so consumer/raw/batch unit tests can fabricate deliveries
// without importing this (gomock-heavy) subpackage (SPEC §10 decision 9, GA-09).
type DeliveryFixture[M any] = warren.DeliveryFixture[M]

// NewDelivery fabricates a *warren.Delivery[M] from f for tests. It re-exports
// [warren.NewDeliveryFixture]; the returned delivery is not bound to a live
// channel, so Ack/Nack/AckIf return an error rather than reaching a broker.
func NewDelivery[M any](f DeliveryFixture[M]) *warren.Delivery[M] {
	return warren.NewDeliveryFixture(f)
}
