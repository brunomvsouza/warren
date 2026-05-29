package otel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// headerCarrier.Keys is part of the propagation.TextMapCarrier contract, but the
// W3C TraceContext propagator never calls it on the inject/extract path (Inject
// uses Set, Extract uses Get), so the Inject→Extract round-trip tests leave it
// uncovered. This white-box test pins the enumeration contract directly: Keys
// must return every header key (order-independent), so a propagator that does
// enumerate the carrier — e.g. a composite or baggage propagator a caller swaps
// in via a custom Propagator — observes the full set.

func TestHeaderCarrier_Keys_returnsEveryKey(t *testing.T) {
	c := headerCarrier{"traceparent": "tp", "tracestate": "ts", "x-other": "v"}
	assert.ElementsMatch(t, []string{"traceparent", "tracestate", "x-other"}, c.Keys())
}

func TestHeaderCarrier_Keys_emptyCarrier(t *testing.T) {
	assert.Empty(t, headerCarrier{}.Keys())
}
