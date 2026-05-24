package metrics

import "time"

// DefaultHistogramBuckets returns the default latency histogram buckets in
// milliseconds, tuned for AMQP-local publish/handle latencies.
// A new slice is returned on each call so callers cannot corrupt shared state.
func DefaultHistogramBuckets() []float64 {
	return []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}
}

// MetricsLabel identifies an optional high-cardinality label that can be
// enabled via WithMetricsLabels on a connection or per-builder.
type MetricsLabel uint8

const (
	// MetricsLabelRoutingKey enables routing_key as an additional label.
	MetricsLabelRoutingKey MetricsLabel = iota + 1
	// MetricsLabelMessageType enables message_type as an additional label.
	MetricsLabelMessageType
)

// ClientMetrics records connection-level observations.
//
// Mandatory Prometheus metrics emitted by the built-in implementation:
//   - connection_reconnects_total{role}
//   - connection_blocked_seconds{role}
//   - topology_redeclare_seconds{role}
//   - connection_degraded_total{role, reason}
type ClientMetrics interface {
	// RecordReconnect increments the reconnect counter for the given role.
	RecordReconnect(role string)
	// RecordBlocked records the duration the connection was blocked by the broker.
	RecordBlocked(role string, d time.Duration)
	// RecordTopologyRedeclare records the duration of a topology redeclaration after reconnect.
	RecordTopologyRedeclare(role string, d time.Duration)
	// RecordDegraded increments the degraded-state counter for the given role and reason.
	// Fired once per degraded-state transition (re-armed on recovery).
	RecordDegraded(role, reason string)
}

// PublisherMetrics records publisher-level observations.
//
// Mandatory Prometheus metrics emitted by the built-in implementation:
//   - publisher_in_flight{exchange}
//   - publisher_publish_seconds{exchange,outcome}
//   - publisher_retry_total{exchange,reason}
type PublisherMetrics interface {
	// InFlightAdd adjusts the in-flight publish gauge by delta (+1 on start, -1 on finish).
	InFlightAdd(exchange string, delta int64)
	// RecordPublish records a completed publish with its outcome and duration.
	RecordPublish(exchange, outcome string, d time.Duration)
	// RecordRetry increments the retry counter for the given exchange and reason.
	RecordRetry(exchange, reason string)
}

// ConsumerMetrics records consumer-level observations.
//
// Mandatory Prometheus metrics emitted by the built-in implementation:
//   - consumer_resubscribed_total{queue}
//   - consumer_handler_aborted_channel_closed_total{queue}
//   - consumer_handler_timeout_total{queue}
//   - consumer_handler_seconds{queue,outcome}
//   - consumer_cancelled_total{queue,reason}
//   - consumer_max_redeliveries_total{queue,cause}
//   - replier_drop_no_dlx_total{queue}
type ConsumerMetrics interface {
	// RecordResubscribed increments the resubscription counter after a reconnect.
	RecordResubscribed(queue string)
	// RecordHandlerAbortedChannelClosed increments the aborted-handler counter when
	// the AMQP channel closes mid-handler.
	RecordHandlerAbortedChannelClosed(queue string)
	// RecordHandlerTimeout increments the handler-timeout counter.
	RecordHandlerTimeout(queue string)
	// RecordHandler records a completed handler invocation with its outcome and duration.
	RecordHandler(queue, outcome string, d time.Duration)
	// RecordCancelled increments the broker-initiated cancel counter.
	// reason is the consumer tag reported by basic.cancel; it identifies which
	// consumer the broker cancelled (useful when multiple consumers share a queue).
	RecordCancelled(queue, reason string)
	// RecordMaxRedeliveries increments the max-redeliveries counter.
	// cause distinguishes the three enforcement paths:
	//   - "x-death"        counter A: x-death ceiling exceeded (cross-process, DLX bounces)
	//   - "in-process"     counter B: in-process requeue loop exceeded (process-local)
	//   - "delivery-limit" broker-enforced on quorum queues (counter B disabled)
	RecordMaxRedeliveries(queue, cause string)
	// RecordReplierDropNoDLX increments the counter for Replier messages dropped
	// because the request queue has no dead-letter exchange configured.
	RecordReplierDropNoDLX(queue string)
}
