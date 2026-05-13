package metrics

import "time"

// DefaultHistogramBuckets are the default latency histogram buckets in milliseconds,
// tuned for AMQP-local publish/handle latencies.
var DefaultHistogramBuckets = []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}

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
type ClientMetrics interface {
	// RecordReconnect increments the reconnect counter for the given role.
	RecordReconnect(role string)
	// RecordBlocked records the duration the connection was blocked by the broker.
	RecordBlocked(role string, d time.Duration)
	// RecordTopologyRedeclare records the duration of a topology redeclaration after reconnect.
	RecordTopologyRedeclare(role string, d time.Duration)
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
	// RecordReplierDropNoDLX increments the counter for Replier messages dropped
	// because the request queue has no dead-letter exchange configured.
	RecordReplierDropNoDLX(queue string)
}
