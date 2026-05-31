package metrics

import "time"

// DefaultHistogramBuckets returns the default latency histogram buckets in
// milliseconds, tuned for AMQP-local publish/handle latencies.
// A new slice is returned on each call so callers cannot corrupt shared state.
func DefaultHistogramBuckets() []float64 {
	return []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}
}

// MetricsLabel identifies an optional high-cardinality label that can be
// enabled when constructing a Prometheus metrics implementation, e.g.
// NewPrometheusPublisherMetrics(reg, buckets, MetricsLabelRoutingKey).
//
// These labels are off by default because they explode time-series cardinality
// on workloads with many routing patterns or message types. Because Prometheus
// fixes a vector's label set at construction, the opt-in lives at construction
// time rather than on a later builder option.
type MetricsLabel uint8

const (
	// MetricsLabelRoutingKey enables routing_key as an additional label on the
	// publisher_publish_seconds histogram. It carries the publisher's configured
	// routing key. Not applied to consumer metrics (a consumer's per-delivery
	// routing key is not a stable dimension).
	MetricsLabelRoutingKey MetricsLabel = iota + 1
	// MetricsLabelMessageType enables message_type as an additional label on the
	// publisher_publish_seconds and consumer_handler_seconds histograms. It
	// carries the Go type name of the message type M.
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
//   - publisher_publish_seconds{exchange,outcome[,routing_key][,message_type]}
//   - publisher_retry_total{exchange,reason}
type PublisherMetrics interface {
	// InFlightAdd adjusts the in-flight publish gauge by delta (+1 on start, -1 on finish).
	InFlightAdd(exchange string, delta int64)
	// RecordPublish records a completed publish with its outcome and duration.
	//
	// routingKey and messageType carry the opt-in high-cardinality label values
	// for the publisher_publish_seconds histogram; an implementation emits each
	// only when the corresponding label was enabled at construction
	// (MetricsLabelRoutingKey / MetricsLabelMessageType) and ignores it otherwise.
	RecordPublish(exchange, routingKey, messageType, outcome string, d time.Duration)
	// RecordRetry increments the retry counter for the given exchange and reason.
	RecordRetry(exchange, reason string)
}

// ConsumerMetrics records consumer-level observations.
//
// Mandatory Prometheus metrics emitted by the built-in implementation:
//   - consumer_resubscribed_total{queue}
//   - consumer_handler_aborted_channel_closed_total{queue}
//   - consumer_handler_timeout_total{queue}
//   - consumer_handler_seconds{queue,outcome[,message_type]}
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
	//
	// messageType carries the opt-in high-cardinality label value for the
	// consumer_handler_seconds histogram; an implementation emits it only when
	// MetricsLabelMessageType was enabled at construction and ignores it otherwise.
	RecordHandler(queue, messageType, outcome string, d time.Duration)
	// RecordCancelled increments the broker-initiated cancel counter.
	// reason is a bounded enum classifying the basic.cancel — warren passes
	// "queue_deleted", "exclusive_revoked", or "unknown" (never the unbounded
	// consumer tag, which would explode label cardinality).
	RecordCancelled(queue, reason string)
	// RecordMaxRedeliveries increments the max-redeliveries counter.
	// cause distinguishes the enforcement paths emitted by this library:
	//   - "x-death"    counter A: x-death ceiling exceeded (cross-process, DLX bounces)
	//   - "in-process" counter B: in-process requeue loop exceeded (process-local)
	RecordMaxRedeliveries(queue, cause string)
	// RecordReplierDropNoDLX increments the counter for Replier messages dropped
	// because the request queue has no dead-letter exchange configured.
	RecordReplierDropNoDLX(queue string)
}
