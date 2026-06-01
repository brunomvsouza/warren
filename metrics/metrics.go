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
//   - publisher_rate_limited_total{exchange}
type PublisherMetrics interface {
	// InFlightAdd adjusts the in-flight publish gauge by delta (+1 on start, -1 on finish).
	InFlightAdd(exchange string, delta int64)
	// RecordRateLimited increments the counter for publishes throttled by the local
	// WithPublishRateLimit token bucket. Fired once per broker attempt that had to
	// wait for a token (whether it then proceeded or its context was cancelled); a
	// single Publish under PublishRetry can fire it more than once. exchange is the
	// only label — the rate limit is a per-publisher guardrail, not per-message.
	RecordRateLimited(exchange string)
	// RecordPublish records a completed publish with its outcome and duration.
	//
	// routingKey and messageType carry the opt-in high-cardinality label values
	// for the publisher_publish_seconds histogram; an implementation emits each
	// only when the corresponding label was enabled at construction
	// (MetricsLabelRoutingKey / MetricsLabelMessageType) and ignores it otherwise.
	RecordPublish(exchange, routingKey, messageType, outcome string, d time.Duration)
	// RecordRetry increments the retry counter for the given exchange and reason.
	RecordRetry(exchange, reason string)
	// RecordChannelPoolWait records how long a publish waited to acquire a channel
	// from the per-connection pool (T71). A non-zero p99 is the leading channel-pool
	// saturation signal — publishers are queueing for a channel slot.
	RecordChannelPoolWait(exchange string, d time.Duration)
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
//   - consumer_inflight_bytes{queue}
//   - queue_depth{queue}
//   - dlq_depth{dlq}
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
	// RecordConsumerDropNoDLX increments the counter for Consumer poison messages
	// dropped (Nack(false)) after MaxRedeliveries when no dead-letter exchange is
	// known for the source queue — the consumer-side parity of
	// RecordReplierDropNoDLX, so a silent poison drop stays observable (T65).
	RecordConsumerDropNoDLX(queue string)
	// RecordShutdownRequeued increments the counter for prefetched-but-undispatched
	// deliveries that were Nack(requeue=true)'d at consumer shutdown (T70). Every
	// rolling deploy is a low-grade incident; this makes the deploy-time duplicate
	// rate boundable and observable (SRE-07).
	RecordShutdownRequeued(queue string)
	// RecordRedelivered increments consumer_redelivered_total when a delivery
	// arrives with Redelivered()==true (T71 / DS-14). The redelivery ratio is the
	// dominant duplicate-budget signal that publisher_retry_total does not cover,
	// and a leading instability indicator (brewing poison storm / pool saturation).
	RecordRedelivered(queue string)
	// ConsumerInFlightAdd adjusts the consumer_in_flight gauge of active handlers
	// by delta (+1 on dispatch, -1 on completion) (T71). A saturating in-flight
	// count is a leading saturation signal.
	ConsumerInFlightAdd(queue string, delta int64)
	// InFlightBytesAdd adjusts the consumer_inflight_bytes gauge by delta (the
	// in-flight memory guardrail, T50): +len(body) when a delivery is dispatched,
	// -len(body) when its handler returns.
	InFlightBytesAdd(queue string, delta int64)
	// SetQueueDepth sets the queue_depth{queue} gauge to the broker-side message
	// backlog of the consumer's source queue, sampled by WithQueueDepthSampler (T52).
	SetQueueDepth(queue string, depth int64)
	// SetDLQDepth sets the dlq_depth{dlq} gauge to the broker-side message backlog of
	// the consumer's conventional "<queue>.dlq" dead-letter queue, sampled by
	// WithQueueDepthSampler (T52). Emitted only when that DLQ exists.
	SetDLQDepth(dlq string, depth int64)
	// DeleteQueueDepth removes the queue_depth{queue} series when the sampler stops
	// (T52b), so a long-lived process that cycles through distinct queue names does
	// not accumulate stale frozen series. A no-op for a series that was never set.
	DeleteQueueDepth(queue string)
	// DeleteDLQDepth removes the dlq_depth{dlq} series when the sampler stops (T52b),
	// the dead-letter counterpart of DeleteQueueDepth.
	DeleteDLQDepth(dlq string)
}
