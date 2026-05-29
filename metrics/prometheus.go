package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusClientMetrics is a ClientMetrics backed by Prometheus counters and
// histograms registered lazily into the provided prometheus.Registerer.
//
// Mandatory metrics:
//   - connection_reconnects_total{role}
//   - connection_blocked_seconds{role}
//   - topology_redeclare_seconds{role}
type PrometheusClientMetrics struct {
	reconnectsTotal   *prometheus.CounterVec
	blockedSeconds    *prometheus.HistogramVec
	topologyRedeclare *prometheus.HistogramVec
	degradedTotal     *prometheus.CounterVec
}

// NewPrometheusClientMetrics creates a PrometheusClientMetrics and registers all
// mandatory collectors into reg. If buckets is nil, DefaultHistogramBuckets() is used
// (a fresh slice per call so callers cannot corrupt shared state).
// Returns an error if any collector is already registered.
func NewPrometheusClientMetrics(reg prometheus.Registerer, buckets []float64) (*PrometheusClientMetrics, error) {
	if buckets == nil {
		buckets = DefaultHistogramBuckets()
	}
	m := &PrometheusClientMetrics{
		reconnectsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connection_reconnects_total",
			Help: "Total number of AMQP connection reconnects.",
		}, []string{"role"}),
		blockedSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "connection_blocked_seconds",
			Help:    "Duration in seconds the AMQP connection was blocked by the broker.",
			Buckets: buckets,
		}, []string{"role"}),
		topologyRedeclare: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "topology_redeclare_seconds",
			Help:    "Duration in seconds of topology redeclaration after an AMQP reconnect.",
			Buckets: buckets,
		}, []string{"role"}),
		degradedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connection_degraded_total",
			Help: "Total number of AMQP connection degraded-state transitions (topology redeclare failed).",
		}, []string{"role", "reason"}),
	}
	for _, c := range []prometheus.Collector{m.reconnectsTotal, m.blockedSeconds, m.topologyRedeclare, m.degradedTotal} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// RecordReconnect increments connection_reconnects_total for the given role.
func (m *PrometheusClientMetrics) RecordReconnect(role string) {
	m.reconnectsTotal.WithLabelValues(role).Inc()
}

// RecordBlocked records d into the connection_blocked_seconds histogram.
func (m *PrometheusClientMetrics) RecordBlocked(role string, d time.Duration) {
	m.blockedSeconds.WithLabelValues(role).Observe(d.Seconds())
}

// RecordTopologyRedeclare records d into the topology_redeclare_seconds histogram.
func (m *PrometheusClientMetrics) RecordTopologyRedeclare(role string, d time.Duration) {
	m.topologyRedeclare.WithLabelValues(role).Observe(d.Seconds())
}

// RecordDegraded increments connection_degraded_total for the given role and reason.
func (m *PrometheusClientMetrics) RecordDegraded(role, reason string) {
	m.degradedTotal.WithLabelValues(role, reason).Inc()
}

// PrometheusPublisherMetrics is a PublisherMetrics backed by Prometheus gauges,
// histograms, and counters registered lazily into the provided prometheus.Registerer.
//
// Mandatory metrics:
//   - publisher_in_flight{exchange}
//   - publisher_publish_seconds{exchange,outcome}
//   - publisher_retry_total{exchange,reason}
type PrometheusPublisherMetrics struct {
	inFlight         *prometheus.GaugeVec
	publishSeconds   *prometheus.HistogramVec
	retryTotal       *prometheus.CounterVec
	labelRoutingKey  bool
	labelMessageType bool
}

// NewPrometheusPublisherMetrics creates a PrometheusPublisherMetrics and registers all
// mandatory collectors into reg. If buckets is nil, DefaultHistogramBuckets() is used
// (a fresh slice per call so callers cannot corrupt shared state).
// Returns an error if any collector is already registered.
//
// enabled opts in to high-cardinality labels on the publisher_publish_seconds
// histogram: MetricsLabelRoutingKey adds routing_key, MetricsLabelMessageType
// adds message_type. They are off by default (see MetricsLabel).
func NewPrometheusPublisherMetrics(reg prometheus.Registerer, buckets []float64, enabled ...MetricsLabel) (*PrometheusPublisherMetrics, error) {
	if buckets == nil {
		buckets = DefaultHistogramBuckets()
	}
	m := &PrometheusPublisherMetrics{}
	for _, l := range enabled {
		switch l {
		case MetricsLabelRoutingKey:
			m.labelRoutingKey = true
		case MetricsLabelMessageType:
			m.labelMessageType = true
		}
	}
	publishLabels := []string{"exchange", "outcome"}
	if m.labelRoutingKey {
		publishLabels = append(publishLabels, "routing_key")
	}
	if m.labelMessageType {
		publishLabels = append(publishLabels, "message_type")
	}
	m.inFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "publisher_in_flight",
		Help: "Current number of AMQP publishes in flight.",
	}, []string{"exchange"})
	m.publishSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "publisher_publish_seconds",
		Help:    "Duration in seconds of AMQP publish operations.",
		Buckets: buckets,
	}, publishLabels)
	m.retryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "publisher_retry_total",
		Help: "Total number of AMQP publish retries.",
	}, []string{"exchange", "reason"})
	for _, c := range []prometheus.Collector{m.inFlight, m.publishSeconds, m.retryTotal} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// InFlightAdd adjusts the publisher_in_flight gauge for the given exchange by delta.
func (m *PrometheusPublisherMetrics) InFlightAdd(exchange string, delta int64) {
	m.inFlight.WithLabelValues(exchange).Add(float64(delta))
}

// RecordPublish records d into the publisher_publish_seconds histogram. routingKey
// and messageType are appended as label values only for the labels enabled at
// construction; the order matches the registered label set (exchange, outcome,
// [routing_key], [message_type]).
func (m *PrometheusPublisherMetrics) RecordPublish(exchange, routingKey, messageType, outcome string, d time.Duration) {
	secs := d.Seconds()
	switch {
	case m.labelRoutingKey && m.labelMessageType:
		m.publishSeconds.WithLabelValues(exchange, outcome, routingKey, messageType).Observe(secs)
	case m.labelRoutingKey:
		m.publishSeconds.WithLabelValues(exchange, outcome, routingKey).Observe(secs)
	case m.labelMessageType:
		m.publishSeconds.WithLabelValues(exchange, outcome, messageType).Observe(secs)
	default:
		m.publishSeconds.WithLabelValues(exchange, outcome).Observe(secs)
	}
}

// RecordRetry increments publisher_retry_total for the given exchange and reason.
func (m *PrometheusPublisherMetrics) RecordRetry(exchange, reason string) {
	m.retryTotal.WithLabelValues(exchange, reason).Inc()
}

// PrometheusConsumerMetrics is a ConsumerMetrics backed by Prometheus counters and
// histograms registered lazily into the provided prometheus.Registerer.
//
// Mandatory metrics:
//   - consumer_resubscribed_total{queue}
//   - consumer_handler_aborted_channel_closed_total{queue}
//   - consumer_handler_timeout_total{queue}
//   - consumer_handler_seconds{queue,outcome}
//   - consumer_cancelled_total{queue,reason}
//   - consumer_max_redeliveries_total{queue,cause}
//   - replier_drop_no_dlx_total{queue}
type PrometheusConsumerMetrics struct {
	resubscribedTotal    *prometheus.CounterVec
	handlerAbortedTotal  *prometheus.CounterVec
	handlerTimeoutTotal  *prometheus.CounterVec
	handlerSeconds       *prometheus.HistogramVec
	cancelledTotal       *prometheus.CounterVec
	maxRedeliveriesTotal *prometheus.CounterVec
	replierDropNoDLX     *prometheus.CounterVec
	labelMessageType     bool
}

// NewPrometheusConsumerMetrics creates a PrometheusConsumerMetrics and registers all
// mandatory collectors into reg. If buckets is nil, DefaultHistogramBuckets() is used
// (a fresh slice per call so callers cannot corrupt shared state).
// Returns an error if any collector is already registered.
//
// Cardinality note: consumer_cancelled_total uses the consumer tag as the "reason"
// label. Auto-generated tags (ctag-<uuidv7>) create one new time series per
// cancellation event; in high-churn environments this can cause unbounded Prometheus
// cardinality. See LATER-22 for the planned remediation in T36.
//
// enabled opts in to high-cardinality labels on the consumer_handler_seconds
// histogram: MetricsLabelMessageType adds message_type. MetricsLabelRoutingKey
// is accepted but ignored for consumers (a per-delivery routing key is not a
// stable consumer dimension).
func NewPrometheusConsumerMetrics(reg prometheus.Registerer, buckets []float64, enabled ...MetricsLabel) (*PrometheusConsumerMetrics, error) {
	if buckets == nil {
		buckets = DefaultHistogramBuckets()
	}
	var labelMessageType bool
	for _, l := range enabled {
		if l == MetricsLabelMessageType {
			labelMessageType = true
		}
	}
	handlerLabels := []string{"queue", "outcome"}
	if labelMessageType {
		handlerLabels = append(handlerLabels, "message_type")
	}
	m := &PrometheusConsumerMetrics{
		labelMessageType: labelMessageType,
		resubscribedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_resubscribed_total",
			Help: "Total number of AMQP consumer resubscriptions after reconnect.",
		}, []string{"queue"}),
		handlerAbortedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_handler_aborted_channel_closed_total",
			Help: "Total number of AMQP consumer handlers aborted due to channel closure.",
		}, []string{"queue"}),
		handlerTimeoutTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_handler_timeout_total",
			Help: "Total number of AMQP consumer handler timeouts.",
		}, []string{"queue"}),
		handlerSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "consumer_handler_seconds",
			Help:    "Duration in seconds of AMQP consumer handler executions.",
			Buckets: buckets,
		}, handlerLabels),
		cancelledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_cancelled_total",
			Help: "Total number of broker-initiated consumer cancellations (basic.cancel).",
		}, []string{"queue", "reason"}),
		maxRedeliveriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_max_redeliveries_total",
			Help: "Total number of messages that exceeded MaxRedeliveries, by enforcement cause.",
		}, []string{"queue", "cause"}),
		replierDropNoDLX: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "replier_drop_no_dlx_total",
			Help: "Total number of Replier messages dropped due to missing dead-letter exchange.",
		}, []string{"queue"}),
	}
	for _, c := range []prometheus.Collector{
		m.resubscribedTotal,
		m.handlerAbortedTotal,
		m.handlerTimeoutTotal,
		m.handlerSeconds,
		m.cancelledTotal,
		m.maxRedeliveriesTotal,
		m.replierDropNoDLX,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// RecordResubscribed increments consumer_resubscribed_total for the given queue.
func (m *PrometheusConsumerMetrics) RecordResubscribed(queue string) {
	m.resubscribedTotal.WithLabelValues(queue).Inc()
}

// RecordHandlerAbortedChannelClosed increments consumer_handler_aborted_channel_closed_total.
func (m *PrometheusConsumerMetrics) RecordHandlerAbortedChannelClosed(queue string) {
	m.handlerAbortedTotal.WithLabelValues(queue).Inc()
}

// RecordHandlerTimeout increments consumer_handler_timeout_total for the given queue.
func (m *PrometheusConsumerMetrics) RecordHandlerTimeout(queue string) {
	m.handlerTimeoutTotal.WithLabelValues(queue).Inc()
}

// RecordHandler records d into the consumer_handler_seconds histogram. messageType
// is appended as a label value only when MetricsLabelMessageType was enabled at
// construction; the order matches the registered label set (queue, outcome,
// [message_type]).
func (m *PrometheusConsumerMetrics) RecordHandler(queue, messageType, outcome string, d time.Duration) {
	if m.labelMessageType {
		m.handlerSeconds.WithLabelValues(queue, outcome, messageType).Observe(d.Seconds())
		return
	}
	m.handlerSeconds.WithLabelValues(queue, outcome).Observe(d.Seconds())
}

// RecordCancelled increments consumer_cancelled_total for the given queue and reason.
// reason is the consumer tag reported by the broker's basic.cancel frame.
func (m *PrometheusConsumerMetrics) RecordCancelled(queue, reason string) {
	m.cancelledTotal.WithLabelValues(queue, reason).Inc()
}

// RecordMaxRedeliveries increments consumer_max_redeliveries_total for the given queue and cause.
func (m *PrometheusConsumerMetrics) RecordMaxRedeliveries(queue, cause string) {
	m.maxRedeliveriesTotal.WithLabelValues(queue, cause).Inc()
}

// RecordReplierDropNoDLX increments replier_drop_no_dlx_total for the given queue.
func (m *PrometheusConsumerMetrics) RecordReplierDropNoDLX(queue string) {
	m.replierDropNoDLX.WithLabelValues(queue).Inc()
}
