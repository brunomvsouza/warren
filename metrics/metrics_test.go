package metrics_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/metrics"
)

// compile-time interface checks
var (
	_ metrics.ClientMetrics    = metrics.NoOpClientMetrics{}
	_ metrics.PublisherMetrics = metrics.NoOpPublisherMetrics{}
	_ metrics.ConsumerMetrics  = metrics.NoOpConsumerMetrics{}
)

// — DefaultHistogramBuckets ————————————————————————————————————————————————

func TestDefaultHistogramBuckets_values(t *testing.T) {
	b := metrics.DefaultHistogramBuckets()
	assert.Len(t, b, 12)
	assert.Equal(t, 0.5, b[0])
	assert.Equal(t, 5000.0, b[len(b)-1])
}

func TestDefaultHistogramBuckets_freshSlicePerCall(t *testing.T) {
	a := metrics.DefaultHistogramBuckets()
	b := metrics.DefaultHistogramBuckets()
	a[0] = 999
	assert.Equal(t, 0.5, b[0], "mutating one call's slice must not affect another call")
}

// — NoOp: zero allocations —————————————————————————————————————————————————

func TestNoOpClientMetrics_zeroAllocs(t *testing.T) {
	m := metrics.NoOpClientMetrics{}
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		m.RecordReconnect("publisher")
		m.RecordBlocked("publisher", time.Second)
		m.RecordTopologyRedeclare("publisher", 50*time.Millisecond)
		m.RecordDegraded("publisher", "topology_failed")
	}))
}

func TestNoOpPublisherMetrics_zeroAllocs(t *testing.T) {
	m := metrics.NoOpPublisherMetrics{}
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		m.InFlightAdd("events", 1)
		m.RecordPublish("events", "", "", "ack", 5*time.Millisecond)
		m.RecordRetry("events", "nacked")
		m.InFlightAdd("events", -1)
	}))
}

func TestNoOpConsumerMetrics_zeroAllocs(t *testing.T) {
	m := metrics.NoOpConsumerMetrics{}
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		m.RecordResubscribed("orders")
		m.RecordHandlerAbortedChannelClosed("orders")
		m.RecordHandlerTimeout("orders")
		m.RecordHandler("orders", "", "ack", 3*time.Millisecond)
		m.RecordReplierDropNoDLX("orders")
		m.RecordCancelled("orders", "broker_initiated")
		m.RecordMaxRedeliveries("orders", "x-death")
	}))
}

// — Prometheus: helper ————————————————————————————————————————————————————

func gatherNames(t *testing.T, reg *prometheus.Registry) map[string]struct{} {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = struct{}{}
	}
	return names
}

// — Prometheus: ClientMetrics ————————————————————————————————————————————

func TestPrometheusClientMetrics_mandatoryMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusClientMetrics(reg, nil)
	require.NoError(t, err)

	m.RecordReconnect("publisher")
	m.RecordBlocked("consumer", 2*time.Second)
	m.RecordTopologyRedeclare("publisher", 80*time.Millisecond)
	m.RecordDegraded("publisher", "topology_failed")

	names := gatherNames(t, reg)
	assert.Contains(t, names, "connection_reconnects_total")
	assert.Contains(t, names, "connection_blocked_seconds")
	assert.Contains(t, names, "topology_redeclare_seconds")
	assert.Contains(t, names, "connection_degraded_total")
}

func TestPrometheusClientMetrics_roleLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusClientMetrics(reg, nil)
	require.NoError(t, err)

	m.RecordReconnect("consumer")

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 1, "only one metric family registered before other methods called")
	metric := mfs[0].GetMetric()
	require.Len(t, metric, 1)
	labels := metric[0].GetLabel()
	require.Len(t, labels, 1)
	assert.Equal(t, "role", labels[0].GetName())
	assert.Equal(t, "consumer", labels[0].GetValue())
}

func TestPrometheusClientMetrics_customBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	_, err := metrics.NewPrometheusClientMetrics(reg, []float64{1, 10, 100})
	require.NoError(t, err)
}

func TestPrometheusClientMetrics_doubleRegisterError(t *testing.T) {
	reg := prometheus.NewRegistry()
	_, err := metrics.NewPrometheusClientMetrics(reg, nil)
	require.NoError(t, err)
	_, err = metrics.NewPrometheusClientMetrics(reg, nil)
	assert.Error(t, err)
}

// — Prometheus: PublisherMetrics —————————————————————————————————————————

func TestPrometheusPublisherMetrics_mandatoryMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusPublisherMetrics(reg, nil)
	require.NoError(t, err)

	m.InFlightAdd("events", 3)
	m.RecordPublish("events", "", "", "ack", 10*time.Millisecond)
	m.RecordRetry("events", "nacked")
	m.InFlightAdd("events", -3)

	names := gatherNames(t, reg)
	assert.Contains(t, names, "publisher_in_flight")
	assert.Contains(t, names, "publisher_publish_seconds")
	assert.Contains(t, names, "publisher_retry_total")
}

func TestPrometheusPublisherMetrics_inFlightGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusPublisherMetrics(reg, nil)
	require.NoError(t, err)

	m.InFlightAdd("orders", 5)
	m.InFlightAdd("orders", -2)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range mfs {
		if mf.GetName() == "publisher_in_flight" {
			found = true
			assert.InEpsilon(t, 3.0, mf.GetMetric()[0].GetGauge().GetValue(), 0.001)
		}
	}
	assert.True(t, found, "publisher_in_flight metric not found")
}

// labelsForMetric returns the label name→value map of the first sample of the
// named metric family, or nil if the family is absent.
func labelsForMetric(t *testing.T, reg *prometheus.Registry, name string) map[string]string {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		require.NotEmpty(t, mf.GetMetric(), "%s has no samples", name)
		out := make(map[string]string)
		for _, lp := range mf.GetMetric()[0].GetLabel() {
			out[lp.GetName()] = lp.GetValue()
		}
		return out
	}
	return nil
}

// — Prometheus: opt-in high-cardinality labels ——————————————————————————————

func TestPrometheusPublisherMetrics_optionalLabelsEmittedWhenEnabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusPublisherMetrics(reg, nil,
		metrics.MetricsLabelRoutingKey, metrics.MetricsLabelMessageType)
	require.NoError(t, err)

	m.RecordPublish("events", "orders.created", "OrderCreated", "ack", 10*time.Millisecond)

	labels := labelsForMetric(t, reg, "publisher_publish_seconds")
	assert.Equal(t, "events", labels["exchange"])
	assert.Equal(t, "ack", labels["outcome"])
	assert.Equal(t, "orders.created", labels["routing_key"])
	assert.Equal(t, "OrderCreated", labels["message_type"])
}

func TestPrometheusPublisherMetrics_noOptionalLabelsByDefault(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusPublisherMetrics(reg, nil)
	require.NoError(t, err)

	m.RecordPublish("events", "orders.created", "OrderCreated", "ack", 10*time.Millisecond)

	labels := labelsForMetric(t, reg, "publisher_publish_seconds")
	assert.Equal(t, "events", labels["exchange"])
	assert.Equal(t, "ack", labels["outcome"])
	assert.NotContains(t, labels, "routing_key")
	assert.NotContains(t, labels, "message_type")
}

func TestPrometheusPublisherMetrics_routingKeyOnly(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusPublisherMetrics(reg, nil, metrics.MetricsLabelRoutingKey)
	require.NoError(t, err)

	m.RecordPublish("events", "orders.created", "OrderCreated", "ack", time.Millisecond)

	labels := labelsForMetric(t, reg, "publisher_publish_seconds")
	assert.Equal(t, "orders.created", labels["routing_key"])
	assert.NotContains(t, labels, "message_type")
}

func TestPrometheusPublisherMetrics_messageTypeOnly(t *testing.T) {
	// Exercises the message-type-only RecordPublish branch: the histogram is
	// registered as {exchange, outcome, message_type}, so a label-order bug here
	// would panic with a Prometheus cardinality mismatch.
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusPublisherMetrics(reg, nil, metrics.MetricsLabelMessageType)
	require.NoError(t, err)

	m.RecordPublish("events", "orders.created", "OrderCreated", "ack", time.Millisecond)

	labels := labelsForMetric(t, reg, "publisher_publish_seconds")
	assert.Equal(t, "OrderCreated", labels["message_type"])
	assert.NotContains(t, labels, "routing_key")
}

func TestPrometheusConsumerMetrics_messageTypeLabelWhenEnabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusConsumerMetrics(reg, nil, metrics.MetricsLabelMessageType)
	require.NoError(t, err)

	m.RecordHandler("orders", "OrderCreated", "ack", 5*time.Millisecond)

	labels := labelsForMetric(t, reg, "consumer_handler_seconds")
	assert.Equal(t, "orders", labels["queue"])
	assert.Equal(t, "ack", labels["outcome"])
	assert.Equal(t, "OrderCreated", labels["message_type"])
}

func TestPrometheusConsumerMetrics_noMessageTypeByDefault(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusConsumerMetrics(reg, nil)
	require.NoError(t, err)

	m.RecordHandler("orders", "OrderCreated", "ack", 5*time.Millisecond)

	labels := labelsForMetric(t, reg, "consumer_handler_seconds")
	assert.NotContains(t, labels, "message_type")
}

func TestPrometheusConsumerMetrics_routingKeyIgnored(t *testing.T) {
	// MetricsLabelRoutingKey is accepted but not applied to consumer metrics
	// (a per-delivery routing key is not a stable consumer dimension).
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusConsumerMetrics(reg, nil, metrics.MetricsLabelRoutingKey)
	require.NoError(t, err)

	m.RecordHandler("orders", "OrderCreated", "ack", 5*time.Millisecond)

	labels := labelsForMetric(t, reg, "consumer_handler_seconds")
	assert.NotContains(t, labels, "routing_key")
	assert.NotContains(t, labels, "message_type")
}

// — Prometheus: ConsumerMetrics —————————————————————————————————————————

func TestPrometheusConsumerMetrics_mandatoryMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusConsumerMetrics(reg, nil)
	require.NoError(t, err)

	m.RecordResubscribed("orders")
	m.RecordHandlerAbortedChannelClosed("orders")
	m.RecordHandlerTimeout("orders")
	m.RecordHandler("orders", "", "ack", 5*time.Millisecond)
	m.RecordReplierDropNoDLX("requests")
	m.RecordCancelled("orders", "broker_initiated")
	m.RecordMaxRedeliveries("orders", "x-death")

	names := gatherNames(t, reg)
	assert.Contains(t, names, "consumer_resubscribed_total")
	assert.Contains(t, names, "consumer_handler_aborted_channel_closed_total")
	assert.Contains(t, names, "consumer_handler_timeout_total")
	assert.Contains(t, names, "consumer_handler_seconds")
	assert.Contains(t, names, "replier_drop_no_dlx_total")
	assert.Contains(t, names, "consumer_cancelled_total")
	assert.Contains(t, names, "consumer_max_redeliveries_total")
}

func TestPrometheusConsumerMetrics_maxRedeliveriesTotal(t *testing.T) {
	// RecordMaxRedeliveries must increment consumer_max_redeliveries_total{queue,cause}.
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusConsumerMetrics(reg, nil)
	require.NoError(t, err)

	m.RecordMaxRedeliveries("orders", "x-death")
	m.RecordMaxRedeliveries("orders", "x-death")
	m.RecordMaxRedeliveries("orders", "in-process")

	names := gatherNames(t, reg)
	assert.Contains(t, names, "consumer_max_redeliveries_total")

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var xDeathVal, inProcessVal float64
	for _, mf := range mfs {
		if mf.GetName() != "consumer_max_redeliveries_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			var q, cause string
			for _, lp := range metric.GetLabel() {
				switch lp.GetName() {
				case "queue":
					q = lp.GetValue()
				case "cause":
					cause = lp.GetValue()
				}
			}
			if q != "orders" {
				continue
			}
			switch cause {
			case "x-death":
				xDeathVal = metric.GetCounter().GetValue()
			case "in-process":
				inProcessVal = metric.GetCounter().GetValue()
			}
		}
	}
	assert.InDelta(t, 2.0, xDeathVal, 0.001, "x-death counter must be 2")
	assert.InDelta(t, 1.0, inProcessVal, 0.001, "in-process counter must be 1")
}

func TestPrometheusConsumerMetrics_cancelledTotalIncrement(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := metrics.NewPrometheusConsumerMetrics(reg, nil)
	require.NoError(t, err)

	m.RecordCancelled("orders", "ctag-abc")
	m.RecordCancelled("orders", "ctag-abc")

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "consumer_cancelled_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			var q, r string
			for _, lp := range metric.GetLabel() {
				switch lp.GetName() {
				case "queue":
					q = lp.GetValue()
				case "reason":
					r = lp.GetValue()
				}
			}
			if q == "orders" && r == "ctag-abc" {
				assert.InDelta(t, 2.0, metric.GetCounter().GetValue(), 0.001,
					"consumer_cancelled_total{queue=orders,reason=ctag-abc} must be 2")
				found = true
			}
		}
	}
	assert.True(t, found, "consumer_cancelled_total{queue=orders,reason=ctag-abc} not found")
}

// — Prometheus: integration workload ————————————————————————————————————

func TestPrometheus_integrationWorkload(t *testing.T) {
	reg := prometheus.NewRegistry()

	cm, err := metrics.NewPrometheusClientMetrics(reg, nil)
	require.NoError(t, err)

	pm, err := metrics.NewPrometheusPublisherMetrics(reg, nil)
	require.NoError(t, err)

	conm, err := metrics.NewPrometheusConsumerMetrics(reg, nil)
	require.NoError(t, err)

	// canned workload — simulate activity across all three metrics
	for i := range 10 {
		cm.RecordReconnect("publisher")
		cm.RecordBlocked("publisher", time.Duration(i+1)*100*time.Millisecond)
		cm.RecordTopologyRedeclare("publisher", 40*time.Millisecond)
		cm.RecordDegraded("publisher", "topology_failed")

		pm.InFlightAdd("events", 1)
		pm.RecordPublish("events", "", "", "ack", time.Duration(i+1)*time.Millisecond)
		pm.RecordRetry("events", "nacked")
		pm.InFlightAdd("events", -1)

		conm.RecordResubscribed("orders")
		conm.RecordHandlerAbortedChannelClosed("orders")
		conm.RecordHandlerTimeout("orders")
		conm.RecordHandler("orders", "", "ack", time.Duration(i+1)*time.Millisecond)
		conm.RecordReplierDropNoDLX("requests")
		conm.RecordCancelled("orders", "broker_initiated")
		conm.RecordMaxRedeliveries("orders", "x-death")
	}

	names := gatherNames(t, reg)

	mandatory := []string{
		"connection_reconnects_total",
		"connection_blocked_seconds",
		"topology_redeclare_seconds",
		"connection_degraded_total",
		"publisher_in_flight",
		"publisher_publish_seconds",
		"publisher_retry_total",
		"consumer_resubscribed_total",
		"consumer_handler_aborted_channel_closed_total",
		"consumer_handler_timeout_total",
		"consumer_handler_seconds",
		"replier_drop_no_dlx_total",
		"consumer_cancelled_total",
		"consumer_max_redeliveries_total",
	}
	for _, name := range mandatory {
		assert.Contains(t, names, name, "mandatory metric %q missing from gathered output", name)
	}
}

// — Benchmarks ————————————————————————————————————————————————————————————

func BenchmarkNoOpClientMetrics(b *testing.B) {
	m := metrics.NoOpClientMetrics{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.RecordReconnect("publisher")
		m.RecordBlocked("publisher", time.Second)
		m.RecordTopologyRedeclare("publisher", 50*time.Millisecond)
	}
}

func BenchmarkNoOpPublisherMetrics(b *testing.B) {
	m := metrics.NoOpPublisherMetrics{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.InFlightAdd("events", 1)
		m.RecordPublish("events", "", "", "ack", 5*time.Millisecond)
		m.RecordRetry("events", "nacked")
		m.InFlightAdd("events", -1)
	}
}

func BenchmarkNoOpConsumerMetrics(b *testing.B) {
	m := metrics.NoOpConsumerMetrics{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.RecordResubscribed("orders")
		m.RecordHandlerAbortedChannelClosed("orders")
		m.RecordHandlerTimeout("orders")
		m.RecordHandler("orders", "", "ack", 3*time.Millisecond)
		m.RecordReplierDropNoDLX("orders")
		m.RecordCancelled("orders", "ctag-test")
		m.RecordMaxRedeliveries("orders", "x-death")
	}
}
