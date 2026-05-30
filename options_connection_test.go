package warren

// White-box tests for the remaining Connection options surfaced by T34:
// WithOnResubscribe wiring plus the client-properties defaults baked into
// every dialled socket. Package warren (not warren_test) to reach the
// unexported connOptions, notifyResubscribed, and buildClientProperties.

import (
	"runtime"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"

	"github.com/brunomvsouza/warren/metrics"
)

func TestWithOnResubscribe_setsCallback(t *testing.T) {
	var got string
	o := &connOptions{}

	WithOnResubscribe(func(queue string) { got = queue })(o)

	if assert.NotNil(t, o.onResubscribe, "WithOnResubscribe must set the callback") {
		o.onResubscribe("orders")
	}
	assert.Equal(t, "orders", got)
}

func TestWithOnResubscribe_lastWins(t *testing.T) {
	o := &connOptions{}

	WithOnResubscribe(func(string) { t.Fatal("first callback must be overwritten") })(o)
	var second bool
	WithOnResubscribe(func(string) { second = true })(o)

	o.onResubscribe("q")
	assert.True(t, second, "last WithOnResubscribe wins")
}

// recordingResubMetrics records every RecordResubscribed call while delegating
// the rest of the ConsumerMetrics surface to the no-op implementation.
type recordingResubMetrics struct {
	metrics.NoOpConsumerMetrics
	queues []string
}

func (r *recordingResubMetrics) RecordResubscribed(queue string) {
	r.queues = append(r.queues, queue)
}

func TestNotifyResubscribed_firesMetricAndCallback(t *testing.T) {
	mc := newBareManaged(t)
	var cbQueue string
	mc.opts.onResubscribe = func(q string) { cbQueue = q }
	rec := &recordingResubMetrics{}

	notifyResubscribed(mc, rec, "orders")

	assert.Equal(t, []string{"orders"}, rec.queues, "metric must be recorded")
	assert.Equal(t, "orders", cbQueue, "callback must fire with the queue name")
}

func TestNotifyResubscribed_nilCallback_onlyRecordsMetric(t *testing.T) {
	mc := newBareManaged(t)
	rec := &recordingResubMetrics{}

	assert.NotPanics(t, func() {
		notifyResubscribed(mc, rec, "orders")
	})
	assert.Equal(t, []string{"orders"}, rec.queues)
}

func TestBuildClientProperties_includesDefaults(t *testing.T) {
	props := buildClientProperties(&connOptions{}, "warren-host-1-pub-0")

	assert.Equal(t, "warren-host-1-pub-0", props["connection_name"])
	assert.Equal(t, "Warren", props["product"])
	assert.Equal(t, "Go "+runtime.Version(), props["platform"])
	assert.NotEmpty(t, props["version"], "version must be sourced from build info")
}

func TestBuildClientProperties_userPropertiesOverrideDefaults(t *testing.T) {
	opts := &connOptions{
		clientProperties: amqp091.Table{"product": "custom-app", "team": "payments"},
	}

	props := buildClientProperties(opts, "n")

	assert.Equal(t, "custom-app", props["product"], "user clientProperties override the defaults")
	assert.Equal(t, "payments", props["team"], "extra user keys are preserved")
	assert.Equal(t, "n", props["connection_name"])
	assert.Equal(t, "Go "+runtime.Version(), props["platform"])
}

func TestWarrenVersion_nonEmpty(t *testing.T) {
	assert.NotEmpty(t, warrenVersion(), "warrenVersion must always return a non-empty token")
}
