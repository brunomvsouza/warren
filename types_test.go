package warren_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	amqp "github.com/brunomvsouza/warren"
)

func TestDeliveryModeValues(t *testing.T) {
	assert.Equal(t, amqp.DeliveryMode(0), amqp.DeliveryModePersistent, "DeliveryModePersistent must be zero value")
	assert.NotEqual(t, amqp.DeliveryModePersistent, amqp.DeliveryModeTransient)
}

func TestExchangeKindValues(t *testing.T) {
	tests := []struct {
		name string
		got  amqp.ExchangeKind
		want string
	}{
		{"Direct", amqp.ExchangeDirect, "direct"},
		{"Fanout", amqp.ExchangeFanout, "fanout"},
		{"Topic", amqp.ExchangeTopic, "topic"},
		{"Headers", amqp.ExchangeHeaders, "headers"},
		{"Delayed", amqp.ExchangeDelayed, "x-delayed-message"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, amqp.ExchangeKind(tc.want), tc.got)
		})
	}
}

func TestQueueTypeValues(t *testing.T) {
	tests := []struct {
		name string
		got  amqp.QueueType
		want string
	}{
		{"Classic", amqp.QueueTypeClassic, "classic"},
		{"Quorum", amqp.QueueTypeQuorum, "quorum"},
		{"Stream", amqp.QueueTypeStream, "stream"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, amqp.QueueType(tc.want), tc.got)
		})
	}
}

func TestOverflowPolicyValues(t *testing.T) {
	tests := []struct {
		name string
		got  amqp.OverflowPolicy
		want string
	}{
		{"DropHead", amqp.OverflowDropHead, "drop-head"},
		{"RejectPublish", amqp.OverflowRejectPublish, "reject-publish"},
		{"RejectPublishDLX", amqp.OverflowRejectPublishDLX, "reject-publish-dlx"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, amqp.OverflowPolicy(tc.want), tc.got)
		})
	}
}

func TestSASLMechanismValues(t *testing.T) {
	tests := []struct {
		name string
		got  amqp.SASLMechanism
		want string
	}{
		{"Plain", amqp.SASLPlain, "PLAIN"},
		{"External", amqp.SASLExternal, "EXTERNAL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, amqp.SASLMechanism(tc.want), tc.got)
		})
	}
}

func TestTimeoutVerdictValues(t *testing.T) {
	assert.Equal(t, amqp.TimeoutVerdict(0), amqp.TimeoutNackNoRequeue, "TimeoutNackNoRequeue must be zero value (iota)")
	assert.NotEqual(t, amqp.TimeoutNackNoRequeue, amqp.TimeoutNackRequeue)
}
