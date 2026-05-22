package warren_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/brunomvsouza/warren"
)

func TestDeliveryModeValues(t *testing.T) {
	assert.Equal(t, warren.DeliveryMode(0), warren.DeliveryModePersistent, "DeliveryModePersistent must be zero value")
	assert.NotEqual(t, warren.DeliveryModePersistent, warren.DeliveryModeTransient)
}

func TestExchangeKindValues(t *testing.T) {
	tests := []struct {
		name string
		got  warren.ExchangeKind
		want string
	}{
		{"Direct", warren.ExchangeDirect, "direct"},
		{"Fanout", warren.ExchangeFanout, "fanout"},
		{"Topic", warren.ExchangeTopic, "topic"},
		{"Headers", warren.ExchangeHeaders, "headers"},
		{"Delayed", warren.ExchangeDelayed, "x-delayed-message"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, warren.ExchangeKind(tc.want), tc.got)
		})
	}
}

func TestQueueTypeValues(t *testing.T) {
	tests := []struct {
		name string
		got  warren.QueueType
		want string
	}{
		{"Classic", warren.QueueTypeClassic, "classic"},
		{"Quorum", warren.QueueTypeQuorum, "quorum"},
		{"Stream", warren.QueueTypeStream, "stream"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, warren.QueueType(tc.want), tc.got)
		})
	}
}

func TestOverflowPolicyValues(t *testing.T) {
	tests := []struct {
		name string
		got  warren.OverflowPolicy
		want string
	}{
		{"DropHead", warren.OverflowDropHead, "drop-head"},
		{"RejectPublish", warren.OverflowRejectPublish, "reject-publish"},
		{"RejectPublishDLX", warren.OverflowRejectPublishDLX, "reject-publish-dlx"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, warren.OverflowPolicy(tc.want), tc.got)
		})
	}
}

func TestSASLMechanismValues(t *testing.T) {
	tests := []struct {
		name string
		got  warren.SASLMechanism
		want string
	}{
		{"Plain", warren.SASLPlain, "PLAIN"},
		{"External", warren.SASLExternal, "EXTERNAL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, warren.SASLMechanism(tc.want), tc.got)
		})
	}
}

func TestTimeoutVerdictValues(t *testing.T) {
	assert.Equal(t, warren.TimeoutVerdict(0), warren.TimeoutNackNoRequeue, "TimeoutNackNoRequeue must be zero value (iota)")
	assert.NotEqual(t, warren.TimeoutNackNoRequeue, warren.TimeoutNackRequeue)
}
