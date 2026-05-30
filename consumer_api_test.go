package warren

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// — NoLocal symbol-absence guard (T36, SPEC §6 note + §10 decision 10) ————————
//
// RabbitMQ silently ignores the AMQP 0-9-1 no-local flag on basic.consume.
// Exposing a NoLocal() builder method would be misleading API surface, so it is
// intentionally omitted. These tests fail loudly if the method is ever added back,
// turning the design decision into an enforced contract rather than a comment.

func TestConsumerBuilder_NoLocal_IsAbsent(t *testing.T) {
	_, ok := reflect.TypeFor[*ConsumerBuilder[string]]().MethodByName("NoLocal")
	assert.False(t, ok, "ConsumerBuilder must not expose a NoLocal method (RabbitMQ ignores no-local)")
}

func TestBatchConsumerBuilder_NoLocal_IsAbsent(t *testing.T) {
	_, ok := reflect.TypeFor[*BatchConsumerBuilder[string]]().MethodByName("NoLocal")
	assert.False(t, ok, "BatchConsumerBuilder must not expose a NoLocal method (RabbitMQ ignores no-local)")
}

// — BatchConsumerBuilder remaining options (T36) — round-trip into the builder ——

func TestBatchConsumerBuilder_Exclusive_RoundTrips(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := BatchConsumerFor[string](conn).Queue("q").Exclusive().Build()
	require.NoError(t, err)
	assert.True(t, c.exclusive, "Exclusive() must set the batch consumer exclusive flag")
}

func TestBatchConsumerBuilder_Exclusive_DefaultsOff(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := BatchConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.False(t, c.exclusive, "exclusive must default to false")
}

func TestBatchConsumerBuilder_Args_RoundTrips(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := BatchConsumerFor[string](conn).Queue("q").Args(Headers{"x-custom": "value"}).Build()
	require.NoError(t, err)
	require.NotNil(t, c.consumeArgs)
	assert.Equal(t, "value", c.consumeArgs["x-custom"], "Args() must round-trip into the batch consumer consume args")
}

func TestBatchConsumerBuilder_OnCancel_RoundTrips(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := BatchConsumerFor[string](conn).Queue("q").OnCancel(func(_ string) {}).Build()
	require.NoError(t, err)
	assert.NotNil(t, c.onCancel, "OnCancel() must store the callback on the batch consumer")
}

// — buildConsumeArgs (T36) — user Args merge + typed Priority overlay —————————

func TestBuildConsumeArgs_NilWhenEmpty(t *testing.T) {
	assert.Nil(t, buildConsumeArgs(nil, false, 0), "no args and no priority must produce a nil table")
	assert.Nil(t, buildConsumeArgs(Headers{}, false, 0), "empty args and no priority must produce a nil table")
}

func TestBuildConsumeArgs_PriorityOnly(t *testing.T) {
	args := buildConsumeArgs(nil, true, 7)
	require.NotNil(t, args)
	assert.Equal(t, 7, args["x-priority"])
}

func TestBuildConsumeArgs_UserArgsPreserved(t *testing.T) {
	args := buildConsumeArgs(Headers{"x-custom": "v", "x-priority": 1}, false, 0)
	require.NotNil(t, args)
	assert.Equal(t, "v", args["x-custom"])
	assert.Equal(t, 1, args["x-priority"], "without Priority(), a user-supplied x-priority arg is respected")
}

func TestBuildConsumeArgs_TypedPriorityWinsOverUserArg(t *testing.T) {
	// Priority() is the typed escape-from-magic option; when both are set it wins
	// over a raw x-priority slipped through Args().
	args := buildConsumeArgs(Headers{"x-priority": 1, "x-custom": "v"}, true, 9)
	require.NotNil(t, args)
	assert.Equal(t, 9, args["x-priority"], "typed Priority() must overlay a user-supplied x-priority arg")
	assert.Equal(t, "v", args["x-custom"], "unrelated user args must survive the priority overlay")
}

func TestBuildConsumeArgs_DoesNotMutateInput(t *testing.T) {
	in := Headers{"x-custom": "v"}
	_ = buildConsumeArgs(in, true, 3)
	_, hasPriority := in["x-priority"]
	assert.False(t, hasPriority, "buildConsumeArgs must not mutate the caller's Headers")
}
