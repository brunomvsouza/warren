package amqperror_test

import (
	"errors"
	"fmt"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/internal/amqperror"
)

// rawAMQPErr builds a *amqp091.Error as the broker would deliver it.
func rawAMQPErr(code uint16, reason string) *amqp091.Error {
	return &amqp091.Error{Code: int(code), Reason: reason}
}

// — Wrap: nil passthrough ——————————————————————————————————————————————————

func TestWrap_nil(t *testing.T) {
	assert.Nil(t, amqperror.Wrap(nil))
}

// — Wrap: non-AMQP error passes through unchanged —————————————————————————

func TestWrap_nonAMQPError_passthrough(t *testing.T) {
	plain := errors.New("something else")
	got := amqperror.Wrap(plain)
	assert.Same(t, plain, got, "non-amqp091.Error must be returned unchanged")
}

// — Wrap: all 15 channel/connection-close codes ——————————————————————————

var codeToSentinel = []struct {
	code     uint16
	sentinel error
}{
	{311, warren.ErrContentTooLarge},
	{320, warren.ErrConnectionForced},
	{402, warren.ErrInvalidPath},
	{403, warren.ErrAccessRefused},
	{404, warren.ErrNotFound},
	{405, warren.ErrResourceLocked},
	{406, warren.ErrPreconditionFailed},
	{501, warren.ErrFrameError},
	{502, warren.ErrSyntaxError},
	{503, warren.ErrCommandInvalid},
	{504, warren.ErrChannelError},
	{505, warren.ErrUnexpectedFrame},
	{506, warren.ErrResourceError},
	{530, warren.ErrNotAllowed},
	{540, warren.ErrNotImplemented},
	{541, warren.ErrInternalError},
}

func TestWrap_allChannelCloseCodes(t *testing.T) {
	for _, tc := range codeToSentinel {
		t.Run(fmt.Sprintf("code_%d", tc.code), func(t *testing.T) {
			raw := rawAMQPErr(tc.code, "test reason")
			wrapped := amqperror.Wrap(raw)

			require.NotNil(t, wrapped)
			assert.True(t, errors.Is(wrapped, tc.sentinel),
				"errors.Is must find sentinel %v for code %d", tc.sentinel, tc.code)
		})
	}
}

// — Wrap: original *amqp091.Error still reachable (chain length 2) ————————

func TestWrap_originalErrorInChain(t *testing.T) {
	raw := rawAMQPErr(404, "no queue 'foo'")
	wrapped := amqperror.Wrap(raw)

	var amqpErr *amqp091.Error
	require.True(t, errors.As(wrapped, &amqpErr),
		"errors.As must find the original *amqp091.Error in the chain")
	assert.Equal(t, 404, amqpErr.Code)
	assert.Equal(t, "no queue 'foo'", amqpErr.Reason)
}

// — Wrap: AMQPCode extracts the code from the wrapped error ———————————————

func TestWrap_AMQPCodeExtraction(t *testing.T) {
	for _, tc := range codeToSentinel {
		t.Run(fmt.Sprintf("code_%d", tc.code), func(t *testing.T) {
			wrapped := amqperror.Wrap(rawAMQPErr(tc.code, "reason"))
			code, ok := warren.AMQPCode(wrapped)
			assert.True(t, ok)
			assert.Equal(t, tc.code, code)
		})
	}
}

// — Wrap: IsTransient / IsPermanent classifiers work on wrapped errors ————

func TestWrap_classifiers(t *testing.T) {
	transientCodes := []uint16{320, 504, 541}
	permanentCodes := []uint16{311, 402, 403, 404, 405, 406, 501, 502, 503, 505, 506, 530, 540}

	for _, code := range transientCodes {
		t.Run(fmt.Sprintf("transient_%d", code), func(t *testing.T) {
			wrapped := amqperror.Wrap(rawAMQPErr(code, "reason"))
			assert.True(t, warren.IsTransient(wrapped), "code %d must be transient", code)
			assert.False(t, warren.IsPermanent(wrapped), "code %d must not be permanent", code)
		})
	}

	for _, code := range permanentCodes {
		t.Run(fmt.Sprintf("permanent_%d", code), func(t *testing.T) {
			wrapped := amqperror.Wrap(rawAMQPErr(code, "reason"))
			assert.True(t, warren.IsPermanent(wrapped), "code %d must be permanent", code)
			assert.False(t, warren.IsTransient(wrapped), "code %d must not be transient", code)
		})
	}
}

// — Wrap: 506 is permanent (not transient) ————————————————————————————————

func TestWrap_resourceError506_isPermanent(t *testing.T) {
	wrapped := amqperror.Wrap(rawAMQPErr(506, "fd exhausted"))
	assert.False(t, warren.IsTransient(wrapped), "506 ErrResourceError must NOT be transient")
	assert.True(t, warren.IsPermanent(wrapped), "506 ErrResourceError must be permanent")
}

// — Wrap: unknown code passes through unchanged ———————————————————————————

func TestWrap_unknownCode_passthrough(t *testing.T) {
	unknown := rawAMQPErr(999, "hypothetical future code")
	got := amqperror.Wrap(unknown)
	// Should pass through — we don't know this code.
	var amqpErr *amqp091.Error
	require.True(t, errors.As(got, &amqpErr))
	assert.Equal(t, 999, amqpErr.Code)
}

// — Wrap: nested amqp091.Error inside another error ———————————————————————

func TestWrap_nestedAMQPError(t *testing.T) {
	raw := rawAMQPErr(403, "access refused")
	outer := fmt.Errorf("channel closed: %w", raw)
	wrapped := amqperror.Wrap(outer)

	assert.True(t, errors.Is(wrapped, warren.ErrAccessRefused),
		"Wrap must detect *amqp091.Error nested via errors.As")
	var amqpErr *amqp091.Error
	require.True(t, errors.As(wrapped, &amqpErr))
	assert.Equal(t, 403, amqpErr.Code)
}
