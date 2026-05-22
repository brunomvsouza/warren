package amqp_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	amqp "github.com/brunomvsouza/amqp"
)

func TestAMQPCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode uint16
		wantOK   bool
	}{
		{name: "nil", err: nil, wantCode: 0, wantOK: false},
		{name: "plain error", err: errors.New("plain"), wantCode: 0, wantOK: false},
		{name: "non-amqp sentinel", err: amqp.ErrNotConnected, wantCode: 0, wantOK: false},

		// Direct AMQP reply-code sentinels.
		{name: "ErrContentTooLarge direct", err: amqp.ErrContentTooLarge, wantCode: 311, wantOK: true},
		{name: "ErrConnectionForced direct", err: amqp.ErrConnectionForced, wantCode: 320, wantOK: true},
		{name: "ErrInvalidPath direct", err: amqp.ErrInvalidPath, wantCode: 402, wantOK: true},
		{name: "ErrAccessRefused direct", err: amqp.ErrAccessRefused, wantCode: 403, wantOK: true},
		{name: "ErrNotFound direct", err: amqp.ErrNotFound, wantCode: 404, wantOK: true},
		{name: "ErrResourceLocked direct", err: amqp.ErrResourceLocked, wantCode: 405, wantOK: true},
		{name: "ErrPreconditionFailed direct", err: amqp.ErrPreconditionFailed, wantCode: 406, wantOK: true},
		{name: "ErrFrameError direct", err: amqp.ErrFrameError, wantCode: 501, wantOK: true},
		{name: "ErrSyntaxError direct", err: amqp.ErrSyntaxError, wantCode: 502, wantOK: true},
		{name: "ErrCommandInvalid direct", err: amqp.ErrCommandInvalid, wantCode: 503, wantOK: true},
		{name: "ErrChannelError direct", err: amqp.ErrChannelError, wantCode: 504, wantOK: true},
		{name: "ErrUnexpectedFrame direct", err: amqp.ErrUnexpectedFrame, wantCode: 505, wantOK: true},
		{name: "ErrResourceError direct", err: amqp.ErrResourceError, wantCode: 506, wantOK: true},
		{name: "ErrNotAllowed direct", err: amqp.ErrNotAllowed, wantCode: 530, wantOK: true},
		{name: "ErrNotImplemented direct", err: amqp.ErrNotImplemented, wantCode: 540, wantOK: true},
		{name: "ErrInternalError direct", err: amqp.ErrInternalError, wantCode: 541, wantOK: true},

		// Wrapped sentinels — code still extractable.
		{name: "ErrNotFound wrapped", err: fmt.Errorf("operation failed: %w", amqp.ErrNotFound), wantCode: 404, wantOK: true},
		{name: "ErrAccessRefused wrapped", err: fmt.Errorf("context: %w", amqp.ErrAccessRefused), wantCode: 403, wantOK: true},
		{name: "ErrPreconditionFailed wrapped", err: fmt.Errorf("ctx: %w", amqp.ErrPreconditionFailed), wantCode: 406, wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotCode, gotOK := amqp.AMQPCode(tc.err)
			assert.Equal(t, tc.wantCode, gotCode)
			assert.Equal(t, tc.wantOK, gotOK)
		})
	}
}

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "plain error", err: errors.New("plain"), want: false},

		// Transient sentinels.
		{name: "ErrChannelPoolExhausted", err: amqp.ErrChannelPoolExhausted, want: true},
		{name: "ErrPublishNacked", err: amqp.ErrPublishNacked, want: true},
		{name: "ErrConnectionBlocked", err: amqp.ErrConnectionBlocked, want: true},
		{name: "ErrConfirmTimeout", err: amqp.ErrConfirmTimeout, want: true},
		{name: "ErrChannelClosed", err: amqp.ErrChannelClosed, want: true},
		{name: "ErrReconnecting", err: amqp.ErrReconnecting, want: true},

		// Transient AMQP reply-code sentinels (320, 504, 541).
		{name: "ErrConnectionForced (320)", err: amqp.ErrConnectionForced, want: true},
		{name: "ErrChannelError (504)", err: amqp.ErrChannelError, want: true},
		{name: "ErrInternalError (541)", err: amqp.ErrInternalError, want: true},

		// ErrTransient wrapper.
		{name: "wrapped ErrTransient", err: fmt.Errorf("ctx: %w", amqp.ErrTransient), want: true},
		{name: "wrapped transient sentinel", err: fmt.Errorf("ctx: %w", amqp.ErrChannelClosed), want: true},

		// Permanent sentinels — must NOT be transient.
		{name: "ErrContentTooLarge (311) is NOT transient", err: amqp.ErrContentTooLarge, want: false},
		{name: "ErrResourceError (506) is NOT transient", err: amqp.ErrResourceError, want: false},
		{name: "ErrNotFound (404)", err: amqp.ErrNotFound, want: false},
		{name: "ErrPreconditionFailed (406)", err: amqp.ErrPreconditionFailed, want: false},
		{name: "ErrTopologyRedeclareFailed", err: amqp.ErrTopologyRedeclareFailed, want: false},
		{name: "ErrInvalidMessage", err: amqp.ErrInvalidMessage, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, amqp.IsTransient(tc.err))
		})
	}
}

func TestIsPermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "plain error", err: errors.New("plain"), want: false},

		// Permanent AMQP reply-code sentinels (311, 402-406, 501-503, 505-506, 530, 540).
		{name: "ErrContentTooLarge (311)", err: amqp.ErrContentTooLarge, want: true},
		{name: "ErrInvalidPath (402)", err: amqp.ErrInvalidPath, want: true},
		{name: "ErrAccessRefused (403)", err: amqp.ErrAccessRefused, want: true},
		{name: "ErrNotFound (404)", err: amqp.ErrNotFound, want: true},
		{name: "ErrResourceLocked (405)", err: amqp.ErrResourceLocked, want: true},
		{name: "ErrPreconditionFailed (406)", err: amqp.ErrPreconditionFailed, want: true},
		{name: "ErrFrameError (501)", err: amqp.ErrFrameError, want: true},
		{name: "ErrSyntaxError (502)", err: amqp.ErrSyntaxError, want: true},
		{name: "ErrCommandInvalid (503)", err: amqp.ErrCommandInvalid, want: true},
		{name: "ErrUnexpectedFrame (505)", err: amqp.ErrUnexpectedFrame, want: true},
		{name: "ErrResourceError (506) is permanent", err: amqp.ErrResourceError, want: true},
		{name: "ErrNotAllowed (530)", err: amqp.ErrNotAllowed, want: true},
		{name: "ErrNotImplemented (540)", err: amqp.ErrNotImplemented, want: true},

		// ErrTopologyRedeclareFailed (permanent).
		{name: "ErrTopologyRedeclareFailed", err: amqp.ErrTopologyRedeclareFailed, want: true},
		{name: "ErrTopologyRedeclareFailed wrapped", err: fmt.Errorf("ctx: %w", amqp.ErrTopologyRedeclareFailed), want: true},

		// ErrPermanent wrapper.
		{name: "wrapped ErrPermanent", err: fmt.Errorf("ctx: %w", amqp.ErrPermanent), want: true},

		// Transient sentinels — must NOT be permanent.
		{name: "ErrChannelClosed", err: amqp.ErrChannelClosed, want: false},
		{name: "ErrPublishNacked", err: amqp.ErrPublishNacked, want: false},
		{name: "ErrReconnecting", err: amqp.ErrReconnecting, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, amqp.IsPermanent(tc.err))
		})
	}
}
