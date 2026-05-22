package warren_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/brunomvsouza/warren"
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
		{name: "non-amqp sentinel", err: warren.ErrNotConnected, wantCode: 0, wantOK: false},

		// Direct AMQP reply-code sentinels.
		{name: "ErrContentTooLarge direct", err: warren.ErrContentTooLarge, wantCode: 311, wantOK: true},
		{name: "ErrConnectionForced direct", err: warren.ErrConnectionForced, wantCode: 320, wantOK: true},
		{name: "ErrInvalidPath direct", err: warren.ErrInvalidPath, wantCode: 402, wantOK: true},
		{name: "ErrAccessRefused direct", err: warren.ErrAccessRefused, wantCode: 403, wantOK: true},
		{name: "ErrNotFound direct", err: warren.ErrNotFound, wantCode: 404, wantOK: true},
		{name: "ErrResourceLocked direct", err: warren.ErrResourceLocked, wantCode: 405, wantOK: true},
		{name: "ErrPreconditionFailed direct", err: warren.ErrPreconditionFailed, wantCode: 406, wantOK: true},
		{name: "ErrFrameError direct", err: warren.ErrFrameError, wantCode: 501, wantOK: true},
		{name: "ErrSyntaxError direct", err: warren.ErrSyntaxError, wantCode: 502, wantOK: true},
		{name: "ErrCommandInvalid direct", err: warren.ErrCommandInvalid, wantCode: 503, wantOK: true},
		{name: "ErrChannelError direct", err: warren.ErrChannelError, wantCode: 504, wantOK: true},
		{name: "ErrUnexpectedFrame direct", err: warren.ErrUnexpectedFrame, wantCode: 505, wantOK: true},
		{name: "ErrResourceError direct", err: warren.ErrResourceError, wantCode: 506, wantOK: true},
		{name: "ErrNotAllowed direct", err: warren.ErrNotAllowed, wantCode: 530, wantOK: true},
		{name: "ErrNotImplemented direct", err: warren.ErrNotImplemented, wantCode: 540, wantOK: true},
		{name: "ErrInternalError direct", err: warren.ErrInternalError, wantCode: 541, wantOK: true},

		// Wrapped sentinels — code still extractable.
		{name: "ErrNotFound wrapped", err: fmt.Errorf("operation failed: %w", warren.ErrNotFound), wantCode: 404, wantOK: true},
		{name: "ErrAccessRefused wrapped", err: fmt.Errorf("context: %w", warren.ErrAccessRefused), wantCode: 403, wantOK: true},
		{name: "ErrPreconditionFailed wrapped", err: fmt.Errorf("ctx: %w", warren.ErrPreconditionFailed), wantCode: 406, wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotCode, gotOK := warren.AMQPCode(tc.err)
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
		{name: "ErrChannelPoolExhausted", err: warren.ErrChannelPoolExhausted, want: true},
		{name: "ErrPublishNacked", err: warren.ErrPublishNacked, want: true},
		{name: "ErrConnectionBlocked", err: warren.ErrConnectionBlocked, want: true},
		{name: "ErrConfirmTimeout", err: warren.ErrConfirmTimeout, want: true},
		{name: "ErrChannelClosed", err: warren.ErrChannelClosed, want: true},
		{name: "ErrReconnecting", err: warren.ErrReconnecting, want: true},

		// Transient AMQP reply-code sentinels (320, 504, 541).
		{name: "ErrConnectionForced (320)", err: warren.ErrConnectionForced, want: true},
		{name: "ErrChannelError (504)", err: warren.ErrChannelError, want: true},
		{name: "ErrInternalError (541)", err: warren.ErrInternalError, want: true},

		// ErrTransient wrapper.
		{name: "wrapped ErrTransient", err: fmt.Errorf("ctx: %w", warren.ErrTransient), want: true},
		{name: "wrapped transient sentinel", err: fmt.Errorf("ctx: %w", warren.ErrChannelClosed), want: true},

		// Permanent sentinels — must NOT be transient.
		{name: "ErrContentTooLarge (311) is NOT transient", err: warren.ErrContentTooLarge, want: false},
		{name: "ErrResourceError (506) is NOT transient", err: warren.ErrResourceError, want: false},
		{name: "ErrNotFound (404)", err: warren.ErrNotFound, want: false},
		{name: "ErrPreconditionFailed (406)", err: warren.ErrPreconditionFailed, want: false},
		{name: "ErrTopologyRedeclareFailed", err: warren.ErrTopologyRedeclareFailed, want: false},
		{name: "ErrInvalidMessage", err: warren.ErrInvalidMessage, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, warren.IsTransient(tc.err))
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
		{name: "ErrContentTooLarge (311)", err: warren.ErrContentTooLarge, want: true},
		{name: "ErrInvalidPath (402)", err: warren.ErrInvalidPath, want: true},
		{name: "ErrAccessRefused (403)", err: warren.ErrAccessRefused, want: true},
		{name: "ErrNotFound (404)", err: warren.ErrNotFound, want: true},
		{name: "ErrResourceLocked (405)", err: warren.ErrResourceLocked, want: true},
		{name: "ErrPreconditionFailed (406)", err: warren.ErrPreconditionFailed, want: true},
		{name: "ErrFrameError (501)", err: warren.ErrFrameError, want: true},
		{name: "ErrSyntaxError (502)", err: warren.ErrSyntaxError, want: true},
		{name: "ErrCommandInvalid (503)", err: warren.ErrCommandInvalid, want: true},
		{name: "ErrUnexpectedFrame (505)", err: warren.ErrUnexpectedFrame, want: true},
		{name: "ErrResourceError (506) is permanent", err: warren.ErrResourceError, want: true},
		{name: "ErrNotAllowed (530)", err: warren.ErrNotAllowed, want: true},
		{name: "ErrNotImplemented (540)", err: warren.ErrNotImplemented, want: true},

		// ErrTopologyRedeclareFailed (permanent).
		{name: "ErrTopologyRedeclareFailed", err: warren.ErrTopologyRedeclareFailed, want: true},
		{name: "ErrTopologyRedeclareFailed wrapped", err: fmt.Errorf("ctx: %w", warren.ErrTopologyRedeclareFailed), want: true},

		// ErrPermanent wrapper.
		{name: "wrapped ErrPermanent", err: fmt.Errorf("ctx: %w", warren.ErrPermanent), want: true},

		// Transient sentinels — must NOT be permanent.
		{name: "ErrChannelClosed", err: warren.ErrChannelClosed, want: false},
		{name: "ErrPublishNacked", err: warren.ErrPublishNacked, want: false},
		{name: "ErrReconnecting", err: warren.ErrReconnecting, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, warren.IsPermanent(tc.err))
		})
	}
}
