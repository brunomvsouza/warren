// Package amqperror translates *amqp091.Error values (delivered by the broker
// on channel/connection close) into wrapped chains of the reply-code sentinels
// declared in the root amqp package. Every component in this library that talks
// to the broker and may receive an *amqp091.Error funnels errors through Wrap so
// that callers can rely on errors.Is(err, amqp.ErrNotFound) et al. and on the
// AMQPCode/IsTransient/IsPermanent classifiers.
//
// Intentional omissions from the code table:
//   - 312 (NO_ROUTE) and 313 (NO_CONSUMERS) are basic.return reply codes, not
//     channel-close codes. They are never delivered as *amqp091.Error and are
//     handled by internal/confirms, which wraps ErrUnroutable with the
//     originating reply code via wrapCode.
package amqperror

import (
	"errors"
	"fmt"

	amqp091 "github.com/rabbitmq/amqp091-go"

	amqp "github.com/brunomvsouza/warren"
)

// codeTable maps AMQP 0-9-1 reply codes to their sentinel errors in the root package.
var codeTable = map[uint16]error{
	311: amqp.ErrContentTooLarge,
	320: amqp.ErrConnectionForced,
	402: amqp.ErrInvalidPath,
	403: amqp.ErrAccessRefused,
	404: amqp.ErrNotFound,
	405: amqp.ErrResourceLocked,
	406: amqp.ErrPreconditionFailed,
	501: amqp.ErrFrameError,
	502: amqp.ErrSyntaxError,
	503: amqp.ErrCommandInvalid,
	504: amqp.ErrChannelError,
	505: amqp.ErrUnexpectedFrame,
	506: amqp.ErrResourceError,
	530: amqp.ErrNotAllowed,
	540: amqp.ErrNotImplemented,
	541: amqp.ErrInternalError,
}

// Wrap converts err into a chain that wraps the appropriate reply-code sentinel
// when err (or any error in its chain via errors.As) is an *amqp091.Error.
//
// The returned error satisfies:
//   - errors.Is(result, amqp.ErrNotFound) — true when the code is 404, etc.
//   - errors.As(result, &(*amqp091.Error){}) — true; the original is preserved (chain depth 2).
//   - amqp.AMQPCode(result) — returns the wire code and true.
//   - amqp.IsTransient / amqp.IsPermanent — correct per the SPEC §6.8 classification table.
//
// If err is nil, nil is returned. If err is not an *amqp091.Error (and does not
// wrap one), it is returned unchanged.
// If the reply code has no mapping (e.g. a future code not yet in the table),
// err is returned unchanged so the caller still has the original information.
func Wrap(err error) error {
	if err == nil {
		return nil
	}
	var amqpErr *amqp091.Error
	if !errors.As(err, &amqpErr) {
		return err
	}
	sentinel, ok := codeTable[uint16(amqpErr.Code)] //nolint:gosec // amqp codes are protocol-defined, range is safe
	if !ok {
		return err
	}
	return fmt.Errorf("%w: %w", sentinel, err)
}
