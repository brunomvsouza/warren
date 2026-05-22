package amqp

import (
	"errors"
	"fmt"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

var (
	// Connection lifecycle.

	// ErrNotConnected is returned when Publish or Consume is called before Dial.
	ErrNotConnected = errors.New("amqp: not connected")
	// ErrAlreadyClosed is returned when Close is called on an already-closed connection.
	ErrAlreadyClosed = errors.New("amqp: already closed")
	// ErrShutdown is returned when an operation is attempted while the connection is shutting down.
	ErrShutdown = errors.New("amqp: client is shutting down")
	// ErrChannelClosed is returned when the broker closes the channel (e.g. after a protocol error).
	ErrChannelClosed = errors.New("amqp: channel closed")
	// ErrConnectionBlocked is returned when the broker blocks the connection due to a memory or disk alarm.
	ErrConnectionBlocked = errors.New("amqp: connection blocked by broker")
	// ErrChannelPoolExhausted is returned when all pooled channels are in-flight and the context is cancelled. Transient.
	ErrChannelPoolExhausted = errors.New("amqp: channel pool exhausted")

	// Publisher errors.

	// ErrConfirmTimeout is returned when the broker does not confirm a publish within the deadline.
	ErrConfirmTimeout = errors.New("amqp: publisher confirm timeout")
	// ErrUnroutable is returned when a mandatory publish has no matching binding (basic.return received).
	ErrUnroutable = errors.New("amqp: mandatory publish was returned")
	// ErrPublishNacked is returned when the broker sends basic.nack (e.g. overflow=reject-publish, disk alarm).
	ErrPublishNacked = errors.New("amqp: broker nacked publish")
	// ErrPartialBatch is returned when one or more messages in a PublishBatch fail.
	ErrPartialBatch = errors.New("amqp: batch publish partially failed")
	// ErrBatchTooLarge is returned when PublishBatch is called with more messages than PublishBatchMaxSize allows.
	ErrBatchTooLarge = errors.New("amqp: PublishBatch exceeds max in-flight budget")

	// Consumer errors.

	// ErrRequeue signals the consumer handler that the message should be nacked with requeue=true.
	ErrRequeue = errors.New("amqp: nack with requeue")
	// ErrPoison signals the consumer handler that the message should be nacked without requeue.
	ErrPoison = errors.New("amqp: poison message (nack no requeue)")
	// ErrMaxRedeliveries is returned when a message exceeds the MaxRedeliveries limit.
	ErrMaxRedeliveries = errors.New("amqp: max redeliveries exceeded")
	// ErrConsumerCancelled is returned when the broker cancels the consumer via basic.cancel
	// (e.g. the queue was deleted or the exclusive lock was revoked).
	ErrConsumerCancelled = errors.New("amqp: consumer cancelled by broker (basic.cancel)")

	// Codec / payload errors.

	// ErrInvalidMessage is returned when the message payload cannot be encoded or decoded,
	// or when a Message field value violates a SPEC constraint.
	ErrInvalidMessage = errors.New("amqp: invalid message payload")

	// Topology errors.

	// ErrTopologyMismatch is returned when Topology.Declare finds an existing queue or exchange
	// whose properties conflict with the requested declaration. It wraps ErrPreconditionFailed.
	ErrTopologyMismatch = errors.New("amqp: topology mismatch")
	// ErrTopologyRedeclareFailed is returned when the reconnect barrier cannot redeclare the topology.
	// The connection enters a degraded state; this error is permanent until a successful redeclare.
	ErrTopologyRedeclareFailed = errors.New("amqp: topology redeclare failed")

	// Reconnect lifecycle.

	// ErrReconnecting is returned while the connection is inside the synchronous reconnect barrier
	// (redeclare → re-subscribe → WithOnReconnect). Publish blocks until the barrier clears. Transient.
	ErrReconnecting = errors.New("amqp: connection reconnecting")

	// RPC errors.

	// ErrCallTimeout is returned when an RPC call exceeds its deadline.
	ErrCallTimeout = errors.New("amqp: rpc call timed out")

	// Configuration errors.

	// ErrInvalidOptions is returned when a builder option value violates a SPEC constraint.
	ErrInvalidOptions = errors.New("amqp: invalid options")

	// AMQP 0-9-1 reply-code sentinels.
	//
	// Broker-originated errors from publish, consume, or declare operations are translated by
	// internal/amqperror into wraps of these sentinels so callers can use errors.Is for precise
	// branching or IsTransient/IsPermanent for coarse classification.

	// ErrContentTooLarge wraps AMQP reply code 311 (content-too-large).
	ErrContentTooLarge = errors.New("amqp: content too large (311)")
	// ErrConnectionForced wraps AMQP reply code 320 (connection-forced).
	ErrConnectionForced = errors.New("amqp: connection forced (320)")
	// ErrInvalidPath wraps AMQP reply code 402 (invalid-path). Permanent.
	ErrInvalidPath = errors.New("amqp: invalid path (402)")
	// ErrAccessRefused wraps AMQP reply code 403 (access-refused). Permanent.
	ErrAccessRefused = errors.New("amqp: access refused (403)")
	// ErrNotFound wraps AMQP reply code 404 (not-found). Permanent.
	ErrNotFound = errors.New("amqp: not found (404)")
	// ErrResourceLocked wraps AMQP reply code 405 (resource-locked). Permanent.
	ErrResourceLocked = errors.New("amqp: resource locked (405)")
	// ErrPreconditionFailed wraps AMQP reply code 406 (precondition-failed). Permanent.
	ErrPreconditionFailed = errors.New("amqp: precondition failed (406)")
	// ErrFrameError wraps AMQP reply code 501 (frame-error). Permanent.
	ErrFrameError = errors.New("amqp: frame error (501)")
	// ErrSyntaxError wraps AMQP reply code 502 (syntax-error). Permanent.
	ErrSyntaxError = errors.New("amqp: syntax error (502)")
	// ErrCommandInvalid wraps AMQP reply code 503 (command-invalid). Permanent.
	ErrCommandInvalid = errors.New("amqp: command invalid (503)")
	// ErrChannelError wraps AMQP reply code 504 (channel-error). Transient.
	ErrChannelError = errors.New("amqp: channel error (504)")
	// ErrUnexpectedFrame wraps AMQP reply code 505 (unexpected-frame). Permanent.
	ErrUnexpectedFrame = errors.New("amqp: unexpected frame (505)")
	// ErrResourceError wraps AMQP reply code 506 (resource-error). Permanent by default.
	// Resource errors cover both transient (disk pressure) and permanent (FD exhaustion)
	// conditions; retrying blindly amplifies pressure. Callers that know their workload
	// can re-classify by wrapping with ErrTransient explicitly.
	ErrResourceError = errors.New("amqp: resource error (506)")
	// ErrNotAllowed wraps AMQP reply code 530 (not-allowed). Permanent.
	ErrNotAllowed = errors.New("amqp: not allowed (530)")
	// ErrNotImplemented wraps AMQP reply code 540 (not-implemented). Permanent.
	ErrNotImplemented = errors.New("amqp: not implemented (540)")
	// ErrInternalError wraps AMQP reply code 541 (internal-error). Transient.
	ErrInternalError = errors.New("amqp: internal error (541)")

	// Retry classifiers.

	// ErrTransient is a sentinel that callers can wrap around any error to mark it as retryable.
	// IsTransient returns true for any error in the chain that wraps ErrTransient.
	ErrTransient = errors.New("amqp: transient error")
	// ErrPermanent is a sentinel that callers can wrap around any error to mark it as non-retryable.
	// IsPermanent returns true for any error in the chain that wraps ErrPermanent.
	ErrPermanent = errors.New("amqp: permanent error")
)

// amqpCodeSentinels maps each AMQP reply-code sentinel to its wire code.
var amqpCodeSentinels = []struct {
	err  error
	code uint16
}{
	{ErrContentTooLarge, 311},
	{ErrConnectionForced, 320},
	{ErrInvalidPath, 402},
	{ErrAccessRefused, 403},
	{ErrNotFound, 404},
	{ErrResourceLocked, 405},
	{ErrPreconditionFailed, 406},
	{ErrFrameError, 501},
	{ErrSyntaxError, 502},
	{ErrCommandInvalid, 503},
	{ErrChannelError, 504},
	{ErrUnexpectedFrame, 505},
	{ErrResourceError, 506},
	{ErrNotAllowed, 530},
	{ErrNotImplemented, 540},
	{ErrInternalError, 541},
}

// AMQPCode returns the AMQP reply code embedded in err (if any) and true on
// success. Returns (0, false) otherwise.
//
// Recognised codes:
//   - Channel/connection close codes: 311, 320, 402-406, 501-506, 530, 540, 541.
//   - basic.return codes: 312 (NO_ROUTE), 313 (NO_CONSUMERS). These are NOT
//     channel-close codes; the library surfaces them by wrapping ErrUnroutable
//     with an internal codeError carrying the originating code.
func AMQPCode(err error) (uint16, bool) {
	if err == nil {
		return 0, false
	}
	// Check for an explicit code tag (used for basic.return codes 312/313 where
	// ErrUnroutable is wrapped with the originating reply code by the confirm tracker).
	var ce *codeError
	if errors.As(err, &ce) {
		return ce.code, true
	}
	for _, s := range amqpCodeSentinels {
		if errors.Is(err, s.err) {
			return s.code, true
		}
	}
	return 0, false
}

// IsTransient reports whether err is classified as retryable.
// True for: ErrTransient wraps; ErrChannelPoolExhausted; ErrPublishNacked;
// ErrConnectionBlocked; ErrConfirmTimeout; ErrChannelClosed; ErrReconnecting;
// AMQP codes 311, 320, 504, 541.
//
// Note on ErrResourceError (506): NOT transient by default — see its godoc.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrTransient) {
		return true
	}
	for _, sentinel := range []error{
		ErrChannelPoolExhausted,
		ErrPublishNacked,
		ErrConnectionBlocked,
		ErrConfirmTimeout,
		ErrChannelClosed,
		ErrReconnecting,
		ErrContentTooLarge,  // 311
		ErrConnectionForced, // 320
		ErrChannelError,     // 504
		ErrInternalError,    // 541
	} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// IsPermanent reports whether err is classified as non-retryable.
// True for: ErrPermanent wraps; ErrTopologyRedeclareFailed; AMQP codes
// 402, 403, 404, 405, 406, 501, 502, 503, 505, 506, 530, 540.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) {
		return true
	}
	if errors.Is(err, ErrTopologyRedeclareFailed) {
		return true
	}
	for _, sentinel := range []error{
		ErrInvalidPath,        // 402
		ErrAccessRefused,      // 403
		ErrNotFound,           // 404
		ErrResourceLocked,     // 405
		ErrPreconditionFailed, // 406
		ErrFrameError,         // 501
		ErrSyntaxError,        // 502
		ErrCommandInvalid,     // 503
		ErrUnexpectedFrame,    // 505
		ErrResourceError,      // 506 — permanent by decision (SPEC §6.8)
		ErrNotAllowed,         // 530
		ErrNotImplemented,     // 540
	} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// codeError carries an AMQP reply code alongside its underlying sentinel.
// internal/amqperror and the confirm tracker use it to tag ErrUnroutable with
// the originating basic.return code (312 NO_ROUTE or 313 NO_CONSUMERS).
type codeError struct {
	code uint16
	err  error
}

func (e *codeError) Error() string { return e.err.Error() }
func (e *codeError) Unwrap() error { return e.err }

// wrapCode wraps err with an explicit AMQP reply code. Called by
// internal/amqperror and the confirm tracker; not part of the public API.
func wrapCode(code uint16, err error) error {
	return &codeError{code: code, err: err}
}

// amqpCodeTable maps wire codes to their sentinel errors. It is the authoritative
// source used by wrapAMQPError (root package) to avoid the import cycle that
// would arise from importing internal/amqperror.
var amqpCodeTable = func() map[uint16]error {
	m := make(map[uint16]error, len(amqpCodeSentinels))
	for _, s := range amqpCodeSentinels {
		m[s.code] = s.err
	}
	return m
}()

// wrapAMQPError translates an *amqp091.Error (if present in err's chain) into a
// wrapped sentinel chain so callers can use errors.Is / AMQPCode / IsTransient.
// Unlike internal/amqperror.Wrap, this function lives in the root package and
// carries no import cycle. Non-AMQP errors are returned unchanged.
func wrapAMQPError(err error) error {
	if err == nil {
		return nil
	}
	var amqpErr *amqp091.Error
	if !errors.As(err, &amqpErr) {
		return err
	}
	sentinel, ok := amqpCodeTable[uint16(amqpErr.Code)] //nolint:gosec // G115: amqp codes are protocol-defined
	if !ok {
		return err
	}
	return fmt.Errorf("%w: %w", sentinel, err)
}
