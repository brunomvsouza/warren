package warren

import (
	"context"
	"errors"
	"fmt"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

var (
	// Connection lifecycle.

	// ErrNotConnected is returned when Publish or Consume is called before Dial.
	ErrNotConnected = errors.New("warren: not connected")
	// ErrAlreadyClosed is returned when an operation is attempted on a resource that
	// has already been closed — either a Connection closed twice, or Ack/Nack/AckIf
	// called on a Delivery whose owning Consumer was shut down via Close(ctx).
	ErrAlreadyClosed = errors.New("warren: already closed")
	// ErrAlreadyResolved is returned by the second (and any later) Ack/Nack/AckIf
	// on a Delivery[M] that has already emitted its verdict frame — including a
	// late handler verdict after a HandlerTimeout fired. The resolved-once guard
	// is a single atomic CAS: only the winner emits a frame; losers are no-ops
	// returning this sentinel. It prevents a second basic.ack/nack frame that
	// would channel-close with PRECONDITION_FAILED (406) and take out every
	// in-flight handler on that channel. See SPEC §6.3.
	ErrAlreadyResolved = errors.New("warren: delivery already resolved")
	// ErrShutdown is returned when an operation is attempted while the connection is shutting down.
	ErrShutdown = errors.New("warren: client is shutting down")
	// ErrChannelClosed is returned when the broker closes the channel (e.g. after a protocol error).
	ErrChannelClosed = errors.New("warren: channel closed")
	// ErrConnectionBlocked is returned when the broker blocks the connection due to a memory or disk alarm.
	ErrConnectionBlocked = errors.New("warren: connection blocked by broker")
	// ErrChannelPoolExhausted is returned when ctx is cancelled before a semaphore
	// token becomes available. It wraps ctx.Err() so callers can distinguish a
	// voluntary cancellation (context.Canceled) from a deadline (context.DeadlineExceeded)
	// via errors.Is. Note: IsTransient treats this error as transient EXCEPT when the
	// wrapped ctx.Err() is context.Canceled — an upstream cancellation will never
	// succeed on retry, so IsTransient returns false for it (T54). A deadline
	// (context.DeadlineExceeded) remains transient.
	ErrChannelPoolExhausted = errors.New("warren: channel pool exhausted")

	// Publisher errors.

	// ErrConfirmTimeout is returned when the broker does not confirm a publish within the deadline.
	ErrConfirmTimeout = errors.New("warren: publisher confirm timeout")
	// ErrUnroutable is returned when a mandatory publish has no matching binding (basic.return received).
	ErrUnroutable = errors.New("warren: mandatory publish was returned")
	// ErrPublishNacked is returned when the broker sends basic.nack (e.g. overflow=reject-publish or
	// reject-publish-dlx). A disk/memory alarm does NOT nack — it raises connection.blocked, surfaced as
	// ErrConnectionBlocked, not ErrPublishNacked.
	ErrPublishNacked = errors.New("warren: broker nacked publish")
	// ErrPartialBatch is returned when one or more messages in a PublishBatch fail.
	ErrPartialBatch = errors.New("warren: batch publish partially failed")
	// ErrBatchTooLarge is returned when PublishBatch is called with more messages than PublishBatchMaxSize allows.
	ErrBatchTooLarge = errors.New("warren: PublishBatch exceeds max in-flight budget")
	// ErrMessageTooLarge is returned when an encoded message body exceeds the
	// publisher's MaxMessageSizeBytes guardrail. The publish is rejected locally
	// before any channel is opened — protecting the publisher from OOM and the
	// broker from frame fragmentation pressure. Classified as permanent: the same
	// body will never succeed on retry. The broker-side equivalent (reply code
	// 311, ErrContentTooLarge) only fires after the payload has been allocated
	// and partially sent; this local guard avoids that round-trip.
	ErrMessageTooLarge = errors.New("warren: message body exceeds MaxMessageSizeBytes")
	// ErrRateLimited is returned when a publish cannot acquire a local rate-limit
	// token before its context is cancelled (WithPublishRateLimit). It wraps
	// ctx.Err() so callers can still distinguish a deadline from a cancellation via
	// errors.Is. Classified transient: the same publish may succeed once the local
	// token bucket refills. Throttled-but-completed publishes do NOT return this
	// error — they only increment publisher_rate_limited_total.
	ErrRateLimited = errors.New("warren: publish rate limited")

	// Consumer errors.

	// ErrRequeue signals the consumer handler that the message should be nacked with requeue=true.
	ErrRequeue = errors.New("warren: nack with requeue")
	// ErrPoison signals the consumer handler that the message should be nacked without requeue.
	ErrPoison = errors.New("warren: poison message (nack no requeue)")
	// ErrMaxRedeliveries is returned when a message exceeds the MaxRedeliveries limit.
	ErrMaxRedeliveries = errors.New("warren: max redeliveries exceeded")
	// ErrConsumerCancelled is returned when the broker cancels the consumer via basic.cancel
	// (e.g. the queue was deleted or the exclusive lock was revoked).
	ErrConsumerCancelled = errors.New("warren: consumer cancelled by broker (basic.cancel)")

	// Codec / payload errors.

	// ErrInvalidMessage is returned when the message payload cannot be encoded or decoded,
	// or when a Message field value violates a SPEC constraint.
	ErrInvalidMessage = errors.New("warren: invalid message payload")

	// Topology errors.

	// ErrTopologyMismatch is returned when Topology.Declare finds an existing queue or exchange
	// whose properties conflict with the requested declaration. It wraps ErrPreconditionFailed.
	ErrTopologyMismatch = errors.New("warren: topology mismatch")
	// ErrTopologyRedeclareFailed is returned when the reconnect barrier cannot redeclare the topology.
	// The connection enters a degraded state; this error is permanent until a successful redeclare.
	ErrTopologyRedeclareFailed = errors.New("warren: topology redeclare failed")

	// Reconnect lifecycle.

	// ErrReconnecting is returned while the connection is inside the synchronous reconnect barrier
	// (redeclare → re-subscribe → WithOnReconnect). Publish blocks until the barrier clears. Transient.
	ErrReconnecting = errors.New("warren: connection reconnecting")

	// RPC errors.

	// ErrCallTimeout is returned when an RPC call exceeds its deadline.
	ErrCallTimeout = errors.New("warren: rpc call timed out")

	// Configuration errors.

	// ErrInvalidOptions is returned when a builder option value violates a SPEC constraint.
	ErrInvalidOptions = errors.New("warren: invalid options")

	// AMQP 0-9-1 reply-code sentinels.
	//
	// Broker-originated errors from publish, consume, or declare operations are translated by
	// internal/amqperror into wraps of these sentinels so callers can use errors.Is for precise
	// branching or IsTransient/IsPermanent for coarse classification.
	//
	// Each sentinel is annotated with its AMQP 0-9-1 scope:
	//
	//   - channel-level (soft error): the broker closes only the offending channel; the TCP
	//     connection survives. Recovery is local — the next operation acquires/reopens a fresh
	//     channel from the pool (channel self-heal, T61); topology stays declared and no full
	//     reconnect runs. Codes: 311, 403, 404, 405, 406.
	//   - connection-level (hard error): the broker closes the whole TCP connection. Recovery
	//     runs the reconnect supervisor barrier (re-dial → re-open channel → redeclare topology
	//     → re-issue basic.consume; see §6.1), and Publish blocks on ErrReconnecting until it
	//     clears. Codes: 320, 402, 501, 502, 503, 504, 505, 506, 530, 540, 541.
	//
	// Scope is orthogonal to the transient/permanent classification: e.g. 504 is connection-level
	// yet transient, while 406 is channel-level yet permanent.

	// ErrContentTooLarge wraps AMQP reply code 311 (content-too-large). Channel-level.
	ErrContentTooLarge = errors.New("warren: content too large (311)")
	// ErrConnectionForced wraps AMQP reply code 320 (connection-forced). Connection-level.
	ErrConnectionForced = errors.New("warren: connection forced (320)")
	// ErrInvalidPath wraps AMQP reply code 402 (invalid-path). Connection-level. Permanent.
	ErrInvalidPath = errors.New("warren: invalid path (402)")
	// ErrAccessRefused wraps AMQP reply code 403 (access-refused). Channel-level. Permanent.
	ErrAccessRefused = errors.New("warren: access refused (403)")
	// ErrNotFound wraps AMQP reply code 404 (not-found). Channel-level. Permanent.
	ErrNotFound = errors.New("warren: not found (404)")
	// ErrResourceLocked wraps AMQP reply code 405 (resource-locked). Channel-level. Permanent.
	ErrResourceLocked = errors.New("warren: resource locked (405)")
	// ErrPreconditionFailed wraps AMQP reply code 406 (precondition-failed). Channel-level. Permanent.
	ErrPreconditionFailed = errors.New("warren: precondition failed (406)")
	// ErrFrameError wraps AMQP reply code 501 (frame-error). Connection-level. Permanent.
	ErrFrameError = errors.New("warren: frame error (501)")
	// ErrSyntaxError wraps AMQP reply code 502 (syntax-error). Connection-level. Permanent.
	ErrSyntaxError = errors.New("warren: syntax error (502)")
	// ErrCommandInvalid wraps AMQP reply code 503 (command-invalid). Connection-level. Permanent.
	ErrCommandInvalid = errors.New("warren: command invalid (503)")
	// ErrChannelError wraps AMQP reply code 504 (channel-error). Connection-level. Transient.
	ErrChannelError = errors.New("warren: channel error (504)")
	// ErrUnexpectedFrame wraps AMQP reply code 505 (unexpected-frame). Connection-level. Permanent.
	ErrUnexpectedFrame = errors.New("warren: unexpected frame (505)")
	// ErrResourceError wraps AMQP reply code 506 (resource-error). Connection-level. Permanent by default.
	// Resource errors cover both transient (disk pressure) and permanent (FD exhaustion)
	// conditions; retrying blindly amplifies pressure. Callers that know their workload
	// can re-classify by wrapping with ErrTransient explicitly.
	ErrResourceError = errors.New("warren: resource error (506)")
	// ErrNotAllowed wraps AMQP reply code 530 (not-allowed). Connection-level. Permanent.
	ErrNotAllowed = errors.New("warren: not allowed (530)")
	// ErrNotImplemented wraps AMQP reply code 540 (not-implemented). Connection-level. Permanent.
	ErrNotImplemented = errors.New("warren: not implemented (540)")
	// ErrInternalError wraps AMQP reply code 541 (internal-error). Connection-level. Transient.
	ErrInternalError = errors.New("warren: internal error (541)")

	// Retry classifiers.

	// ErrTransient is a sentinel that callers can wrap around any error to mark it as retryable.
	// IsTransient returns true for any error in the chain that wraps ErrTransient.
	ErrTransient = errors.New("warren: transient error")
	// ErrPermanent is a sentinel that callers can wrap around any error to mark it as non-retryable.
	// IsPermanent returns true for any error in the chain that wraps ErrPermanent.
	ErrPermanent = errors.New("warren: permanent error")
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
// ErrRateLimited; AMQP codes 320, 504, 541.
//
// Note on ErrContentTooLarge (311): NOT transient. A payload that exceeds
// frame-max will fail on every retry unchanged — retrying it burns connections
// without any chance of success.
//
// Note on ErrResourceError (506): NOT transient by default — see its godoc.
//
// Note on context.Canceled: NEVER transient, even when the error also wraps a
// transient sentinel (e.g. ErrChannelPoolExhausted observed mid-cancellation).
// An upstream request cancellation will fail identically on every retry, so a
// PublishRetry would burn connections without any chance of success (T54).
// context.DeadlineExceeded is deliberately NOT special-cased — a timeout may
// succeed on a subsequent attempt.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	// Context cancellation overrides any transient classification below.
	if errors.Is(err, context.Canceled) {
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
		ErrRateLimited,
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
// True for: ErrPermanent wraps; ErrTopologyRedeclareFailed; ErrMessageTooLarge;
// AMQP codes 311, 402, 403, 404, 405, 406, 501, 502, 503, 505, 506, 530, 540.
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
	if errors.Is(err, ErrMessageTooLarge) {
		return true
	}
	for _, sentinel := range []error{
		ErrContentTooLarge,    // 311 — payload never changes; retry is futile
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
