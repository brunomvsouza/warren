// Package confirms manages publisher confirmations for a single AMQP channel.
// It tracks in-flight publishes, correlates basic.return + basic.ack frames for
// mandatory messages, and resolves each Wait call with the appropriate outcome.
//
// Import-cycle note: this package does not import the root amqp package so it
// can itself be imported by files in that package (channelpool, publisher, etc.).
// Callers are responsible for mapping ErrNacked → amqp.ErrPublishNacked,
// ErrClosed → amqp.ErrChannelClosed, ErrTimeout → amqp.ErrConfirmTimeout, and
// *UnroutableError → wrapCode(ue.ReplyCode, amqp.ErrUnroutable).
package confirms

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Sentinel errors returned by Tracker.Wait and Register.
var (
	// ErrTimeout is returned when no confirm arrives within the deadline.
	ErrTimeout = errors.New("confirms: confirm timeout")
	// ErrNacked is returned when the broker sends basic.nack (e.g. overflow=reject-publish).
	ErrNacked = errors.New("confirms: broker nacked publish")
	// ErrClosed is returned when the channel is closed while a publish is in flight,
	// or when Register is called on a tracker that has already been closed.
	ErrClosed = errors.New("confirms: channel closed")
)

// UnroutableError is returned by Wait when a mandatory publish received basic.return
// followed by basic.ack. ReplyCode is the originating basic.return code (312 or 313).
type UnroutableError struct {
	ReplyCode uint16
}

func (e *UnroutableError) Error() string {
	return fmt.Sprintf("confirms: mandatory publish unroutable (reply code %d)", e.ReplyCode)
}

type waiter struct {
	// done is a buffered channel (capacity 1). A value in the buffer means the
	// confirm has been resolved. The channel is never closed — only written to.
	// resolveOne uses a non-blocking send so duplicate resolves are silently
	// ignored. Wait or CloseAll reads/drains and then deletes the entry.
	done       chan error
	returnCode uint16
	returned   bool
}

// Tracker manages in-flight publisher confirmations for one AMQP channel.
// One Tracker must be created per channel lifetime; create a new one when
// the channel is replaced. Calling Register on a closed Tracker returns ErrClosed.
// Zero value is not usable; use New.
type Tracker struct {
	mu      sync.Mutex
	pending map[uint64]*waiter
	closed  bool // set by CloseAll; prevents Register on a dead channel
}

// New creates a ready-to-use Tracker.
func New() *Tracker {
	return &Tracker{pending: make(map[uint64]*waiter)}
}

// Register prepares a pending slot for deliveryTag. Must be called before the
// corresponding publish so that subsequent Ack/Nack/CloseAll can resolve it.
// Returns ErrClosed if CloseAll has already been called on this Tracker —
// this guards against accidental reuse across channel lifetimes.
func (t *Tracker) Register(deliveryTag uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	t.pending[deliveryTag] = &waiter{done: make(chan error, 1)}
	return nil
}

// MarkReturned records that deliveryTag received a basic.return with replyCode.
// The subsequent Ack for this tag will resolve Wait with *UnroutableError.
// MarkReturned is a no-op if deliveryTag is not registered.
func (t *Tracker) MarkReturned(deliveryTag uint64, replyCode uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if w, ok := t.pending[deliveryTag]; ok {
		w.returned = true
		w.returnCode = replyCode
	}
}

// Ack resolves the confirm for deliveryTag (or all tags ≤ deliveryTag if multiple).
// Tags marked via MarkReturned resolve with *UnroutableError; others resolve with nil.
func (t *Tracker) Ack(deliveryTag uint64, multiple bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if multiple {
		t.resolveUpTo(deliveryTag, nil)
	} else {
		t.resolveOne(deliveryTag, nil)
	}
}

// Nack resolves the confirm for deliveryTag (or all tags ≤ deliveryTag if multiple)
// with ErrNacked.
func (t *Tracker) Nack(deliveryTag uint64, multiple bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if multiple {
		t.resolveUpTo(deliveryTag, ErrNacked)
	} else {
		t.resolveOne(deliveryTag, ErrNacked)
	}
}

// CloseAll marks the Tracker as closed, resolves all unresolved pending confirms
// with ErrClosed, and removes their entries. Entries already resolved by Ack/Nack
// are left for Wait to read and clean up — their result is not overwritten.
// After CloseAll, Register returns ErrClosed for any new delivery tag.
func (t *Tracker) CloseAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	// Deleting from a map during range is safe in Go.
	for tag, w := range t.pending {
		select {
		case w.done <- ErrClosed:
			// was unresolved; now resolved with ErrClosed
			delete(t.pending, tag)
		default:
			// already resolved (channel buffer full); leave for Wait to clean up
		}
	}
}

// Cancel removes deliveryTag from the pending map without resolving it. Use when
// the corresponding publish was never sent (e.g. PublishWithContext returned an error
// after Register was called). After Cancel, Wait for the same tag returns ErrClosed.
func (t *Tracker) Cancel(deliveryTag uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pending, deliveryTag)
}

// Wait blocks until deliveryTag is confirmed, ctx is cancelled, or timeout elapses.
// Returns nil for a clean ack, *UnroutableError for return+ack, ErrNacked for nack,
// ErrClosed for channel-close, ErrTimeout if timeout > 0 and it elapses, or
// ctx.Err() if ctx is cancelled first.
// If timeout ≤ 0, no confirm deadline is applied beyond ctx.
// The pending slot is removed when Wait returns.
func (t *Tracker) Wait(ctx context.Context, deliveryTag uint64, timeout time.Duration) error {
	t.mu.Lock()
	w, ok := t.pending[deliveryTag]
	t.mu.Unlock()
	if !ok {
		return ErrClosed
	}

	// A nil channel in a select case is never selected, so timerC == nil when
	// timeout ≤ 0 effectively disables the confirm-deadline case.
	var timerC <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timerC = timer.C
	}

	select {
	case err := <-w.done:
		t.mu.Lock()
		delete(t.pending, deliveryTag)
		t.mu.Unlock()
		return err
	case <-timerC:
		t.mu.Lock()
		delete(t.pending, deliveryTag)
		t.mu.Unlock()
		return ErrTimeout
	case <-ctx.Done():
		t.mu.Lock()
		delete(t.pending, deliveryTag)
		t.mu.Unlock()
		return ctx.Err()
	}
}

// resolveOne resolves a single deliveryTag. If the tag was MarkReturned and
// baseErr is nil, *UnroutableError is used instead. If baseErr is non-nil
// (e.g. ErrNacked), it takes precedence over *UnroutableError even when the
// tag was MarkReturned — this matches RabbitMQ wire behaviour where
// return+nack is a broker-internal error, not a routing failure.
// Uses a non-blocking send so that duplicate resolve is silently ignored.
// Must be called with t.mu held. Does NOT delete the entry — Wait does that.
func (t *Tracker) resolveOne(deliveryTag uint64, baseErr error) {
	w, ok := t.pending[deliveryTag]
	if !ok {
		return
	}
	err := baseErr
	if w.returned && baseErr == nil {
		err = &UnroutableError{ReplyCode: w.returnCode}
	}
	select {
	case w.done <- err:
	default: // already resolved; ignore duplicate
	}
}

// resolveUpTo resolves all pending tags ≤ deliveryTag in ascending order.
// Must be called with t.mu held.
func (t *Tracker) resolveUpTo(deliveryTag uint64, baseErr error) {
	tags := make([]uint64, 0, len(t.pending))
	for tag := range t.pending {
		if tag <= deliveryTag {
			tags = append(tags, tag)
		}
	}
	slices.Sort(tags)
	for _, tag := range tags {
		t.resolveOne(tag, baseErr)
	}
}
