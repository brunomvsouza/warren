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
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Sentinel errors returned by Tracker.Wait.
var (
	// ErrTimeout is returned when no confirm arrives within the deadline.
	ErrTimeout = errors.New("confirms: confirm timeout")
	// ErrNacked is returned when the broker sends basic.nack (e.g. overflow=reject-publish).
	ErrNacked = errors.New("confirms: broker nacked publish")
	// ErrClosed is returned when the channel is closed while a publish is in flight.
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
	// done is a buffered channel (capacity 1). A value in the buffer means
	// the confirm has been resolved. The channel is never closed — only written.
	// resolveOne uses a non-blocking send so that a duplicate resolve is silently
	// ignored. Wait or CloseAll reads/drains and then deletes the entry.
	done       chan error
	returnCode uint16
	returned   bool
}

// Tracker manages in-flight publisher confirmations for one AMQP channel.
// A single Tracker must be created per channel; call CloseAll when the channel closes.
// Zero value is not usable; use New.
type Tracker struct {
	mu      sync.Mutex
	pending map[uint64]*waiter
}

// New creates a ready-to-use Tracker.
func New() *Tracker {
	return &Tracker{
		pending: make(map[uint64]*waiter),
	}
}

// Register prepares a pending slot for deliveryTag. Must be called before
// the corresponding publish so that subsequent Ack/Nack/CloseAll can resolve it.
func (t *Tracker) Register(deliveryTag uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[deliveryTag] = &waiter{done: make(chan error, 1)}
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

// CloseAll resolves all unresolved pending confirms with ErrClosed and removes
// their entries. Entries already resolved by Ack/Nack are left for Wait to read
// and clean up — their result is not overwritten.
func (t *Tracker) CloseAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
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

// Wait blocks until deliveryTag is confirmed or timeout elapses. Returns nil
// for a clean ack, *UnroutableError for return+ack, ErrNacked for nack,
// ErrClosed for channel-close, or ErrTimeout if the deadline is exceeded.
// The pending slot is removed when Wait returns.
func (t *Tracker) Wait(deliveryTag uint64, timeout time.Duration) error {
	t.mu.Lock()
	w, ok := t.pending[deliveryTag]
	t.mu.Unlock()
	if !ok {
		return ErrClosed
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-w.done:
		t.mu.Lock()
		delete(t.pending, deliveryTag)
		t.mu.Unlock()
		return err
	case <-timer.C:
		t.mu.Lock()
		delete(t.pending, deliveryTag)
		t.mu.Unlock()
		return ErrTimeout
	}
}

// resolveOne resolves a single deliveryTag with baseErr (overridden by *UnroutableError
// when the tag was MarkReturned and baseErr is nil). Uses a non-blocking send so
// that a duplicate resolve for the same tag is silently ignored.
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
	default: // already resolved; ignore
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
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	for _, tag := range tags {
		t.resolveOne(tag, baseErr)
	}
}
