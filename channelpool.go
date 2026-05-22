package warren

import (
	"context"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// amqpChannel is the minimal interface the channel pool needs from an AMQP
// channel. *amqp091.Channel satisfies this interface; tests inject fakes.
type amqpChannel interface {
	NotifyClose(chan *amqp091.Error) chan *amqp091.Error
	Close() error
}

// Compile-time proof that *amqp091.Channel satisfies amqpChannel.
var _ amqpChannel = (*amqp091.Channel)(nil)

// pooledEntry wraps an amqpChannel with its close-notification channel so the
// pool can detect broker-side channel closure without holding a lock.
type pooledEntry struct {
	ch      amqpChannel
	closeCh chan *amqp091.Error
}

// channelPool is a per-publisher-TCP-connection pool of AMQP channels.
//
// Capacity is hard-limited to size: Acquire blocks until a slot is available
// or the context is cancelled, returning ErrChannelPoolExhausted. Release
// returns the channel to the idle list or discards it if the broker closed it.
//
// The pool is concurrency-safe. All shared state is managed through channels,
// which provides lock-free synchronisation.
type channelPool struct {
	tokens chan struct{}    // counting semaphore, cap = size
	free   chan pooledEntry // idle channels, cap = size
	openFn func() (amqpChannel, error)
}

// newChannelPool creates a pool with the given capacity. openFn is called
// whenever a new channel must be opened (idle list empty or stale).
func newChannelPool(size int, openFn func() (amqpChannel, error)) *channelPool {
	p := &channelPool{
		tokens: make(chan struct{}, size),
		free:   make(chan pooledEntry, size),
		openFn: openFn,
	}
	for range size {
		p.tokens <- struct{}{}
	}
	return p
}

// Acquire returns a pooled channel and a release func that must be called
// exactly once when the caller is done with the channel. If all pool slots
// are busy and ctx is cancelled, ErrChannelPoolExhausted is returned.
func (p *channelPool) Acquire(ctx context.Context) (amqpChannel, func(), error) {
	select {
	case <-ctx.Done():
		return nil, nil, ErrChannelPoolExhausted
	case <-p.tokens:
	}

	entry, err := p.getOrOpen()
	if err != nil {
		p.tokens <- struct{}{} // return the token on open failure
		return nil, nil, err
	}

	return entry.ch, func() { p.release(entry) }, nil
}

// getOrOpen drains stale entries from the free list and returns a live
// channel, opening a new one when the free list is empty.
func (p *channelPool) getOrOpen() (pooledEntry, error) {
	for {
		select {
		case entry := <-p.free:
			// Non-blocking: check whether the broker already closed this channel.
			select {
			case <-entry.closeCh:
				// Stale — discard and try the next idle entry or open fresh.
				_ = entry.ch.Close()
				continue
			default:
				return entry, nil
			}
		default:
			// Free list empty: open a new channel.
			ch, err := p.openFn()
			if err != nil {
				return pooledEntry{}, err
			}
			closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))
			return pooledEntry{ch: ch, closeCh: closeCh}, nil
		}
	}
}

// Drain discards all idle channels in the free list without waiting for
// in-flight acquires to complete. It is called by the reconnect supervisor
// (T07d) after a TCP reconnect so that stale channels from the dead socket
// are never reused: the supervisor opens fresh channels on the new connection
// and the pool repopulates lazily on subsequent Acquire calls.
func (p *channelPool) Drain() {
	for {
		select {
		case entry := <-p.free:
			_ = entry.ch.Close()
		default:
			return
		}
	}
}

// release returns entry to the idle list, or discards it if the broker closed
// the channel during use. The semaphore token is always returned.
func (p *channelPool) release(entry pooledEntry) {
	defer func() { p.tokens <- struct{}{} }()

	select {
	case <-entry.closeCh:
		// Channel was closed by the broker while in use — discard.
		_ = entry.ch.Close()
		return
	default:
	}

	// Return to the idle list. The select-default branch is a safety valve;
	// the free channel has the same capacity as the token semaphore, so
	// overflow should not occur under correct usage.
	select {
	case p.free <- entry:
	default:
		_ = entry.ch.Close()
	}
}
