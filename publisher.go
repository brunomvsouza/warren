package warren

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/internal/confirms"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// pubChannel is the AMQP channel interface required by Publisher.
// *amqp091.Channel satisfies this interface; tests may inject fakes.
type pubChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp091.Publishing) error
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp091.Confirmation) chan amqp091.Confirmation
	NotifyReturn(c chan amqp091.Return) chan amqp091.Return
	NotifyClose(c chan *amqp091.Error) chan *amqp091.Error
	GetNextPublishSeqNo() uint64
	Close() error
}

// publisherEntry bundles a publisher channel with its per-lifetime confirm tracker.
// One entry is created per channel open; the tracker survives pool recycles until
// the channel closes.
//
// NOTE: publisherEntry is copied by value when the pool returns it from acquire.
// Any field that must be shared between Publish (which holds the copy) and the
// background goroutine (which holds the original) MUST be a pointer type so
// that both sides refer to the same memory. activeTag is such a field.
type publisherEntry struct {
	ch      pubChannel
	tracker *confirms.Tracker
	closeCh chan *amqp091.Error
	// activeTag holds the delivery tag of the in-flight publish (0 if none).
	// Stored as a pointer so that the copy returned by pool.acquire and the
	// original entry in the goroutine share the same atomic.
	activeTag *atomic.Uint64
}

// publisherConnPool is a per-publisher-TCP-connection pool of AMQP channels.
// It mirrors channelPool's semaphore design and adds an in-flight counter for
// least-in-flight pool selection across the publisher connection set.
type publisherConnPool struct {
	tokens   chan struct{}
	free     chan publisherEntry
	inflight atomic.Int64
	openFn   func() (publisherEntry, error)
}

func newPublisherConnPool(size int, openFn func() (publisherEntry, error)) *publisherConnPool {
	p := &publisherConnPool{
		tokens: make(chan struct{}, size),
		free:   make(chan publisherEntry, size),
		openFn: openFn,
	}
	for range size {
		p.tokens <- struct{}{}
	}
	return p
}

// acquire returns a pooled entry and a release func. Returns ErrChannelPoolExhausted
// if ctx is cancelled before a semaphore slot is available.
func (p *publisherConnPool) acquire(ctx context.Context) (publisherEntry, func(), error) {
	select {
	case <-ctx.Done():
		return publisherEntry{}, nil, fmt.Errorf("%w: %w", ErrChannelPoolExhausted, ctx.Err())
	case <-p.tokens:
	}

	entry, err := p.getOrOpen()
	if err != nil {
		p.tokens <- struct{}{}
		return publisherEntry{}, nil, err
	}
	return entry, func() { p.release(entry) }, nil
}

func (p *publisherConnPool) getOrOpen() (publisherEntry, error) {
	for {
		select {
		case entry := <-p.free:
			select {
			case <-entry.closeCh:
				_ = entry.ch.Close()
				continue
			default:
				return entry, nil
			}
		default:
			return p.openFn()
		}
	}
}

func (p *publisherConnPool) release(entry publisherEntry) {
	defer func() { p.tokens <- struct{}{} }()
	select {
	case <-entry.closeCh:
		_ = entry.ch.Close()
		return
	default:
	}
	select {
	case p.free <- entry:
	default:
		_ = entry.ch.Close()
	}
}

// drainFree closes and removes all idle entries from the free queue.
func (p *publisherConnPool) drainFree() {
	for {
		select {
		case entry := <-p.free:
			_ = entry.ch.Close()
		default:
			return
		}
	}
}

// drain discards idle entries. Called after reconnect so stale channels from
// the dead socket are never reused.
func (p *publisherConnPool) drain() { p.drainFree() }

// closeAll drains and closes all idle entries. Called by Publisher.Close.
func (p *publisherConnPool) closeAll() { p.drainFree() }

// openPublisherEntry opens a new AMQP channel on mc, enables publisher confirms,
// and starts a single goroutine that routes both basic.return and basic.ack/nack
// frames to the tracker. Using one goroutine for both frame types ensures that
// MarkReturned is always called before the corresponding Ack, which is required
// for correct ErrUnroutable resolution.
func (mc *managedConn) openPublisherEntry(poolSize int, onReturn func(amqp091.Return)) (publisherEntry, error) {
	mc.mu.RLock()
	raw := mc.raw
	mc.mu.RUnlock()
	if raw == nil {
		return publisherEntry{}, ErrNotConnected
	}
	ch, err := raw.Channel()
	if err != nil {
		return publisherEntry{}, wrapAMQPError(err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return publisherEntry{}, wrapAMQPError(err)
	}
	tracker := confirms.New()
	closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))

	buf := poolSize
	if buf < 8 {
		buf = 8
	}

	entry := publisherEntry{
		ch:        ch,
		tracker:   tracker,
		closeCh:   closeCh,
		activeTag: new(atomic.Uint64),
	}

	confirmCh := ch.NotifyPublish(make(chan amqp091.Confirmation, buf))
	returnCh := ch.NotifyReturn(make(chan amqp091.Return, buf))

	go func() {
		for {
			select {
			case ret, ok := <-returnCh:
				if !ok {
					returnCh = nil
					continue
				}
				tag := entry.activeTag.Load() //nolint:gocritic // entry.activeTag is always non-nil here
				if tag != 0 {
					if onReturn != nil {
						onReturn(ret)
					}
					tracker.MarkReturned(tag, ret.ReplyCode)
				}
			case c, ok := <-confirmCh:
				if !ok {
					tracker.CloseAll()
					return
				}
				// amqp091-go fans out basic.ack/nack multiple=true into individual
				// Confirmations per delivery tag, so Multiple is always false here.
				if c.Ack {
					tracker.Ack(c.DeliveryTag, false)
				} else {
					tracker.Nack(c.DeliveryTag, false)
				}
			}
		}
	}()

	return entry, nil
}

// defaultConfirmTimeout is the internal default when no ConfirmTimeout builder
// option is set. T13 exposes this via PublisherBuilder.ConfirmTimeout.
const defaultConfirmTimeout = 30 * time.Second

// Publisher publishes typed AMQP messages to the broker.
//
// Publisher is safe for concurrent use by multiple goroutines.
type Publisher[M any] struct {
	conn           *Connection
	pools          []*publisherConnPool
	mcs            []*managedConn
	exchange       string
	routingKey     string
	codec          codec.Codec
	pm             metrics.PublisherMetrics
	tracer         otel.Tracer
	confirmTimeout time.Duration
	mandatory      bool
	onReturn       func(Return)
	publishTimeout time.Duration
	// publishBatchMaxSize is validated at PublishBatch-time only (T22).
	publishBatchMaxSize int
	retryPolicy         *RetryPolicy
	stampUserID         bool
	// authUser is the identity from conn.AuthenticatedUser(); used for UserID
	// validation and StampUserID without holding the conn reference per-publish.
	authUser string

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// callOnReturn is called by the entry's return listener goroutine when a
// basic.return frame arrives. It converts the raw frame and invokes the
// user-supplied OnReturn callback (if any).
func (p *Publisher[M]) callOnReturn(r amqp091.Return) {
	if p.onReturn == nil {
		return
	}
	var exp time.Duration
	if ms := r.Expiration; ms != "" {
		if ms64, err := strconv.ParseInt(ms, 10, 64); err == nil {
			exp = time.Duration(ms64) * time.Millisecond
		}
	}
	p.onReturn(Return{
		ReplyCode:  r.ReplyCode,
		ReplyText:  r.ReplyText,
		Exchange:   r.Exchange,
		RoutingKey: r.RoutingKey,
		Properties: ReturnedProperties{
			ContentType:     r.ContentType,
			ContentEncoding: r.ContentEncoding,
			Headers:         Headers(r.Headers),
			DeliveryMode:    DeliveryMode(r.DeliveryMode), //nolint:gosec // G115: wire value
			Priority:        r.Priority,
			CorrelationID:   r.CorrelationId,
			ReplyTo:         r.ReplyTo,
			Expiration:      exp,
			MessageID:       r.MessageId,
			Timestamp:       r.Timestamp,
			Type:            r.Type,
			UserID:          r.UserId,
			AppID:           r.AppId,
		},
	})
}

// Health returns nil if the publisher's underlying connection is live.
func (p *Publisher[M]) Health(ctx context.Context) error {
	return p.conn.Health(ctx)
}

// Close drains all in-flight Publish calls and releases pool resources.
// Returns ErrAlreadyClosed if called more than once.
func (p *Publisher[M]) Close(_ context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrAlreadyClosed
	}
	p.closed = true
	p.mu.Unlock()

	p.wg.Wait()
	for _, pool := range p.pools {
		pool.closeAll()
	}
	return nil
}

// Publish encodes msg and sends it to the broker. It blocks until the broker
// sends a publisher confirm (basic.ack or basic.nack).
//
// Publish is safe for concurrent use by multiple goroutines.
func (p *Publisher[M]) Publish(ctx context.Context, msg Message[M]) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrAlreadyClosed
	}
	p.wg.Add(1)
	p.mu.Unlock()
	defer p.wg.Done()

	if p.publishTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.publishTimeout)
		defer cancel()
	}

	if err := msg.applyDefaults(p.codec); err != nil {
		return err
	}
	if err := msg.validateHeaders(); err != nil {
		return err
	}

	// StampUserID: auto-set UserID from the authenticated connection identity.
	if p.stampUserID && msg.UserID == "" {
		msg.UserID = p.authUser
	}

	// Client-side UserID validation: if caller set a UserID that doesn't match
	// the connection identity, reject locally without writing a publish frame.
	// This prevents the 406-channel-close footgun from a mismatched stamp.
	if msg.UserID != "" && p.authUser != "" && msg.UserID != p.authUser {
		return fmt.Errorf("%w: UserID %q does not match the authenticated connection identity", ErrInvalidMessage, msg.UserID)
	}

	body, err := p.codec.Encode(msg.Body)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}

	var attempt int
	for {
		err := p.publishOnce(ctx, msg, body)
		if err == nil {
			return nil
		}
		if p.retryPolicy == nil || !IsTransient(err) {
			return err
		}
		if p.retryPolicy.Retries > 0 && attempt >= p.retryPolicy.Retries {
			return err
		}
		attempt++
		p.pm.RecordRetry(p.exchange, retryReason(err))
		d := p.retryPolicy.NextBackoff(attempt)
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}

// publishOnce performs a single publish attempt (no retry logic).
func (p *Publisher[M]) publishOnce(ctx context.Context, msg Message[M], body []byte) error {
	exchange := p.exchange
	start := time.Now()

	pool, mc := p.selectPool()
	if err := mc.waitBarrier(ctx); err != nil {
		return err
	}

	entry, release, err := pool.acquire(ctx)
	if err != nil {
		return err
	}
	pool.inflight.Add(1)
	p.pm.InFlightAdd(exchange, 1)
	defer func() {
		pool.inflight.Add(-1)
		p.pm.InFlightAdd(exchange, -1)
		release()
	}()

	pub := buildPublishing(msg, body)

	deliveryTag := entry.ch.GetNextPublishSeqNo()
	if entry.activeTag != nil {
		entry.activeTag.Store(deliveryTag)
		defer entry.activeTag.Store(0)
	}

	if err := entry.tracker.Register(deliveryTag); err != nil {
		p.pm.RecordPublish(exchange, "error", time.Since(start))
		return p.mapConfirmError(err)
	}

	if err := entry.ch.PublishWithContext(ctx, exchange, p.routingKey, p.mandatory, false, pub); err != nil {
		entry.tracker.Cancel(deliveryTag)
		p.pm.RecordPublish(exchange, "error", time.Since(start))
		return wrapAMQPError(err)
	}

	waitErr := entry.tracker.Wait(ctx, deliveryTag, p.confirmTimeout)
	if waitErr != nil {
		p.pm.RecordPublish(exchange, "error", time.Since(start))
		return p.mapConfirmError(waitErr)
	}

	p.pm.RecordPublish(exchange, "success", time.Since(start))
	return nil
}

// selectPool returns the publisher connection pool with the lowest in-flight count.
func (p *Publisher[M]) selectPool() (*publisherConnPool, *managedConn) {
	minFlight := int64(-1)
	minIdx := 0
	for i, pool := range p.pools {
		n := pool.inflight.Load()
		if minFlight < 0 || n < minFlight {
			minFlight = n
			minIdx = i
		}
	}
	return p.pools[minIdx], p.mcs[minIdx]
}

// mapConfirmError translates internal confirms sentinel errors to public sentinels.
func (p *Publisher[M]) mapConfirmError(err error) error {
	switch {
	case errors.Is(err, confirms.ErrTimeout):
		return ErrConfirmTimeout
	case errors.Is(err, confirms.ErrNacked):
		return ErrPublishNacked
	case errors.Is(err, confirms.ErrClosed):
		return ErrChannelClosed
	default:
		var ue *confirms.UnroutableError
		if errors.As(err, &ue) {
			return wrapCode(ue.ReplyCode, ErrUnroutable)
		}
		return err
	}
}

// retryReason maps an error to the publisher_retry_total reason label.
func retryReason(err error) string {
	switch {
	case errors.Is(err, ErrPublishNacked):
		return "nacked"
	case errors.Is(err, ErrConfirmTimeout):
		return "confirm_timeout"
	case errors.Is(err, ErrChannelClosed):
		return "channel_closed"
	case errors.Is(err, ErrChannelPoolExhausted):
		return "pool_exhausted"
	case errors.Is(err, ErrConnectionBlocked):
		return "blocked"
	case errors.Is(err, ErrReconnecting):
		return "reconnecting"
	default:
		return "network"
	}
}

// buildPublishing converts a Message[M] into an amqp091.Publishing frame.
func buildPublishing[M any](msg Message[M], body []byte) amqp091.Publishing {
	pub := amqp091.Publishing{
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		Headers:         amqp091.Table(msg.Headers),
		DeliveryMode:    uint8(msg.DeliveryMode), //nolint:gosec // G115: wire values are spec-defined
		Priority:        msg.Priority,
		MessageId:       msg.MessageID,
		CorrelationId:   msg.CorrelationID,
		ReplyTo:         msg.ReplyTo,
		Type:            msg.Type,
		AppId:           msg.AppID,
		UserId:          msg.UserID,
		Timestamp:       msg.Timestamp,
		Body:            body,
	}
	if msg.Expiration > 0 {
		pub.Expiration = fmt.Sprintf("%d", msg.Expiration.Milliseconds())
	}
	return pub
}
