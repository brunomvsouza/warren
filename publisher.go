package amqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/amqp/codec"
	"github.com/brunomvsouza/amqp/internal/confirms"
	"github.com/brunomvsouza/amqp/metrics"
	"github.com/brunomvsouza/amqp/otel"
)

// pubChannel is the AMQP channel interface required by Publisher.
// *amqp091.Channel satisfies this interface; tests may inject fakes.
type pubChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp091.Publishing) error
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp091.Confirmation) chan amqp091.Confirmation
	NotifyClose(c chan *amqp091.Error) chan *amqp091.Error
	GetNextPublishSeqNo() uint64
	Close() error
}

// publisherEntry bundles a publisher channel with its per-lifetime confirm tracker.
// One entry is created per channel open; the tracker survives pool recycles until
// the channel closes.
type publisherEntry struct {
	ch      pubChannel
	tracker *confirms.Tracker
	closeCh chan *amqp091.Error
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

// drain discards idle entries. Called after reconnect so stale channels from
// the dead socket are never reused.
func (p *publisherConnPool) drain() {
	for {
		select {
		case entry := <-p.free:
			_ = entry.ch.Close()
		default:
			return
		}
	}
}

// closeAll drains and closes all idle entries. Called by Publisher.Close.
func (p *publisherConnPool) closeAll() {
	for {
		select {
		case entry := <-p.free:
			_ = entry.ch.Close()
		default:
			return
		}
	}
}

// openPublisherEntry opens a new AMQP channel on mc, enables publisher confirms,
// and starts the goroutine that routes ack/nack frames to the tracker.
func (mc *managedConn) openPublisherEntry(poolSize int) (publisherEntry, error) {
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
	go func() {
		// amqp091-go fans out basic.ack/nack multiple=true into individual
		// Confirmations per delivery tag, so Multiple is always false here.
		for c := range ch.NotifyPublish(make(chan amqp091.Confirmation, buf)) {
			if c.Ack {
				tracker.Ack(c.DeliveryTag, false)
			} else {
				tracker.Nack(c.DeliveryTag, false)
			}
		}
		tracker.CloseAll()
	}()
	return publisherEntry{ch: ch, tracker: tracker, closeCh: closeCh}, nil
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

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
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

	if err := msg.applyDefaults(p.codec); err != nil {
		return err
	}
	if err := msg.validateHeaders(); err != nil {
		return err
	}

	body, err := p.codec.Encode(msg.Body)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}

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
	if err := entry.tracker.Register(deliveryTag); err != nil {
		p.pm.RecordPublish(exchange, "error", time.Since(start))
		return p.mapConfirmError(err)
	}

	if err := entry.ch.PublishWithContext(ctx, exchange, p.routingKey, false, false, pub); err != nil {
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
			// T13 will integrate OnReturn callback here.
			return fmt.Errorf("%w (reply code %d)", ErrUnroutable, ue.ReplyCode)
		}
		return err
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
