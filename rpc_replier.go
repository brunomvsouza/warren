package warren

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/internal/confirms"
	"github.com/brunomvsouza/warren/metrics"
)

// replyPublisherIface is the reply-publish surface the Replier depends on. The
// production implementation is *replyPublisher; unit tests inject a fake to drive
// the at-least-once ordering and drop accounting without a broker.
type replyPublisherIface interface {
	publish(ctx context.Context, replyTo string, msg amqp091.Publishing) error
	close()
}

// Replier serves request/reply RPC: it consumes requests from a queue, runs a
// ReplyHandler, and publishes the response to the request's ReplyTo with the
// matching CorrelationID.
//
// At-least-once reply ordering (SPEC §6.7). For a successful handler the Replier
// publishes the reply, AWAITS its broker confirm, and only THEN acks the request.
// If the reply publish fails (ErrPublishNacked, ErrConfirmTimeout, ErrChannelClosed)
// the request is Nack(false)'d so it routes to the request queue's DLX (if any) and
// the caller observes ErrCallTimeout on its ctx deadline. A crash between the reply
// confirm and the request ack causes the broker to redeliver the request and the
// Replier to send a SECOND reply — callers MUST treat replies as at-least-once and
// dedupe by CorrelationID.
//
// Handler errors never produce an error envelope on the wire. A handler that
// returns a non-nil error triggers Nack(false) on the request and invokes the
// OnError hook; the caller just times out on its ctx. Without a DLX on the request
// queue, Nack(false) is a silent drop — the mandatory metric
// replier_drop_no_dlx_total makes it observable, and OnError is the only
// client-side signal. Configure a DLX on the request queue if you need failed
// requests preserved for forensics.
//
// Use ReplierFor[Req, Resp](conn) to build a Replier.
type Replier[Req, Resp any] struct {
	queue string

	codec   codec.Codec
	cm      metrics.ConsumerMetrics
	onError func(ctx context.Context, req Req, err error)

	// knownHasDLX is true only when a Topology with a matching DeadLetter was wired
	// at Build time. When false, every Nack(false) increments
	// replier_drop_no_dlx_total — the framework cannot prove a DLX exists, so drops
	// are never invisible even when OnError is not wired.
	knownHasDLX bool

	consumer *Consumer[Req]
	replyPub replyPublisherIface
}

// Serve consumes requests and serves them with h until ctx is cancelled, at which
// point it waits for in-flight handlers to finish and returns. Serve may only be
// called once per Replier (the underlying consumer is single-use; build a new
// Replier to restart).
//
// See the Replier type docs for the at-least-once reply ordering and the
// crash-between-confirm-and-ack window that callers must dedupe against by
// CorrelationID.
func (r *Replier[Req, Resp]) Serve(ctx context.Context, h ReplyHandler[Req, Resp]) error {
	defer r.replyPub.close()
	return r.consumer.ConsumeRaw(ctx, r.makeRawHandler(h))
}

// makeRawHandler adapts a ReplyHandler into the RawHandler the internal consumer
// drives, enforcing the at-least-once publish→confirm→ack ordering and the
// handler-error / reply-failure drop semantics.
func (r *Replier[Req, Resp]) makeRawHandler(h ReplyHandler[Req, Resp]) RawHandler[Req] {
	return func(ctx context.Context, d *Delivery[Req]) error {
		req := *d.Body()

		resp, herr := h(ctx, req)
		if herr != nil {
			if r.onError != nil {
				r.onError(ctx, req, herr)
			}
			r.nackDrop(d)
			return nil
		}

		replyTo := d.raw.ReplyTo
		if replyTo == "" {
			// No reply address: the request cannot be answered. Drop it (its DLX
			// preserves it if configured) rather than leave it unacked forever.
			r.nackDrop(d)
			return nil
		}

		replyMsg := Message[Resp]{Body: &resp, CorrelationID: d.CorrelationID()}
		body, err := encodeMessageBody(r.codec, &replyMsg)
		if err != nil {
			// An un-encodable response is treated like a handler failure: report + drop.
			if r.onError != nil {
				r.onError(ctx, req, err)
			}
			r.nackDrop(d)
			return nil //nolint:nilerr // verdict applied via nackDrop; the RawHandler owns ack/nack and returns nil
		}

		if err := r.replyPub.publish(ctx, replyTo, buildPublishing(replyMsg, body)); err != nil {
			// The reply never landed: Nack(false) so the request goes to its DLX and
			// the caller times out. This is NOT a handler error — OnError does not fire.
			r.nackDrop(d)
			return nil //nolint:nilerr // verdict applied via nackDrop; the RawHandler owns ack/nack and returns nil
		}

		// Reply confirmed: ack the request. A failed ack (channel closed) means the
		// broker redelivers and the handler runs again — the at-least-once window the
		// caller dedupes by CorrelationID.
		_ = d.Ack()
		return nil
	}
}

// nackDrop nacks a request without requeue and, when no DLX is known to preserve
// it, increments replier_drop_no_dlx_total so the silent drop stays observable.
func (r *Replier[Req, Resp]) nackDrop(d *Delivery[Req]) {
	if !r.knownHasDLX {
		r.cm.RecordReplierDropNoDLX(r.queue)
	}
	_ = d.Nack(false)
}

// Health reports whether the Replier's consumer connection is healthy.
func (r *Replier[Req, Resp]) Health(ctx context.Context) error {
	return r.consumer.Health(ctx)
}

// replyPublisher publishes confirm-tracked replies to dynamic routing keys on a
// pinned publisher-role connection. It lazily opens a confirm-enabled channel and
// transparently reopens it after a channel close (reconnect).
type replyPublisher struct {
	mc             *managedConn
	confirmTimeout time.Duration
	poolSize       int

	mu    sync.Mutex
	entry publisherEntry
	open  bool
}

// publish writes msg to the default exchange keyed on replyTo, then blocks on the
// broker confirm (bounded by confirmTimeout). The tag-allocate→register→publish
// critical section is serialised under mu so concurrent replies cannot interleave
// delivery tags on the shared channel; the confirm wait runs lock-free.
func (rp *replyPublisher) publish(ctx context.Context, replyTo string, msg amqp091.Publishing) error {
	rp.mu.Lock()
	e, err := rp.ensureEntryLocked(ctx)
	if err != nil {
		rp.mu.Unlock()
		return err
	}
	tag := e.ch.GetNextPublishSeqNo()
	if rerr := e.tracker.Register(tag); rerr != nil {
		rp.mu.Unlock()
		return mapConfirmSentinel(rerr)
	}
	if perr := e.ch.PublishWithContext(ctx, "", replyTo, false, false, msg); perr != nil {
		e.tracker.Cancel(tag)
		rp.mu.Unlock()
		return wrapAMQPError(perr)
	}
	rp.mu.Unlock()

	if werr := e.tracker.Wait(ctx, tag, rp.confirmTimeout); werr != nil {
		return mapConfirmSentinel(werr)
	}
	return nil
}

// ensureEntryLocked returns a live confirm-enabled entry, reopening it if the
// current channel has closed. Caller must hold rp.mu.
func (rp *replyPublisher) ensureEntryLocked(ctx context.Context) (publisherEntry, error) {
	if rp.open {
		select {
		case <-rp.entry.closeCh:
			_ = rp.entry.ch.Close()
			rp.open = false
		default:
			return rp.entry, nil
		}
	}
	if err := rp.mc.waitBarrier(ctx); err != nil {
		return publisherEntry{}, err
	}
	e, err := rp.mc.openPublisherEntry(rp.poolSize, nil)
	if err != nil {
		return publisherEntry{}, err
	}
	rp.entry = e
	rp.open = true
	return e, nil
}

func (rp *replyPublisher) close() {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.open {
		_ = rp.entry.ch.Close()
		rp.open = false
	}
}

// mapConfirmSentinel translates internal confirms sentinels to the public reply
// errors. The reply publish is never mandatory, so confirms.UnroutableError cannot
// arise here (that path needs basic.return); the three failure sentinels suffice.
func mapConfirmSentinel(err error) error {
	switch {
	case errors.Is(err, confirms.ErrTimeout):
		return ErrConfirmTimeout
	case errors.Is(err, confirms.ErrNacked):
		return ErrPublishNacked
	case errors.Is(err, confirms.ErrClosed):
		return ErrChannelClosed
	default:
		return err
	}
}
