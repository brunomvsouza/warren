package warren

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/internal/confirms"
	"github.com/brunomvsouza/warren/metrics"
)

// replierDropSpy embeds the NoOp recorder and captures only the load-bearing
// replier_drop_no_dlx_total increments so the silent-drop observability contract
// can be asserted.
type replierDropSpy struct {
	metrics.NoOpConsumerMetrics
	mu    sync.Mutex
	drops []string
}

func (s *replierDropSpy) RecordReplierDropNoDLX(queue string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drops = append(s.drops, queue)
}

func (s *replierDropSpy) dropped() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.drops))
	copy(out, s.drops)
	return out
}

// verdictRecorder captures the ack/nack verdicts (and their order relative to the
// reply publish) applied to a fabricated request delivery.
type verdictRecorder struct {
	mu      sync.Mutex
	acks    int
	nackReq []bool
	order   []string
}

func (v *verdictRecorder) recordAck() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.acks++
	v.order = append(v.order, "ack")
}

func (v *verdictRecorder) recordNack(requeue bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.nackReq = append(v.nackReq, requeue)
	v.order = append(v.order, "nack")
}

func (v *verdictRecorder) note(ev string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.order = append(v.order, ev)
}

func (v *verdictRecorder) snapshot() (acks int, nacks []bool, order []string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	nacks = append(nacks, v.nackReq...)
	order = append(order, v.order...)
	return v.acks, nacks, order
}

func (v *verdictRecorder) acknowledger() *fakeAcknowledger {
	return &fakeAcknowledger{
		ackFn:  func(uint64, bool) error { v.recordAck(); return nil },
		nackFn: func(_ uint64, _, requeue bool) error { v.recordNack(requeue); return nil },
	}
}

// fakeReplyPublisher records reply publishes and can be primed to fail, standing
// in for the broker-backed replyPublisher in raw-handler unit tests.
type fakeReplyPublisher struct {
	mu       sync.Mutex
	bodies   [][]byte
	replyTos []string
	corrIDs  []string
	err      error
	onCall   func()
	closed   bool
}

func (f *fakeReplyPublisher) publish(_ context.Context, replyTo string, msg amqp091.Publishing) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onCall != nil {
		f.onCall()
	}
	if f.err != nil {
		return f.err
	}
	f.bodies = append(f.bodies, msg.Body)
	f.replyTos = append(f.replyTos, replyTo)
	f.corrIDs = append(f.corrIDs, msg.CorrelationId)
	return nil
}

func (f *fakeReplyPublisher) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeReplyPublisher) snapshot() (bodies [][]byte, replyTos, corrIDs []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append(bodies, f.bodies...), append(replyTos, f.replyTos...), append(corrIDs, f.corrIDs...)
}

// fabricateRequest builds a *Delivery[echoPayload] with the given reply address
// and correlation id, wired to ack, for raw-handler unit tests.
func fabricateRequest(t *testing.T, body echoPayload, replyTo, corrID string, ack amqp091.Acknowledger) *Delivery[echoPayload] {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	d := amqp091.Delivery{
		Acknowledger:  ack,
		CorrelationId: corrID,
		ReplyTo:       replyTo,
		ContentType:   "application/json",
		Body:          raw,
		DeliveryTag:   1,
	}
	return newDelivery[echoPayload](&body, "rpc.requests", d, nil)
}

func newTestReplier(cm metrics.ConsumerMetrics, pub replyPublisherIface, knownHasDLX bool, onError func(context.Context, echoPayload, error)) *Replier[echoPayload, echoPayload] {
	return &Replier[echoPayload, echoPayload]{
		queue:       "rpc.requests",
		codec:       codec.NewJSON(),
		cm:          cm,
		replyPub:    pub,
		knownHasDLX: knownHasDLX,
		onError:     onError,
	}
}

func TestReplier_RawHandler_Success_PublishesThenAcks(t *testing.T) {
	pub := &fakeReplyPublisher{}
	rec := &verdictRecorder{}
	pub.onCall = func() { rec.note("publish") }

	r := newTestReplier(&replierDropSpy{}, pub, true, nil)
	h := func(_ context.Context, req echoPayload) (echoPayload, error) {
		return echoPayload{N: req.N * 2}, nil
	}
	d := fabricateRequest(t, echoPayload{N: 21}, "amq.rabbitmq.reply-to.abc", "corr-1", rec.acknowledger())

	require.NoError(t, r.makeRawHandler(h)(context.Background(), d))

	bodies, replyTos, corrIDs := pub.snapshot()
	require.Len(t, bodies, 1, "exactly one reply must be published")
	var resp echoPayload
	require.NoError(t, json.Unmarshal(bodies[0], &resp))
	assert.Equal(t, 42, resp.N, "reply must carry the handler's response")
	assert.Equal(t, "amq.rabbitmq.reply-to.abc", replyTos[0], "reply routes to the request ReplyTo")
	assert.Equal(t, "corr-1", corrIDs[0], "reply must echo the request CorrelationID")

	acks, nacks, order := rec.snapshot()
	assert.Equal(t, 1, acks, "the request must be acked on success")
	assert.Empty(t, nacks)
	assert.Equal(t, []string{"publish", "ack"}, order, "at-least-once ordering: publish+confirm before ack")
}

func TestReplier_RawHandler_HandlerError_NoDLX_DropsAndIncrementsMetric(t *testing.T) {
	pub := &fakeReplyPublisher{}
	rec := &verdictRecorder{}
	spy := &replierDropSpy{}

	var gotErr error
	var gotReq echoPayload
	onError := func(_ context.Context, req echoPayload, err error) {
		gotReq = req
		gotErr = err
	}
	r := newTestReplier(spy, pub, false /* no DLX */, onError)

	sentinel := errors.New("handler boom")
	h := func(_ context.Context, _ echoPayload) (echoPayload, error) {
		return echoPayload{}, sentinel
	}
	d := fabricateRequest(t, echoPayload{N: 5}, "reply.addr", "corr-2", rec.acknowledger())

	require.NoError(t, r.makeRawHandler(h)(context.Background(), d))

	bodies, _, _ := pub.snapshot()
	assert.Empty(t, bodies, "a handler error must not publish a reply envelope")
	acks, nacks, _ := rec.snapshot()
	assert.Equal(t, []bool{false}, nacks, "handler error nacks without requeue")
	assert.Equal(t, 0, acks)
	assert.ErrorIs(t, gotErr, sentinel, "OnError must receive the original handler error")
	assert.Equal(t, 5, gotReq.N, "OnError must receive the decoded request")
	assert.Equal(t, []string{"rpc.requests"}, spy.dropped(), "a drop with no DLX must increment the metric")
}

func TestReplier_RawHandler_HandlerError_WithDLX_NoMetric(t *testing.T) {
	pub := &fakeReplyPublisher{}
	rec := &verdictRecorder{}
	spy := &replierDropSpy{}
	var onErrCalls int
	r := newTestReplier(spy, pub, true /* has DLX */, func(context.Context, echoPayload, error) { onErrCalls++ })

	h := func(_ context.Context, _ echoPayload) (echoPayload, error) {
		return echoPayload{}, errors.New("boom")
	}
	d := fabricateRequest(t, echoPayload{N: 1}, "reply.addr", "corr-3", rec.acknowledger())

	require.NoError(t, r.makeRawHandler(h)(context.Background(), d))

	_, nacks, _ := rec.snapshot()
	assert.Equal(t, []bool{false}, nacks)
	assert.Equal(t, 1, onErrCalls, "OnError fires once on handler error")
	assert.Empty(t, spy.dropped(), "a configured DLX means the drop is not silent: no metric")
}

func TestReplier_RawHandler_ReplyPublishFails_NacksAndIncrementsMetric(t *testing.T) {
	pub := &fakeReplyPublisher{err: ErrChannelClosed}
	rec := &verdictRecorder{}
	spy := &replierDropSpy{}
	var onErrCalls int
	r := newTestReplier(spy, pub, false /* no DLX */, func(context.Context, echoPayload, error) { onErrCalls++ })

	h := func(_ context.Context, req echoPayload) (echoPayload, error) {
		return echoPayload{N: req.N}, nil
	}
	d := fabricateRequest(t, echoPayload{N: 9}, "reply.addr", "corr-4", rec.acknowledger())

	require.NoError(t, r.makeRawHandler(h)(context.Background(), d))

	acks, nacks, _ := rec.snapshot()
	assert.Equal(t, []bool{false}, nacks, "a failed reply publish nacks the request to its DLX")
	assert.Equal(t, 0, acks, "the request must NOT be acked when the reply never landed")
	assert.Equal(t, 0, onErrCalls, "reply-publish failure is not a handler error; OnError must not fire")
	assert.Equal(t, []string{"rpc.requests"}, spy.dropped())
}

// badResp is a response payload that cannot be JSON-encoded (channels are
// unsupported by encoding/json), used to drive the un-encodable-reply branch.
type badResp struct {
	Ch chan int `json:"ch"`
}

func TestReplier_RawHandler_EmptyReplyTo_DropsAndSignalsOnError(t *testing.T) {
	pub := &fakeReplyPublisher{}
	rec := &verdictRecorder{}
	spy := &replierDropSpy{}
	var onErrCalls int
	var gotErr error
	r := newTestReplier(spy, pub, false /* no DLX */, func(_ context.Context, _ echoPayload, err error) {
		onErrCalls++
		gotErr = err
	})

	// The handler succeeds, but the request carries no ReplyTo address, so the
	// reply has nowhere to go.
	h := func(_ context.Context, req echoPayload) (echoPayload, error) {
		return echoPayload{N: req.N}, nil
	}
	d := fabricateRequest(t, echoPayload{N: 3}, "" /* no replyTo */, "corr-empty", rec.acknowledger())

	require.NoError(t, r.makeRawHandler(h)(context.Background(), d))

	bodies, _, _ := pub.snapshot()
	assert.Empty(t, bodies, "no reply can be published without a reply address")
	acks, nacks, _ := rec.snapshot()
	assert.Equal(t, []bool{false}, nacks, "an unanswerable request is nacked without requeue")
	assert.Equal(t, 0, acks, "an unanswerable request must not be acked")
	assert.Equal(t, 1, onErrCalls, "a missing reply address must signal OnError")
	assert.ErrorIs(t, gotErr, ErrInvalidMessage, "the missing-ReplyTo signal wraps ErrInvalidMessage")
	assert.Equal(t, []string{"rpc.requests"}, spy.dropped(), "a drop with no DLX increments the metric")
}

func TestReplier_RawHandler_UnencodableResponse_DropsAndSignalsOnError(t *testing.T) {
	pub := &fakeReplyPublisher{}
	rec := &verdictRecorder{}
	spy := &replierDropSpy{}
	var onErrCalls int
	var gotErr error
	r := &Replier[echoPayload, badResp]{
		queue:       "rpc.requests",
		codec:       codec.NewJSON(),
		cm:          spy,
		replyPub:    pub,
		knownHasDLX: false,
		onError: func(_ context.Context, _ echoPayload, err error) {
			onErrCalls++
			gotErr = err
		},
	}

	// The handler succeeds but returns a value the codec cannot encode.
	h := func(_ context.Context, _ echoPayload) (badResp, error) {
		return badResp{Ch: make(chan int)}, nil
	}
	d := fabricateRequest(t, echoPayload{N: 1}, "reply.addr", "corr-enc", rec.acknowledger())

	require.NoError(t, r.makeRawHandler(h)(context.Background(), d))

	bodies, _, _ := pub.snapshot()
	assert.Empty(t, bodies, "an un-encodable response must not be published")
	acks, nacks, _ := rec.snapshot()
	assert.Equal(t, []bool{false}, nacks, "an un-encodable response nacks the request without requeue")
	assert.Equal(t, 0, acks, "the request must not be acked when no reply was sent")
	assert.Equal(t, 1, onErrCalls, "an encode failure is reported via OnError")
	assert.ErrorIs(t, gotErr, ErrInvalidMessage, "the encode failure wraps ErrInvalidMessage")
	assert.Equal(t, []string{"rpc.requests"}, spy.dropped())
}

func TestReplierBuilder_Build_DLXValidation(t *testing.T) {
	conn := &Connection{
		conConns: []*managedConn{{}},
		pubConns: []*managedConn{{}},
	}

	t.Run("Topology without DeadLetter for the queue -> ErrInvalidOptions", func(t *testing.T) {
		topo := &Topology{Queues: []Queue{{Name: "rpc.q"}}}
		_, err := ReplierFor[echoPayload, echoPayload](conn).
			Queue("rpc.q").
			Topology(topo).
			Build()
		require.ErrorIs(t, err, ErrInvalidOptions)
		assert.Contains(t, err.Error(), "no DeadLetter entry")
	})

	t.Run("AllowMissingDLX opts out of the validation", func(t *testing.T) {
		topo := &Topology{Queues: []Queue{{Name: "rpc.q"}}}
		_, err := ReplierFor[echoPayload, echoPayload](conn).
			Queue("rpc.q").
			Topology(topo).
			AllowMissingDLX().
			Build()
		require.NoError(t, err)
	})

	t.Run("Topology with a matching DeadLetter -> success", func(t *testing.T) {
		topo := &Topology{
			Queues:      []Queue{{Name: "rpc.q"}},
			DeadLetters: []DeadLetter{{Source: "rpc.q"}},
		}
		_, err := ReplierFor[echoPayload, echoPayload](conn).
			Queue("rpc.q").
			Topology(topo).
			Build()
		require.NoError(t, err)
	})

	t.Run("Topology with a manual x-dead-letter-exchange arg -> success", func(t *testing.T) {
		// A DLX wired directly through Queue.Args (no DeadLetter entry) is a real
		// DLX; topologyHasDLX must recognise it so the Replier does not reject a
		// valid manual-DLX config — parity with the Consumer missing-DLX check.
		topo := &Topology{
			Queues: []Queue{{Name: "rpc.q", Args: map[string]any{
				"x-dead-letter-exchange": "rpc.dlx",
			}}},
		}
		_, err := ReplierFor[echoPayload, echoPayload](conn).
			Queue("rpc.q").
			Topology(topo).
			Build()
		require.NoError(t, err)
	})

	t.Run("nil conn / empty queue", func(t *testing.T) {
		_, err := ReplierFor[echoPayload, echoPayload](nil).Queue("q").Build()
		assert.ErrorIs(t, err, ErrInvalidOptions)
		_, err = ReplierFor[echoPayload, echoPayload](conn).Build()
		assert.ErrorIs(t, err, ErrInvalidOptions)
	})
}

// TestReplierBuilder_OptionsAreLastWins asserts the last-wins option policy that
// CLAUDE.md lists as a builder invariant (calling a setter twice keeps the final).
func TestReplierBuilder_OptionsAreLastWins(t *testing.T) {
	t1, t2 := &Topology{}, &Topology{}
	b := ReplierFor[echoPayload, echoPayload](nil).
		Queue("q1").Queue("q2").
		Topology(t1).Topology(t2).
		ConfirmTimeout(time.Second).ConfirmTimeout(5 * time.Second)
	assert.Equal(t, "q2", b.queue)
	assert.Same(t, t2, b.topology, "the last Topology wins")
	assert.Equal(t, 5*time.Second, b.confirmTimeout)
	assert.True(t, b.confirmTimeoutSet)

	strict := codec.NewJSONStrict()
	b2 := ReplierFor[echoPayload, echoPayload](nil).Codec(codec.NewJSON()).Codec(strict)
	assert.Equal(t, strict, b2.c, "the last Codec wins")
}

// TestReplyPublisher_ensureEntryLocked_ReusesLiveEntryAndReopensAfterClose drives
// the lazy-reopen seam directly. This is the load-bearing transport seam of the
// at-least-once reply path: after a reconnect closes the confirm channel, the
// Replier MUST transparently reopen it, or every subsequent reply fails. A live
// entry is reused without reopening; a closed channel (closeCh fired) triggers
// exactly one reopen with a fresh entry.
func TestReplyPublisher_ensureEntryLocked_ReusesLiveEntryAndReopensAfterClose(t *testing.T) {
	fake1 := newFakePubCh(false)
	fake2 := newFakePubCh(false)
	entries := []publisherEntry{
		{ch: fake1, tracker: confirms.New(), closeCh: fake1.closedCh, returnTagMap: &sync.Map{}},
		{ch: fake2, tracker: confirms.New(), closeCh: fake2.closedCh, returnTagMap: &sync.Map{}},
	}
	var opens int
	rp := &replyPublisher{
		confirmTimeout: time.Second,
		openEntry: func(context.Context) (publisherEntry, error) {
			e := entries[opens]
			opens++
			return e, nil
		},
	}
	ctx := context.Background()

	// First acquire opens entry #1.
	e1, err := rp.ensureEntryLocked(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, opens)
	assert.True(t, e1.ch == pubChannel(fake1), "first acquire returns entry #1")

	// A live entry is reused — the default select branch returns it, no reopen.
	e1b, err := rp.ensureEntryLocked(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, opens, "a live entry must be reused, not reopened")
	assert.True(t, e1b.ch == pubChannel(fake1), "reuse returns the same entry")

	// Fire entry #1's close notification: the next acquire detects the dead
	// channel, closes the stale entry, and reopens entry #2 exactly once.
	fake1.closedCh <- &amqp091.Error{Code: 504, Reason: "CHANNEL_ERROR"}
	e2, err := rp.ensureEntryLocked(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, opens, "a closed channel must trigger exactly one reopen")
	assert.True(t, e2.ch == pubChannel(fake2), "reopen swaps in the fresh entry")
}

// TestMapConfirmSentinel asserts the internal confirms sentinels are translated to
// the public reply errors (and an unrecognised error passes through unchanged).
func TestMapConfirmSentinel(t *testing.T) {
	assert.ErrorIs(t, mapConfirmSentinel(confirms.ErrTimeout), ErrConfirmTimeout)
	assert.ErrorIs(t, mapConfirmSentinel(confirms.ErrNacked), ErrPublishNacked)
	assert.ErrorIs(t, mapConfirmSentinel(confirms.ErrClosed), ErrChannelClosed)

	other := errors.New("some other error")
	assert.Equal(t, other, mapConfirmSentinel(other), "an unrecognised error is returned unchanged")
}
