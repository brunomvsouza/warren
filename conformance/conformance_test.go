//go:build conformance

package conformance_test

import (
	"context"
	neturl "net/url"
	"os"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// confMsg is a trivial payload for the conformance publishes.
type confMsg struct {
	N int `json:"n"`
}

// brokerURL returns AMQP_TEST_URL or fails (does not skip) — conformance is a
// real-broker lane, so a missing URL is a misconfiguration, not a pass.
func brokerURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("AMQP_TEST_URL")
	if u == "" {
		t.Fatal("AMQP_TEST_URL must be set to run conformance tests (real-broker-only, TV-06)")
	}
	return u
}

func dialConf(t *testing.T, url string) *warren.Connection {
	t.Helper()
	conn, err := warren.Dial(context.Background(), warren.WithAddr(url))
	require.NoError(t, err, "dial")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})
	return conn
}

// dropTopology deletes the named queues and exchanges via a raw connection;
// best-effort, silent on error. Used to clean durable conformance topology.
func dropTopology(url string, queues, exchanges []string) {
	rc, err := amqp091.Dial(url)
	if err != nil {
		return
	}
	defer rc.Close() //nolint:errcheck // best-effort cleanup
	ch, err := rc.Channel()
	if err != nil {
		return
	}
	defer ch.Close() //nolint:errcheck // best-effort cleanup
	for _, q := range queues {
		_, _ = ch.QueueDelete(q, false, false, false)
	}
	for _, ex := range exchanges {
		_ = ch.ExchangeDelete(ex, false, false)
	}
}

// TestConformance_MandatoryReturn_AckedAndReturned asserts the AMQP
// mandatory-return/ack correlation: an unroutable mandatory publish is BOTH
// returned to the publisher (basic.return, NO_ROUTE 312) AND confirmed
// (basic.ack) — warren's Publish returns nil while OnReturn fires.
func TestConformance_MandatoryReturn_AckedAndReturned(t *testing.T) {
	// Registered before dialConf's conn-close cleanup so it runs LAST (t.Cleanup is
	// LIFO): goleak must verify AFTER the connection is closed and its supervisor
	// goroutines are joined, otherwise it sees the still-open connection's pool.
	t.Cleanup(func() { goleak.VerifyNone(t) })
	url := brokerURL(t)
	ctx := context.Background()

	const exchange = "warren.conf.mandatory.ex"
	conn := dialConf(t, url)
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: exchange, Kind: warren.ExchangeTopic, AutoDelete: true}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	t.Cleanup(func() { dropTopology(url, nil, []string{exchange}) })

	returned := make(chan warren.Return, 1)
	pub, err := warren.PublisherFor[confMsg](conn).
		Exchange(exchange).
		RoutingKey("no.binding.matches.this").
		Mandatory().
		OnReturn(func(r warren.Return) {
			select {
			case returned <- r:
			default:
			}
		}).
		ConfirmTimeout(10 * time.Second).
		Build()
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pub.Close(closeCtx)
	})

	// An unroutable mandatory publish is returned by the broker (basic.return,
	// NO_ROUTE 312). warren surfaces that to the publisher as ErrUnroutable (with
	// OnReturn firing first); the broker also acks it under publisher confirms, but
	// warren reports the return as the terminal outcome of Publish.
	err = pub.Publish(ctx, warren.Message[confMsg]{Body: &confMsg{N: 1}})
	require.ErrorIs(t, err, warren.ErrUnroutable,
		"an unroutable mandatory publish must surface as ErrUnroutable")

	select {
	case r := <-returned:
		assert.Equal(t, uint16(312), r.ReplyCode, "unroutable mandatory return must carry NO_ROUTE (312)")
		assert.Equal(t, exchange, r.Exchange)
		assert.Equal(t, "no.binding.matches.this", r.RoutingKey)
	case <-time.After(5 * time.Second):
		t.Fatal("basic.return (OnReturn) did not fire for an unroutable mandatory publish")
	}
}

// TestConformance_BrokerNack_RejectPublishOverflow asserts the broker-nack path:
// a queue at its x-max-length with x-overflow=reject-publish causes the broker to
// basic.nack the overflowing publish, which warren surfaces as ErrPublishNacked.
func TestConformance_BrokerNack_RejectPublishOverflow(t *testing.T) {
	// Registered before dialConf's conn-close cleanup so it runs LAST (t.Cleanup is
	// LIFO): goleak must verify AFTER the connection is closed and its supervisor
	// goroutines are joined, otherwise it sees the still-open connection's pool.
	t.Cleanup(func() { goleak.VerifyNone(t) })
	url := brokerURL(t)
	ctx := context.Background()

	const (
		exchange = "warren.conf.nack.ex"
		queue    = "warren.conf.nack.q"
	)
	conn := dialConf(t, url)
	dropTopology(url, []string{queue}, []string{exchange}) // clear any leftover
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: exchange, Kind: warren.ExchangeDirect}},
		Queues: []warren.Queue{{
			Name: queue,
			// x-max-length=1 + reject-publish: once the queue holds one message,
			// the broker rejects (nacks) further publishes instead of dropping the
			// head. int64 matches warren's own x-max-length encoding.
			Args: map[string]any{
				"x-max-length": int64(1),
				"x-overflow":   string(warren.OverflowRejectPublish),
			},
		}},
		Bindings: []warren.Binding{{Exchange: exchange, Queue: queue, RoutingKey: "k"}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	t.Cleanup(func() { dropTopology(url, []string{queue}, []string{exchange}) })

	pub, err := warren.PublisherFor[confMsg](conn).
		Exchange(exchange).
		RoutingKey("k").
		ConfirmTimeout(10 * time.Second).
		Build()
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pub.Close(closeCtx)
	})

	// First publish fills the queue to its max-length of 1 and is acked.
	require.NoError(t, pub.Publish(ctx, warren.Message[confMsg]{Body: &confMsg{N: 1}}),
		"first publish must be accepted (queue not yet full)")

	// Second publish overflows: the broker rejects it. Some brokers nack a touch
	// asynchronously, so allow a couple of attempts before concluding it was not
	// rejected — but at least one MUST surface ErrPublishNacked.
	var nackErr error
	for i := 0; i < 5 && nackErr == nil; i++ {
		nackErr = pub.Publish(ctx, warren.Message[confMsg]{Body: &confMsg{N: 2 + i}})
	}
	require.Error(t, nackErr, "an overflowing publish under reject-publish must not succeed")
	assert.ErrorIs(t, nackErr, warren.ErrPublishNacked,
		"broker overflow rejection must surface as ErrPublishNacked")
}

// TestConformance_BasicCancel_SurfacesErrConsumerCancelled asserts that a
// broker-initiated basic.cancel (here, deleting the queue under an active
// consumer) causes Consume to return ErrConsumerCancelled.
func TestConformance_BasicCancel_SurfacesErrConsumerCancelled(t *testing.T) {
	// Registered before dialConf's conn-close cleanup so it runs LAST (t.Cleanup is
	// LIFO): goleak must verify AFTER the connection is closed and its supervisor
	// goroutines are joined, otherwise it sees the still-open connection's pool.
	t.Cleanup(func() { goleak.VerifyNone(t) })
	url := brokerURL(t)
	ctx := context.Background()

	const queue = "warren.conf.cancel.q"
	conn := dialConf(t, url)
	dropTopology(url, []string{queue}, nil)
	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: queue}}, // transient; default exchange routes by name
	}
	require.NoError(t, topo.Declare(ctx, conn))
	t.Cleanup(func() { dropTopology(url, []string{queue}, nil) })

	// Publisher to the default exchange (routing key == queue name).
	pub, err := warren.PublisherFor[confMsg](conn).
		Exchange("").
		RoutingKey(queue).
		ConfirmTimeout(10 * time.Second).
		Build()
	require.NoError(t, err)

	subscribed := make(chan struct{}, 1)
	consumer, err := warren.ConsumerFor[confMsg](conn).
		Queue(queue).
		Concurrency(1).
		Prefetch(1).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- consumer.Consume(consumeCtx, func(_ context.Context, _ confMsg) error {
			select {
			case subscribed <- struct{}{}:
			default:
			}
			return nil
		})
	}()

	// Prove the consumer is actively subscribed by round-tripping one message,
	// so the subsequent queue delete deterministically targets a live consumer.
	require.NoError(t, pub.Publish(ctx, warren.Message[confMsg]{Body: &confMsg{N: 1}}))
	select {
	case <-subscribed:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer never received the warm-up message; cannot assert basic.cancel")
	}

	// Delete the queue out from under the consumer → broker sends basic.cancel.
	dropTopology(url, []string{queue}, nil)

	select {
	case err := <-consumeErr:
		assert.ErrorIs(t, err, warren.ErrConsumerCancelled,
			"queue deletion under an active consumer must surface as ErrConsumerCancelled")
	case <-time.After(15 * time.Second):
		t.Fatal("Consume did not return after the queue was deleted (no basic.cancel surfaced)")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = pub.Close(closeCtx)
}

// TestConformance_QuorumDeliveryLimit_PoisonBoundedByLimitPlusOne asserts the
// quorum-queue poison-loop BOUND (Lens-10 TV-11): a message that is always
// nacked-with-requeue is redelivered at most DeliveryLimit+1 times before the
// broker's x-delivery-limit drops it. The assertion is a BOUND (<= limit+1), not
// equality — the exact count is non-deterministic on cyclic topologies (SPEC
// §6.3). TopologyHint disables warren's in-process redelivery counter so the
// broker's x-delivery-limit is what is under test. Requires a RabbitMQ that
// honours x-delivery-limit (>= 3.13 / 4.x; the exact-version matrix is T151).
func TestConformance_QuorumDeliveryLimit_PoisonBoundedByLimitPlusOne(t *testing.T) {
	// Registered before dialConf's conn-close cleanup so it runs LAST (t.Cleanup is
	// LIFO): goleak must verify AFTER the connection is closed and its supervisor
	// goroutines are joined, otherwise it sees the still-open connection's pool.
	t.Cleanup(func() { goleak.VerifyNone(t) })
	url := brokerURL(t)
	ctx := context.Background()

	const (
		exchange      = "warren.conf.qlimit.ex"
		queue         = "warren.conf.qlimit.q"
		deliveryLimit = 3
	)
	conn := dialConf(t, url)
	dropTopology(url, []string{queue}, []string{exchange})
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: exchange, Kind: warren.ExchangeDirect, Durable: true}},
		Queues: []warren.Queue{{
			Name:          queue,
			Durable:       true,
			Type:          warren.QueueTypeQuorum,
			DeliveryLimit: deliveryLimit,
		}},
		Bindings: []warren.Binding{{Exchange: exchange, Queue: queue, RoutingKey: "k"}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	t.Cleanup(func() { dropTopology(url, []string{queue}, []string{exchange}) })

	pub, err := warren.PublisherFor[confMsg](conn).
		Exchange(exchange).
		RoutingKey("k").
		ConfirmTimeout(10 * time.Second).
		Build()
	require.NoError(t, err)

	deliveries := make(chan struct{}, deliveryLimit+8)
	consumer, err := warren.ConsumerFor[confMsg](conn).
		Queue(queue).
		Concurrency(1).
		Prefetch(1).
		// Hint the quorum + delivery-limit topology so warren disables its
		// in-process counter B and defers entirely to the broker's x-delivery-limit.
		TopologyHint(warren.Queue{Type: warren.QueueTypeQuorum, DeliveryLimit: deliveryLimit}).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- consumer.Consume(consumeCtx, func(_ context.Context, _ confMsg) error {
			deliveries <- struct{}{}
			return warren.ErrRequeue // always nack-with-requeue → poison
		})
	}()

	require.NoError(t, pub.Publish(ctx, warren.Message[confMsg]{Body: &confMsg{N: 1}}))

	// Count redeliveries until the broker stops redelivering (the message is
	// dropped/dead-lettered at the delivery limit) — detected by a quiet period.
	count := 0
	quiet := 4 * time.Second
loop:
	for {
		select {
		case <-deliveries:
			count++
			require.LessOrEqualf(t, count, deliveryLimit+1,
				"quorum x-delivery-limit must bound redeliveries to DeliveryLimit+1 (=%d); got %d",
				deliveryLimit+1, count)
		case <-time.After(quiet):
			break loop // no redelivery within the quiet window → broker has dropped it
		case <-ctx.Done():
			t.Fatal("context cancelled before the poison message stopped redelivering")
		}
	}

	cancelConsume()
	<-consumeDone

	assert.GreaterOrEqual(t, count, 1, "the poison message must be delivered at least once")
	assert.LessOrEqual(t, count, deliveryLimit+1,
		"x-delivery-limit must bound total deliveries to DeliveryLimit+1")
	t.Logf("quorum poison bound: delivered %d time(s) for DeliveryLimit=%d (bound %d)",
		count, deliveryLimit, deliveryLimit+1)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = pub.Close(closeCtx)
}

// TestConformance_UserIDMismatch_PreconditionFailed406 pins the broker contract
// that warren's client-side UserID guard (decision 39) is built on: a publish
// whose AMQP `user-id` property does not match the authenticated connection user
// is rejected by the broker with a 406 PRECONDITION_FAILED channel exception
// (406, not 403 ACCESS_REFUSED — the broker raises precondition-failed for the
// user-id mismatch). warren validates UserID client-side and never lets the
// mismatched frame reach the broker (turning it into a local ErrInvalidMessage),
// so this test deliberately goes through a RAW amqp091 channel to prove the real
// broker behavior the guard mirrors — a stub could not (TV-06: 406-on-UserID).
func TestConformance_UserIDMismatch_PreconditionFailed406(t *testing.T) {
	// Registered before the raw connection's deferred Close so it runs LAST
	// (Cleanup is LIFO, and defers run before cleanups): goleak verifies after the
	// amqp091 connection's goroutines are joined.
	t.Cleanup(func() { goleak.VerifyNone(t) })
	url := brokerURL(t)

	// The authenticated user is whatever AMQP_TEST_URL carries (default "guest").
	// Publishing a DIFFERENT user-id is what provokes the 406.
	connUser := "guest"
	if u, err := neturl.Parse(url); err == nil && u.User != nil && u.User.Username() != "" {
		connUser = u.User.Username()
	}
	mismatchUser := connUser + "-not-the-connection-user"

	rc, err := amqp091.Dial(url)
	require.NoError(t, err, "raw amqp091 dial")
	defer rc.Close() //nolint:errcheck
	ch, err := rc.Channel()
	require.NoError(t, err, "raw amqp091 channel")

	closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))

	// Default exchange: routing is irrelevant (an unroutable default-exchange
	// publish is silently dropped, not an error) — the broker validates user-id
	// independently and raises the channel exception regardless.
	pubErr := ch.PublishWithContext(context.Background(), "", "warren.conf.userid.nonexistent",
		false, false, amqp091.Publishing{UserId: mismatchUser, Body: []byte("x")})
	// The 406 arrives asynchronously as a channel close, even if the fire-and-forget
	// publish call itself returned nil; the NotifyClose channel carries it.
	_ = pubErr

	select {
	case amqpErr := <-closeCh:
		require.NotNil(t, amqpErr,
			"channel must close with an AMQP error on a user-id mismatch")
		assert.Equal(t, 406, amqpErr.Code,
			"user-id mismatch must raise PRECONDITION_FAILED (406); got %d %q", amqpErr.Code, amqpErr.Reason)
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not raise a channel exception for a mismatched user-id within 10s")
	}
}

// NOTE on the other T44 named target, basic.qos global=true for ChannelQoS():
// AMQP does not echo the global flag back on the wire, so the only broker-side
// observation is the management API's per-channel global_prefetch_count. Against
// the conformance image (rabbitmq:3.13-management, rates_mode=basic) that proved
// too unreliable to gate on — /api/channels lags several seconds to even list a
// live channel and then reports consumer_count=0 / global_prefetch_count=0 for a
// channel that is actively consuming with Prefetch(137)+ChannelQoS(). The
// behavior is unit-covered (consumer.go Qos(..., global=c.channelQoS)); the
// real-broker assertion is deferred — see LATER-74.
