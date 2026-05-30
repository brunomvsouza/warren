//go:build integration

package warren_test

// Reconnect chaos test (T45 / SPEC §1, §9 + Lens-08 CR-10, Lens-10 TV-09/10).
//
// A reconnect storm driven by (*Connection).ForceReconnect — the deterministic
// outage injector the rest of the suite standardizes on — runs continuously
// while a multi-connection publisher streams confirmed messages and a consumer
// drains them. The contract under test is the §1 headline: at-least-once with
// ZERO LOSS. Loss is measured exactly as TV-09 mandates — the published-set
// minus the consumed-set, deduplicated by MessageID — so the duplicates the
// reconnect barrier and PublishRetry produce by design are tolerated, while a
// genuinely dropped message is caught. The loss accounting itself is unit-tested
// with a VG-6 injected-drop self-test in chaos_reconnect_loss_test.go, so a green
// run means "no loss", never "the harness cannot see loss".
//
// Intensity scaling: SPEC §9 nominates a 5-minute outage at 10k msg/s and a
// 1000-cycle churn sub-test. Those are the nightly / release-candidate numbers;
// the CI lane defaults to a short duration and a smaller cycle count (the loss
// and goleak invariants are exercised at any intensity) and is dialed up to the
// §9 figures via WARREN_CHAOS_DURATION / WARREN_CHAOS_CHURN_CYCLES. The sustained
// 1h campaign at the §9 target is a separate task (T167).

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

const (
	chaosExchange = "warren.chaos.ex"
	chaosQueue    = "warren.chaos.q"
	chaosRouting  = "chaos.created"
)

type chaosMsg struct {
	Seq int `json:"seq"`
}

// chaosRetry retries a publish that hits the reconnect barrier so a confirmed
// publish is durable despite the storm; jitter off for deterministic timing.
var chaosRetry = warren.RetryPolicy{
	Min:           10 * time.Millisecond,
	Max:           200 * time.Millisecond,
	Factor:        2.0,
	Retries:       6,
	WithoutJitter: true,
}

func chaosDuration(t *testing.T) time.Duration {
	t.Helper()
	if v := os.Getenv("WARREN_CHAOS_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		require.NoErrorf(t, err, "WARREN_CHAOS_DURATION=%q must be a Go duration", v)
		return d
	}
	return 5 * time.Second
}

func churnCycles(t *testing.T) int {
	t.Helper()
	if v := os.Getenv("WARREN_CHAOS_CHURN_CYCLES"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoErrorf(t, err, "WARREN_CHAOS_CHURN_CYCLES=%q must be an integer", v)
		require.Positive(t, n)
		return n
	}
	return 100
}

// TestChaosReconnect_ZeroLoss_integration streams confirmed messages through a
// continuous ForceReconnect storm and asserts every confirmed publish is
// eventually consumed (deduplicated by MessageID).
func TestChaosReconnect_ZeroLoss_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := warren.Dial(ctx,
		warren.WithAddr(url),
		warren.WithPublisherConnections(2),
		warren.WithConsumerConnections(2),
		warren.WithChannelPoolSize(8),
		warren.WithReconnectBackoff(fastFailoverBackoff),
	)
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = conn.Close(closeCtx)
	}()

	// Durable, non-auto-delete topology: an auto-delete queue would be torn down
	// by the broker during the window where the consumer is disconnected mid-storm,
	// destroying queued messages and corrupting the loss measurement.
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: chaosExchange, Kind: warren.ExchangeTopic, Durable: true}},
		Queues:    []warren.Queue{{Name: chaosQueue, Durable: true}},
		Bindings:  []warren.Binding{{Exchange: chaosExchange, Queue: chaosQueue, RoutingKey: "chaos.#"}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn)) // redeclare on every reconnect barrier
	t.Cleanup(func() {
		deleteQueues(url, chaosQueue)
		deleteExchanges(url, chaosExchange)
	})

	var (
		mu           sync.Mutex
		publishedSet = make(map[string]struct{})
		consumedSet  = make(map[string]struct{})
	)

	// — Consumer ——————————————————————————————————————————————————————————————
	consumer, err := warren.ConsumerFor[chaosMsg](conn).
		Queue(chaosQueue).
		Concurrency(4).
		Prefetch(64).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- consumer.ConsumeRaw(consumeCtx, func(_ context.Context, d *warren.Delivery[chaosMsg]) error {
			mu.Lock()
			consumedSet[d.MessageID()] = struct{}{}
			mu.Unlock()
			return d.Ack()
		})
	}()

	// — Outage injector: a continuous ForceReconnect storm ————————————————————
	outageDone := make(chan struct{})
	var outageWG sync.WaitGroup
	outageWG.Add(1)
	go func() {
		defer outageWG.Done()
		tk := time.NewTicker(150 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-outageDone:
				return
			case <-tk.C:
				_ = conn.ForceReconnect() // ignore ErrAlreadyClosed at teardown
			}
		}
	}()

	// — Publisher: stream confirmed messages for the configured duration ————————
	pub, err := warren.PublisherFor[chaosMsg](conn).
		Exchange(chaosExchange).
		RoutingKey(chaosRouting).
		ConfirmTimeout(10 * time.Second).
		PublishRetry(chaosRetry).
		Build()
	require.NoError(t, err)

	start := time.Now()
	dur := chaosDuration(t)
	seq := 0
	for time.Since(start) < dur {
		id := fmt.Sprintf("chaos-%d", seq)
		// Pin MessageID so the published set is known exactly; on a confirmed
		// (nil) return the broker durably holds it and it MUST be consumed.
		if perr := pub.Publish(ctx, warren.Message[chaosMsg]{Body: &chaosMsg{Seq: seq}, MessageID: id}); perr == nil {
			mu.Lock()
			publishedSet[id] = struct{}{}
			mu.Unlock()
		}
		seq++
	}

	// Stop the storm and let the connection settle so the consumer can drain.
	close(outageDone)
	outageWG.Wait()

	{
		closeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		_ = pub.Close(closeCtx)
		c()
	}

	require.Eventually(t, func() bool { return conn.Health(ctx) == nil },
		30*time.Second, 100*time.Millisecond, "connection must recover to healthy after the storm")

	// Drain: every confirmed publish must eventually be consumed.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(publishedSet) > 0 && len(lossByMessageID(publishedSet, consumedSet)) == 0
	}, 90*time.Second, 250*time.Millisecond, "all confirmed publishes must be consumed (zero loss)")

	cancelConsume()
	require.NoError(t, filterCanceled(<-consumeErr), "consumer must stop cleanly")

	mu.Lock()
	lost := lossByMessageID(publishedSet, consumedSet)
	nPub, nCon := len(publishedSet), len(consumedSet)
	mu.Unlock()

	require.Positive(t, nPub, "the workload must confirm at least one publish")
	require.Empty(t, lost, "zero message loss: %d confirmed, %d consumed-distinct, lost=%v", nPub, nCon, lost)
	t.Logf("chaos zero-loss: confirmed=%d consumed-distinct=%d (duplicates tolerated) over %s storm",
		nPub, nCon, dur)
}

// TestChaosReconnect_ChurnGoleak_integration hammers connect/disconnect with a
// confirmed publish per cycle and asserts no goroutine leaks (Lens-08 CR-10).
func TestChaosReconnect_ChurnGoleak_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := warren.Dial(ctx,
		warren.WithAddr(url),
		warren.WithPublisherConnections(1),
		warren.WithConsumerConnections(1),
		warren.WithReconnectBackoff(fastFailoverBackoff),
	)
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: chaosExchange, Kind: warren.ExchangeTopic, Durable: true}},
		Queues:    []warren.Queue{{Name: chaosQueue, Durable: true}},
		Bindings:  []warren.Binding{{Exchange: chaosExchange, Queue: chaosQueue, RoutingKey: "chaos.#"}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn))
	t.Cleanup(func() {
		deleteQueues(url, chaosQueue)
		deleteExchanges(url, chaosExchange)
	})

	pub, err := warren.PublisherFor[chaosMsg](conn).
		Exchange(chaosExchange).
		RoutingKey(chaosRouting).
		ConfirmTimeout(10 * time.Second).
		PublishRetry(chaosRetry).
		Build()
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = pub.Close(closeCtx)
	}()

	cycles := churnCycles(t)
	for i := 0; i < cycles; i++ {
		require.NoErrorf(t, conn.ForceReconnect(), "ForceReconnect at cycle %d", i)
		require.Eventuallyf(t, func() bool { return conn.Health(ctx) == nil },
			10*time.Second, 10*time.Millisecond, "recover after ForceReconnect at cycle %d", i)
		require.NoErrorf(t, pub.Publish(ctx, warren.Message[chaosMsg]{Body: &chaosMsg{Seq: i}}),
			"confirmed publish at cycle %d", i)
	}
	t.Logf("chaos churn: %d connect/disconnect + confirm cycles, goleak-clean", cycles)
}
