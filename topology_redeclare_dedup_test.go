package warren

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingDeclareChannel is a topologyChannel that counts queue/exchange/bind
// declares into a shared atomic counter, for de-amplification assertions (T62).
type countingDeclareChannel struct {
	declares *atomic.Int64
}

func (c countingDeclareChannel) ExchangeDeclare(_, _ string, _, _, _, _ bool, _ amqp091.Table) error {
	c.declares.Add(1)
	return nil
}

func (c countingDeclareChannel) QueueDeclare(_ string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	c.declares.Add(1)
	return amqp091.Queue{}, nil
}

func (c countingDeclareChannel) QueueBind(_, _, _ string, _ bool, _ amqp091.Table) error {
	c.declares.Add(1)
	return nil
}

func (countingDeclareChannel) Close() error { return nil }

// TestTopologyRedeclare_deAmplified_oncePerRecovery proves that a pool-wide
// reconnect wave (every socket's barrier fires the topology hook at once)
// declares the broker-global topology ONCE, not N× (T62 / DS-09 / SRE-06).
func TestTopologyRedeclare_deAmplified_oncePerRecovery(t *testing.T) {
	const poolSize = 8 // 4 publisher + 4 consumer, the verify scenario

	var declares atomic.Int64
	mcs := make([]*managedConn, poolSize)
	for i := range mcs {
		mc := newBareManaged(t)
		mc.chanFactory = func() (topologyChannel, error) {
			return countingDeclareChannel{declares: &declares}, nil
		}
		mcs[i] = mc
	}
	conn := &Connection{pubConns: mcs[:4], conConns: mcs[4:]}

	// A topology with exactly one queue → one declare per full redeclare.
	topo := &Topology{Queues: []Queue{{Name: "orders"}}}
	require.NoError(t, topo.AttachTo(conn))

	// Fire every connection's topology hook concurrently, as a broker restart does.
	var wg sync.WaitGroup
	for _, mc := range mcs {
		wg.Add(1)
		go func(mc *managedConn) {
			defer wg.Done()
			_ = conn.ensureTopologyRedeclare(context.Background(), mc)
		}(mc)
	}
	wg.Wait()

	assert.Equal(t, int64(1), declares.Load(),
		"a pool-wide reconnect wave must declare the topology once (topology size), not %d×", poolSize)
}

// TestTopologyRedeclare_independentReconnectsAfterWindow redeclares again once
// the coalesce window has elapsed (a genuinely new recovery event), so a later
// independent reconnect is not silently skipped.
func TestTopologyRedeclare_independentReconnectsAfterWindow(t *testing.T) {
	var declares atomic.Int64
	mc := newBareManaged(t)
	mc.chanFactory = func() (topologyChannel, error) {
		return countingDeclareChannel{declares: &declares}, nil
	}
	conn := &Connection{pubConns: []*managedConn{mc}}

	topo := &Topology{Queues: []Queue{{Name: "orders"}}}
	require.NoError(t, topo.AttachTo(conn))

	require.NoError(t, conn.ensureTopologyRedeclare(context.Background(), mc))
	require.Equal(t, int64(1), declares.Load())

	// Simulate the coalesce window having elapsed by clearing the last-declare stamp.
	conn.topoRedeclareMu.Lock()
	conn.topoLastDeclare = time.Time{}
	conn.topoRedeclareMu.Unlock()

	require.NoError(t, conn.ensureTopologyRedeclare(context.Background(), mc))
	assert.Equal(t, int64(2), declares.Load(),
		"a reconnect outside the coalesce window must redeclare again")
}
