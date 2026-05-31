//go:build integration

package warren_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// queueArgsViaManagement reads a queue's declared arguments from the RabbitMQ
// management HTTP API. The API base (scheme, host, port, credentials) must be
// supplied explicitly via the AMQP_TEST_MANAGEMENT_URL env var, e.g.
// http://guest:guest@localhost:15672 (or https://…:15671). The test fails — it
// does not skip — when the var is unset, so a misconfigured environment is loud
// rather than silently un-asserted. The vhost is taken from amqpURL.
func queueArgsViaManagement(t *testing.T, amqpURL, queue string) map[string]any {
	t.Helper()

	mgmt := os.Getenv("AMQP_TEST_MANAGEMENT_URL")
	if mgmt == "" {
		t.Fatal("AMQP_TEST_MANAGEMENT_URL must be set to read broker-side queue arguments " +
			"(e.g. http://guest:guest@localhost:15672)")
	}
	base, err := url.Parse(mgmt)
	require.NoError(t, err, "AMQP_TEST_MANAGEMENT_URL must be a valid URL")
	require.NotEmpty(t, base.Host, "AMQP_TEST_MANAGEMENT_URL must include host:port (e.g. http://guest:guest@localhost:15672)")

	// vhost comes from the AMQP connection URL; the default vhost is "/".
	av, err := url.Parse(amqpURL)
	require.NoError(t, err)
	vhost := strings.TrimPrefix(av.Path, "/")
	if vhost == "" {
		vhost = "/"
	}

	apiURL := fmt.Sprintf("%s://%s/api/queues/%s/%s",
		base.Scheme, base.Host, url.PathEscape(vhost), url.PathEscape(queue))

	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	require.NoError(t, err)
	// Credentials come from the management URL userinfo and ride the
	// Authorization header — never the request URL (so they cannot leak via an
	// error or log line).
	if base.User != nil {
		pass, _ := base.User.Password()
		req.SetBasicAuth(base.User.Username(), pass)
	}

	// DisableKeepAlives so no pooled connection goroutine survives into goleak.
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	resp, err := client.Do(req)
	require.NoErrorf(t, err, "management API GET %s://%s failed", base.Scheme, base.Host)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("management API GET %s returned %d: %s", apiURL, resp.StatusCode, string(body))
	}

	var payload struct {
		Arguments map[string]any `json:"arguments"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	return payload.Arguments
}

// deleteDurableQueue removes a durable queue left behind by a test so reruns
// start clean. Best-effort: failures are ignored.
func deleteDurableQueue(url, name string) {
	rawConn, err := amqp091.Dial(url)
	if err != nil {
		return
	}
	defer rawConn.Close() //nolint:errcheck
	ch, err := rawConn.Channel()
	if err != nil {
		return
	}
	defer ch.Close() //nolint:errcheck
	_, _ = ch.QueueDelete(name, false, false, false)
}

func TestTopology_Declare_happyPath_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: "test.events", Kind: warren.ExchangeTopic, Durable: false, AutoDelete: true},
		},
		Queues: []warren.Queue{
			{Name: "test.orders", Durable: false, AutoDelete: true},
		},
		Bindings: []warren.Binding{
			{Exchange: "test.events", Queue: "test.orders", RoutingKey: "order.#"},
		},
	}

	err = topo.Declare(ctx, conn)
	require.NoError(t, err)
}

func TestTopology_Declare_idempotent_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{
			{Name: "test.idempotent", Durable: false, AutoDelete: true},
		},
	}

	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.Declare(ctx, conn), "second Declare must be idempotent")
}

func TestTopology_Declare_mismatch_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Declare a durable queue first.
	topo1 := &warren.Topology{
		Queues: []warren.Queue{
			{Name: "test.mismatch", Durable: true},
		},
	}
	require.NoError(t, topo1.Declare(ctx, conn))

	conn2, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn2.Close(closeCtx)
	}()

	// Try to redeclare with a conflicting non-durable flag.
	topo2 := &warren.Topology{
		Queues: []warren.Queue{
			{Name: "test.mismatch", Durable: false},
		},
	}
	err = topo2.Declare(ctx, conn2)
	require.Error(t, err)
	assert.ErrorIs(t, err, warren.ErrTopologyMismatch, "must be ErrTopologyMismatch")
	assert.ErrorIs(t, err, warren.ErrPreconditionFailed, "must also unwrap ErrPreconditionFailed")
}

// TestTopology_Declare_noWaitDowngradesMismatchToAsync_integration is the T15
// regression for the NoWait caveat: a conflicting redeclare normally surfaces
// ErrTopologyMismatch, but with NoWait=true mismatch detection is downgraded to
// asynchronous so Declare returns nil.
func TestTopology_Declare_noWaitDowngradesMismatchToAsync_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const qname = "test.nowait.async"
	t.Cleanup(func() { deleteDurableQueue(url, qname) })

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Declare a durable queue synchronously.
	base := &warren.Topology{
		Queues: []warren.Queue{{Name: qname, Durable: true}},
	}
	require.NoError(t, base.Declare(ctx, conn))

	// A conflicting redeclare WITHOUT NoWait surfaces ErrTopologyMismatch.
	conflict := &warren.Topology{
		Queues: []warren.Queue{{Name: qname, Durable: false}},
	}
	err = conflict.Declare(ctx, conn)
	require.Error(t, err)
	assert.ErrorIs(t, err, warren.ErrTopologyMismatch)
	assert.ErrorIs(t, err, warren.ErrPreconditionFailed)

	// The same conflict WITH NoWait=true is downgraded to async: Declare returns nil.
	conflictNoWait := &warren.Topology{
		Queues: []warren.Queue{{Name: qname, Durable: false, NoWait: true}},
	}
	assert.NoError(t, conflictNoWait.Declare(ctx, conn),
		"NoWait=true must downgrade mismatch detection to async (Declare returns nil)")
}

func TestTopology_Declare_dlxExpansion_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{
			{Name: "test.dlx.source", Durable: false, AutoDelete: true},
		},
		DeadLetters: []warren.DeadLetter{
			{Source: "test.dlx.source", Exchange: "test.dlx.exchange"},
		},
	}

	// DLX expansion must succeed: source queue gets x-dead-letter-exchange injected
	// and the DLX exchange + DLQ queue are created automatically.
	err = topo.Declare(ctx, conn)
	require.NoError(t, err)
}

// TestTopology_Declare_dlxRoutesToDLQ_integration is the T47 end-to-end proof
// that the auto-expanded DeadLetter topology no longer routes dead-lettered
// messages into limbo: before T47 the expansion created the DLX exchange and
// the <source>.dlq queue but never bound them, so a nacked message vanished.
// Here we publish to the source queue (via the default exchange), nack it
// without requeue, and assert it actually arrives in the auto-created DLQ.
func TestTopology_Declare_dlxRoutesToDLQ_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const (
		srcQ = "test.dlx.route.src"
		dlqQ = "test.dlx.route.src.dlq"
	)
	purgeQueues(t, url, srcQ, dlqQ)
	t.Cleanup(func() {
		deleteQueues(url, srcQ, dlqQ)
		deleteExchanges(url, "test.dlx.route.src.dlx")
	})

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// No manual DLX binding: rely entirely on DeadLetter auto-expansion.
	topo := &warren.Topology{
		Queues: []warren.Queue{
			{Name: srcQ, Durable: false},
		},
		DeadLetters: []warren.DeadLetter{
			{Source: srcQ},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	// Publish into the source queue via the default exchange (routing key == queue name).
	pub, err := warren.PublisherFor[string](conn).RoutingKey(srcQ).Build()
	require.NoError(t, err)
	body := "dead-letter-me"
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &body}))

	// Source consumer: nack-without-requeue so the broker dead-letters the message.
	srcConsumer, err := warren.ConsumerFor[string](conn).Queue(srcQ).Prefetch(1).Build()
	require.NoError(t, err)
	srcCtx, srcCancel := context.WithCancel(ctx)
	srcDone := make(chan struct{})
	go func() {
		defer close(srcDone)
		_ = srcConsumer.Consume(srcCtx, func(_ context.Context, _ string) error {
			defer srcCancel()
			return warren.ErrPoison
		})
	}()
	select {
	case <-srcDone:
	case <-time.After(5 * time.Second):
		t.Fatal("source consumer did not process the message")
	}

	// DLQ consumer: the dead-lettered message must land here thanks to the
	// auto-created DLX->DLQ binding (the T47 fix).
	dlqConsumer, err := warren.ConsumerFor[string](conn).Queue(dlqQ).Prefetch(1).Build()
	require.NoError(t, err)
	dlqCtx, dlqCancel := context.WithCancel(ctx)
	dlqDone := make(chan struct{})
	var got string
	go func() {
		defer close(dlqDone)
		_ = dlqConsumer.Consume(dlqCtx, func(_ context.Context, m string) error {
			got = m
			dlqCancel()
			return nil
		})
	}()
	select {
	case <-dlqDone:
	case <-time.After(5 * time.Second):
		t.Fatal("dead-lettered message did not arrive in the auto-created DLQ (routing limbo)")
	}
	assert.Equal(t, body, got, "DLQ must receive the original dead-lettered payload")
}

func TestTopology_Declare_quorumWithDeliveryLimit_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{
			{
				Name:          "test.quorum.dl",
				Durable:       true,
				Type:          warren.QueueTypeQuorum,
				DeliveryLimit: 5,
			},
		},
	}

	err = topo.Declare(ctx, conn)
	require.NoError(t, err)
}

// TestTopology_Declare_quorumDLXStrategy_integration is the T15 broker-side
// assertion (LATER-54): a quorum source queue with a DeadLetter entry must carry
// x-dead-letter-strategy=at-least-once (SPEC §10 decision 52) plus the DLX args
// and x-delivery-limit, verified by reading them back from the management API
// rather than only trusting the in-memory expansion.
func TestTopology_Declare_quorumDLXStrategy_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	brokerURL := amqpTestURL(t)
	ctx := context.Background()

	const qname = "test.quorum.dlx.strategy"
	t.Cleanup(func() { deleteDurableQueue(brokerURL, qname) })
	t.Cleanup(func() { deleteDurableQueue(brokerURL, qname+".dlq") })

	conn, err := warren.Dial(ctx, warren.WithAddr(brokerURL))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{
			{Name: qname, Durable: true, Type: warren.QueueTypeQuorum, DeliveryLimit: 3},
		},
		DeadLetters: []warren.DeadLetter{
			{Source: qname, Exchange: qname + ".dlx"},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	args := queueArgsViaManagement(t, brokerURL, qname)
	assert.Equal(t, "at-least-once", args["x-dead-letter-strategy"],
		"quorum + DLX must declare x-dead-letter-strategy=at-least-once (SPEC §10 decision 52)")
	assert.Equal(t, qname+".dlx", args["x-dead-letter-exchange"])
	assert.Equal(t, "quorum", args["x-queue-type"])
	assert.EqualValues(t, 3, args["x-delivery-limit"]) // JSON number decodes as float64
}
