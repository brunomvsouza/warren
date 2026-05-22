//go:build integration

package main_test

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// amqpTestURL returns the broker URL for integration tests.
func amqpTestURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("AMQP_TEST_URL")
	if u == "" {
		t.Skip("AMQP_TEST_URL not set — skipping integration test")
	}
	return u
}

// TestTopologyExample_integration runs the topology example as a subprocess and
// asserts that the declared queue is visible on the broker after the example exits.
func TestTopologyExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)

	// Pre-clean any leftover state from previous runs so the example's
	// idempotent declare path is also exercised from a fresh slate.
	cleanTopology(t, url)
	// Guaranteed post-cleanup regardless of test outcome.
	t.Cleanup(func() { cleanTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Run the example as a subprocess.
	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	output := string(out)
	assert.Contains(t, output, "topology declared", "expected first declare log line")
	assert.Contains(t, output, "idempotent", "expected idempotent log line")
	assert.Contains(t, output, "topology re-declared (after reconnect)", "expected reconnect redeclare log line")

	// Verify that the declared queue is visible on the broker. The queues are
	// purely durable (no AutoDelete), so they persist after the example closes
	// its connection.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck

	ch, err := rawConn.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck

	// Passive-declare verifies the queue exists without mutating broker state.
	q, err := ch.QueueDeclarePassive(
		"warren.examples.orders",
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	require.NoError(t, err, "queue warren.examples.orders should exist after example exits")
	assert.Equal(t, "warren.examples.orders", q.Name)
}

// cleanTopology deletes the queues and exchanges created by the topology example.
func cleanTopology(t *testing.T, url string) {
	t.Helper()
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

	for _, name := range []string{"warren.examples.orders", "warren.examples.payments", "warren.examples.alerts"} {
		_, _ = ch.QueueDelete(name, false, false, false)
	}
	for _, name := range []string{"warren.examples.events", "warren.examples.notify"} {
		_ = ch.ExchangeDelete(name, false, false)
	}
}
