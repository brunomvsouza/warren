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
		t.Fatal("AMQP_TEST_URL must be set to run integration tests")
	}
	return u
}

// TestDeadletterExample_integration runs the deadletter example as a subprocess,
// asserts exit 0, and verifies that the example printed x-death header info.
func TestDeadletterExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)

	// Pre-clean any leftover state from previous runs.
	cleanDeadletterTopology(t, url)
	// Guaranteed post-cleanup regardless of test outcome.
	t.Cleanup(func() { cleanDeadletterTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Run the example as a subprocess.
	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	output := string(out)
	// (a) exit 0 — already asserted by require.NoError above.
	// (c) The example must have printed x-death info from the DLQ message.
	assert.Contains(t, output, "x-death", "expected x-death header in output")
	assert.Contains(t, output, "deadletter example complete")
	assert.Contains(t, output, "dead-001", "expected order ID in output")
}

// cleanDeadletterTopology deletes the queues and exchanges created by the
// deadletter example.
func cleanDeadletterTopology(t *testing.T, url string) {
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

	for _, name := range []string{
		"warren.examples.dl.orders",
		"warren.examples.dl.orders.dlq",
	} {
		_, _ = ch.QueueDelete(name, false, false, false)
	}
	for _, name := range []string{
		"warren.examples.dl.topic",
		"warren.examples.dl.orders.dlx",
	} {
		_ = ch.ExchangeDelete(name, false, false)
	}
}
