//go:build integration

package main_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
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

// TestDeadletterExample_integration runs the deadletter example as a subprocess,
// asserts exit 0, and verifies that the example printed x-death header info.
func TestDeadletterExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Pre-clean: delete queues and exchanges that may linger from a previous run.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	ch, err := rawConn.Channel()
	require.NoError(t, err)
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
	ch.Close()    //nolint:errcheck
	rawConn.Close() //nolint:errcheck

	// Run the example as a subprocess.
	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	output := string(out)
	// (a) exit 0 — already asserted by require.NoError above.
	// (c) The example must have printed x-death info from the DLQ message.
	assert.True(t, strings.Contains(output, "x-death"),
		"expected x-death in output:\n%s", output)
	assert.Contains(t, output, "deadletter example complete")
	assert.Contains(t, output, "dead-001", "expected order ID in output")
}
