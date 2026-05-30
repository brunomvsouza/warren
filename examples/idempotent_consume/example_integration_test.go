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

// TestIdempotentConsumeExample_integration runs the idempotent_consume example
// as a subprocess and asserts the OBSERVABLE OUTCOME (not just exit-0):
//
//	(a) The example exits 0 — its internal assertion that every distinct
//	    MessageID was handled exactly once held.
//	(b) The dedupe cache observed the duplicate (a "dedupe: ... already
//	    processed" line was logged), proving the second copy reached the
//	    consumer and was suppressed by MessageID, not by chance.
//	(c) The completion line confirms the duplicated MessageID was handled
//	    exactly once.
//	(d) No goroutine leaks in the test process itself.
func TestIdempotentConsumeExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	cleanTopology(t, url)
	t.Cleanup(func() { cleanTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()

	output := string(out)
	require.NoError(t, err, "example exited non-zero:\n%s", output)

	assert.Contains(t, output, "already processed → ack & skip handler",
		"the duplicate MessageID must be observed and suppressed by the dedupe cache")
	assert.Contains(t, output, "handled exactly once",
		"the example must confirm exactly-once handler invocation for the duplicate")
}

// cleanTopology removes the example's queue and exchange. They are auto-delete,
// so this is only defensive against a prior crashed run.
func cleanTopology(t *testing.T, url string) {
	t.Helper()
	conn, err := amqp091.Dial(url)
	if err != nil {
		return
	}
	defer conn.Close() //nolint:errcheck

	ch, err := conn.Channel()
	if err != nil {
		return
	}
	defer ch.Close() //nolint:errcheck

	_, _ = ch.QueueDelete("warren.examples.idem.orders", false, false, false)
	_ = ch.ExchangeDelete("warren.examples.idem", false, false)
}
