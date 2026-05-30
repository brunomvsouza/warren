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

// TestOrderedConsumeExample_integration runs the ordered_consume example as a
// subprocess and asserts the OBSERVABLE OUTCOME:
//
//	(a) The example exits 0 — its internal assertion that the deduped accepted
//	    stream is exactly 0..N-1 (publish order == handler order) held.
//	(b) The active consumer was actually killed and the broker promoted the
//	    standby (the "killing active consumer" line appears), so the in-order
//	    completion was achieved ACROSS a real failover, not by a single consumer.
//	(c) The completion line confirms order was preserved across the failover.
//	(d) No goroutine leaks in the test process itself.
func TestOrderedConsumeExample_integration(t *testing.T) {
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

	assert.Contains(t, output, "killing active consumer",
		"the example must kill the active consumer to exercise the standby promotion")
	assert.Contains(t, output, "handled in order across the failover",
		"the standby must continue the sequence in order after promotion")
	assert.Contains(t, output, "publish order == handler order across failover",
		"the example must confirm the ordering invariant held end to end")
}

// cleanTopology removes the example's queue and exchange (both auto-delete; this
// is defensive against a prior crashed run).
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

	_, _ = ch.QueueDelete("warren.examples.ordered.events", false, false, false)
	_ = ch.ExchangeDelete("warren.examples.ordered", false, false)
}
