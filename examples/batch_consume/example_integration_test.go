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

// TestBatchConsumeExample_integration runs the batch_consume example as a
// subprocess and asserts:
//
//   (a) The example exits 0.
//   (b) The output contains "flush-by-size" (size-based flush triggered).
//   (c) The output contains "flush-by-timer" (timer-based flush triggered).
//   (d) The completion message is present.
//   (e) No goroutine leaks in the test process.
func TestBatchConsumeExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)

	cleanBatchConsumeTopology(t, url)
	t.Cleanup(func() { cleanBatchConsumeTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()

	// (a) Assert exit 0.
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	output := string(out)

	// (b) Size-based flush must have fired.
	assert.Contains(t, output, "flush-by-size",
		"expected flush-by-size in output; got:\n%s", output)

	// (c) Timer-based flush must have fired.
	assert.Contains(t, output, "flush-by-timer",
		"expected flush-by-timer in output; got:\n%s", output)

	// (d) Completion message.
	assert.Contains(t, output, "batch_consume example complete",
		"expected completion message in output; got:\n%s", output)
}

// cleanBatchConsumeTopology deletes the resources created by the batch_consume example.
func cleanBatchConsumeTopology(t *testing.T, url string) {
	t.Helper()
	rc, err := amqp091.Dial(url)
	if err != nil {
		return
	}
	defer rc.Close() //nolint:errcheck

	ch, err := rc.Channel()
	if err != nil {
		return
	}
	defer ch.Close() //nolint:errcheck

	_, _ = ch.QueueDelete("warren.examples.batchconsume.orders", false, false, false)
	_ = ch.ExchangeDelete("warren.examples.batchconsume", false, false)
}
