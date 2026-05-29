//go:build integration

package main_test

import (
	"context"
	"encoding/json"
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

// TestBatchPublishExample_integration runs the batch_publish example as a
// subprocess and asserts:
//
//   (a) The example exits 0.
//   (b) 1000 messages actually reached the broker queue.
//   (c) The ErrBatchTooLarge guard message appears in the output.
//   (d) No goroutine leaks in the test process.
func TestBatchPublishExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)

	cleanBatchPublishTopology(t, url)
	t.Cleanup(func() { cleanBatchPublishTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()

	// (a) Assert exit 0.
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	output := string(out)

	// (c) ErrBatchTooLarge guard must have fired.
	assert.Contains(t, output, "ErrBatchTooLarge guard OK",
		"expected ErrBatchTooLarge guard message in output")

	// (b) Verify 1000 messages reached the broker.
	count := drainBatchPublishQueue(t, url, "warren.examples.batch.orders", 1000)
	assert.Equal(t, 1000, count,
		"expected 1000 messages in the batch queue, got %d", count)

	assert.Contains(t, output, "batch_publish example complete",
		"expected completion message in output")
}

// drainBatchPublishQueue polls the queue with basic.get until count reaches
// want or the deadline (30s) passes. Returns the actual count received.
func drainBatchPublishQueue(t *testing.T, url, queue string, want int) int {
	t.Helper()

	rc, err := amqp091.Dial(url)
	require.NoError(t, err, "dial for queue drain")
	defer rc.Close() //nolint:errcheck

	ch, err := rc.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck

	// Pre-declare the queue in case the auto-delete triggered before we connect.
	_, err = ch.QueueDeclare(queue, false, true, false, false, nil)
	if err != nil {
		t.Logf("drainBatchPublishQueue: queue may have auto-deleted: %v", err)
		return 0
	}

	count := 0
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		d, ok, err := ch.Get(queue, true /* autoAck */)
		if err != nil {
			t.Logf("drainBatchPublishQueue: get error: %v", err)
			break
		}
		if !ok {
			if count >= want {
				break
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// Decode to verify it's a valid Order payload.
		var o struct {
			ID     string `json:"id"`
			Amount int    `json:"amount"`
		}
		if err := json.Unmarshal(d.Body, &o); err != nil {
			t.Logf("drainBatchPublishQueue: decode error at msg %d: %v", count, err)
		}
		count++
	}
	return count
}

// cleanBatchPublishTopology deletes the resources created by the batch_publish example.
func cleanBatchPublishTopology(t *testing.T, url string) {
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

	_, _ = ch.QueueDelete("warren.examples.batch.orders", false, false, false)
	_ = ch.ExchangeDelete("warren.examples.batch", false, false)
}
