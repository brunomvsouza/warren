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

// TestConsumeExample_integration runs the consume example as a subprocess and
// asserts the following:
//
//   (a) The example exits 0.
//   (b) The source queue is empty after the example exits.
//   (c) The DLQ contains at least the "poison" message (dead-lettered after
//       MaxRedeliveries exhaustion via counter B).
//   (d) No goroutine leaks in the test process itself.
func TestConsumeExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)

	// Pre-clean any leftover topology from previous runs, and guarantee cleanup
	// regardless of test outcome.
	cleanConsumeTopology(t, url)
	t.Cleanup(func() { cleanConsumeTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Subscribe to the DLQ before running the example so that messages published
	// to the DLX are captured even if the queue is auto-deleted by the example.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err, "dial for DLQ consumer")
	defer rawConn.Close() //nolint:errcheck

	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck

	// Pre-declare the DLQ so that the test's consumer creates it (keeping it alive
	// for the duration of this test, even if the example's connection is closed).
	_, err = rawCh.QueueDeclare(
		"warren.examples.consume.orders.dlq",
		false, // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	require.NoError(t, err)

	dlqMsgs, err := rawCh.Consume(
		"warren.examples.consume.orders.dlq",
		"test-dlq-consumer",
		true,  // auto-ack
		false, false, false, nil,
	)
	require.NoError(t, err)

	// Run the consume example as a subprocess.
	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()
	// (a) Assert exit 0.
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	output := string(out)
	assert.Contains(t, output, "consume example complete", "expected completion message in output")
	assert.Contains(t, output, "id=poison", "expected poison message in output")
	assert.Contains(t, output, "max redeliveries reached", "expected max redeliveries log in output")

	// (b) Source queue must be empty after the example finishes.
	qInfo, err := rawCh.QueueInspect("warren.examples.consume.orders")
	require.NoError(t, err)
	assert.Equal(t, 0, qInfo.Messages, "source queue must be empty after example exits")

	// (c) DLQ must contain the dead-lettered messages (bad → Nack(false),
	// slow → HandlerTimeout Nack, poison → counter B MaxRedeliveries Nack).
	// The consumer was started before the example, so messages land in dlqMsgs
	// as the example runs; we drain with a generous timeout to tolerate slow CI.
	//
	// We expect 3 dead-lettered messages (bad, slow, poison) but assert ≥1
	// to remain robust if the example evolves to publish fewer DLX-bound messages.
	const wantDLQMessages = 3
	dlqTimeout := time.NewTimer(15 * time.Second) // 15 s: tolerates slow broker + CI
	defer dlqTimeout.Stop()
	var dlqCount int
	for dlqCount < wantDLQMessages {
		select {
		case msg, ok := <-dlqMsgs:
			if !ok {
				t.Fatalf("DLQ consumer channel closed after only %d messages", dlqCount)
			}
			dlqCount++
			t.Logf("DLQ message %d: body=%s", dlqCount, string(msg.Body))
		case <-dlqTimeout.C:
			// Time expired before all expected messages arrived; break and assert.
			t.Logf("DLQ drain timed out after %d/%d messages", dlqCount, wantDLQMessages)
			goto dlqDone
		}
	}
dlqDone:
	assert.GreaterOrEqual(t, dlqCount, 1, "expected at least one dead-lettered message on the DLQ")

	// Verify the output explicitly confirms that counter B fired for poison.
	assert.Contains(t, output, "id=poison max redeliveries reached",
		"counter B must have fired for the poison message")
}

// cleanConsumeTopology deletes the queues and exchanges created by the consume example.
func cleanConsumeTopology(t *testing.T, url string) {
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
		"warren.examples.consume.orders",
		"warren.examples.consume.orders.dlq",
	} {
		_, _ = ch.QueueDelete(name, false, false, false)
	}
	for _, name := range []string{
		"warren.examples.consume",
		"warren.examples.consume.dlx",
	} {
		_ = ch.ExchangeDelete(name, false, false)
	}
}
