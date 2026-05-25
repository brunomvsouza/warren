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
		t.Skip("AMQP_TEST_URL not set — skipping integration test")
	}
	return u
}

// TestPublishExample_integration runs the publish example as a subprocess and
// asserts that the expected message arrives on the example's queue.
func TestPublishExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Subscribe to the example's queue before running the example so we don't
	// miss the message.
	raw, err := amqp091.Dial(url)
	require.NoError(t, err, "dial for consumer")
	defer raw.Close()

	ch, err := raw.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// The example declares the queue as auto-delete, so we just need to bind a
	// consumer to it before the example runs.
	_, err = ch.QueueDeclare("warren.examples.pub.orders", false, true, false, false, nil)
	require.NoError(t, err)

	msgs, err := ch.Consume("warren.examples.pub.orders", "test-consumer", true, false, false, false, nil)
	require.NoError(t, err)

	// Run the example as a subprocess.
	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	// Assert the message arrived.
	select {
	case msg, ok := <-msgs:
		require.True(t, ok, "consumer channel closed before message arrived")
		assert.Equal(t, "application/json", msg.ContentType)
		var payload map[string]any
		require.NoError(t, json.Unmarshal(msg.Body, &payload))
		assert.Equal(t, "ord-001", payload["id"])
		assert.Equal(t, float64(42), payload["amount"])
	case <-ctx.Done():
		t.Fatal("timed out waiting for message from example")
	}
}
