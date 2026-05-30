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

// TestOTelExample_integration runs the otel example as a subprocess and asserts
// the OBSERVABLE OUTCOME: the publish and consume spans were recorded, share a
// trace, and the consume span is a child of the publish span (printed by the
// example after its in-process assertion passes).
func TestOTelExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	cleanTopology(t, url)
	t.Cleanup(func() { cleanTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()

	output := string(out)
	require.NoError(t, err, "example exited non-zero:\n%s", output)

	assert.Contains(t, output, "publish span: trace=",
		"the example must record and print the publish span")
	assert.Contains(t, output, "consume span: trace=",
		"the example must record and print the consume span")
	assert.Contains(t, output, "with correct parent linkage",
		"the example must confirm publish→consume trace continuity")
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

	_, _ = ch.QueueDelete("warren.examples.otel.events", false, false, false)
	_ = ch.ExchangeDelete("warren.examples.otel", false, false)
}
