//go:build integration

package main_test

import (
	"context"
	"fmt"
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

// requireDelayedExchange fails the test loudly when the broker lacks the
// rabbitmq_delayed_message_exchange plugin. It probes by declaring a throwaway
// x-delayed-message exchange: a plugin-less broker answers with a command-invalid
// channel error. The integration lane provisions the plugin itself — the broker is
// built from Dockerfile.rabbitmq-delayed and started by `make integration-up` — so a
// broker without it is a misconfiguration, not a reason to silently skip. Fail fast
// with the reason, mirroring how a missing AMQP_TEST_URL fails rather than skips.
// Once amqptest/ (T37) lands this migrates to amqptest.RequireDelayedExchange(t).
func requireDelayedExchange(t *testing.T, url string) {
	t.Helper()
	rc, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rc.Close() //nolint:errcheck
	ch, err := rc.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck

	probe := fmt.Sprintf("warren.examples.delayprobe.%d", time.Now().UnixNano())
	derr := ch.ExchangeDeclare(probe, "x-delayed-message", false, true, false, false,
		amqp091.Table{"x-delayed-type": "topic"})
	if derr != nil {
		t.Fatalf("rabbitmq_delayed_message_exchange plugin unavailable at %s (%v); "+
			"the integration lane requires it — start the broker with `make integration-up` "+
			"(built from Dockerfile.rabbitmq-delayed, which bakes the plugin in) or enable "+
			"the plugin on your own broker", url, derr)
	}
	_ = ch.ExchangeDelete(probe, false, false)
}

// TestDelayedExample_integration runs the delayed example as a subprocess and
// asserts exit 0 plus the arrival log. It fails fast on a plugin-less broker.
func TestDelayedExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	requireDelayedExchange(t, url)
	cleanDelayedTopology(t, url)
	t.Cleanup(func() { cleanDelayedTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	output := string(out)
	assert.Contains(t, output, "event arrived after",
		"expected the consumer to log the delayed arrival")
	assert.Contains(t, output, "delayed example complete")
}

// cleanDelayedTopology deletes the queue and exchange created by the delayed example.
func cleanDelayedTopology(t *testing.T, url string) {
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

	_, _ = ch.QueueDelete("warren.examples.delayed.events", false, false, false)
	_ = ch.ExchangeDelete("warren.examples.delayed", false, false)
}
