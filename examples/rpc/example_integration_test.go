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

// TestRPCExample_integration runs the rpc example as a subprocess against the
// standard broker, asserts exit 0, and verifies it exercised both the concurrent
// happy path (replies demultiplexed by CorrelationID) and the ErrCallTimeout path.
func TestRPCExample_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	cleanRPCTopology(t, url)
	t.Cleanup(func() { cleanRPCTopology(t, url) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", ".") //nolint:gosec
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_URL="+url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "example exited non-zero:\n%s", string(out))

	output := string(out)
	assert.Contains(t, output, "all 3 concurrent calls matched",
		"expected the concurrent happy path to complete")
	assert.Contains(t, output, "ErrCallTimeout",
		"expected the slow call to surface ErrCallTimeout")
	assert.Contains(t, output, "rpc example complete")
}

// cleanRPCTopology deletes the queues and exchange created by the rpc example.
func cleanRPCTopology(t *testing.T, url string) {
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
		"warren.examples.rpc.prices",
		"warren.examples.rpc.prices.dlq",
	} {
		_, _ = ch.QueueDelete(name, false, false, false)
	}
	_ = ch.ExchangeDelete("warren.examples.rpc.prices.dlx", false, false)
}
