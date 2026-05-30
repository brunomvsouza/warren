//go:build integration

package amqptest_test

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/internal/amqptest"
)

const delayedExchange = "warren-amqptest-delayed"

// TestNewRabbitMQ_DelayedExchange_integration spins the default (plain-AMQP)
// broker and verifies the delayed-message plugin with a real x-delayed-message
// exchange that actually holds a message for its x-delay. Run it with
// AMQPTEST_IMAGE pointing at a plugin-enabled broker, e.g.
//
//	AMQPTEST_IMAGE=warren-rabbitmq-delayed:3.13 TESTCONTAINERS_RYUK_DISABLED=true \
//	  go test -race -tags=integration ./amqptest/...
func TestNewRabbitMQ_DelayedExchange_integration(t *testing.T) {
	amqptest.RequireDelayedExchange(t)

	rmq := amqptest.NewRabbitMQ(t)
	require.NotEmpty(t, rmq.URI())

	conn, err := amqp091.Dial(rmq.URI())
	require.NoError(t, err, "dial plain AMQP")
	defer conn.Close() //nolint:errcheck // best-effort cleanup
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck // best-effort cleanup

	// Declaring an x-delayed-message exchange proves the plugin is enabled.
	require.NoError(t, ch.ExchangeDeclare(
		delayedExchange, "x-delayed-message", false, true, false, false,
		amqp091.Table{"x-delayed-type": "direct"},
	), "x-delayed-message exchange declare proves the delayed plugin is enabled")

	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(q.Name, "k", delayedExchange, false, nil))

	const delay = 300 * time.Millisecond
	start := time.Now()
	require.NoError(t, ch.Publish(delayedExchange, "k", false, false, amqp091.Publishing{
		Headers: amqp091.Table{"x-delay": int32(delay.Milliseconds())},
		Body:    []byte("delayed-hi"),
	}))

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case m := <-msgs:
		assert.Equal(t, "delayed-hi", string(m.Body))
		assert.GreaterOrEqual(t, time.Since(start), delay, "broker must hold the message for x-delay")
	case <-time.After(5 * time.Second):
		t.Fatal("delayed message was never delivered")
	}
}

// TestNewRabbitMQ_TLS_integration spins a WithTLS broker and proves the embedded
// certificates provision a working TLS listener: an amqps:// handshake against
// AMQPSURI() with the embedded CA succeeds. The broker is TLS-only, so URI()
// (plain) is intentionally not exercised here.
func TestNewRabbitMQ_TLS_integration(t *testing.T) {
	rmq := amqptest.NewRabbitMQ(t, amqptest.WithTLS())
	require.NotEmpty(t, rmq.AMQPSURI(), "AMQPSURI must be set when TLS is provisioned")

	sconn, err := amqp091.DialTLS(rmq.AMQPSURI(), clientTLS(t))
	require.NoError(t, err, "amqps dial against the provisioned TLS listener")
	require.NoError(t, sconn.Close())
}

func clientTLS(t *testing.T) *tls.Config {
	t.Helper()
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(amqptest.CACertPEM()))
	cert, err := tls.X509KeyPair(amqptest.ClientCertPEM(), amqptest.ClientKeyPEM())
	require.NoError(t, err)
	return &tls.Config{
		RootCAs:      roots,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
}
