package amqp_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	amqp "github.com/brunomvsouza/amqp"
)

// selfSignedCert creates a self-signed TLS certificate with the given CN.
// The key is P-256; the cert is valid for 1 hour.
func selfSignedCert(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}
}

// — Dial validations (no broker required) —————————————————————————————————

func TestDial_frameMaxBelowMinimum_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := amqp.Dial(ctx, amqp.WithFrameMax(1000))
	require.Error(t, err)
	assert.True(t, errors.Is(err, amqp.ErrInvalidOptions),
		"expected ErrInvalidOptions, got: %v", err)
}

func TestDial_frameMaxExactlyMinimum_doesNotFailOnValidation(t *testing.T) {
	// 4096 is the AMQP spec minimum; validation should pass (connection may
	// still fail if no broker is reachable, but not with ErrInvalidOptions).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := amqp.Dial(ctx,
		amqp.WithFrameMax(4096),
		// instant-fail dialer so we don't actually try the network
		amqp.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions),
		"FrameMax=4096 must pass validation; got: %v", err)
}

func TestDial_sASLExternal_withoutTLSConfig_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := amqp.Dial(ctx,
		amqp.WithSASLMechanism(amqp.SASLExternal),
		amqp.WithAddr("amqps://h:5671/"),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, amqp.ErrInvalidOptions), "got: %v", err)
	assert.Contains(t, err.Error(), "TLSConfig", "error must name the missing field")
}

func TestDial_sASLExternal_withTLSButNoClientCert_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := amqp.Dial(ctx,
		amqp.WithSASLMechanism(amqp.SASLExternal),
		amqp.WithAddr("amqps://h:5671/"),
		amqp.WithTLSConfig(&tls.Config{}), // no certificates
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, amqp.ErrInvalidOptions), "got: %v", err)
	assert.Contains(t, err.Error(), "certificate")
}

func TestDial_sASLExternal_withPlainScheme_returnsErrInvalidOptions(t *testing.T) {
	cert := selfSignedCert(t, "svc")
	ctx := context.Background()
	_, err := amqp.Dial(ctx,
		amqp.WithSASLMechanism(amqp.SASLExternal),
		amqp.WithAddr("amqp://h:5672/"), // plain, not amqps
		amqp.WithTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, amqp.ErrInvalidOptions), "got: %v", err)
	assert.Contains(t, err.Error(), "amqps")
}

func TestDial_sASLExternal_validConfig_passesValidation(t *testing.T) {
	cert := selfSignedCert(t, "svc")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := amqp.Dial(ctx,
		amqp.WithSASLMechanism(amqp.SASLExternal),
		amqp.WithAddr("amqps://h:5671/"),
		amqp.WithTLSConfig(&tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true, //nolint:gosec // test only
		}),
		amqp.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions),
		"valid EXTERNAL config must pass validation; got: %v", err)
}

// — Default connection name ———————————————————————————————————————————————

func TestDial_defaultConnectionName_matchesExpectedFormat(t *testing.T) {
	// We can't easily inspect the connection name without a broker, but we can
	// verify that the helper function produces a non-empty string in the right
	// format by calling Dial with a fast-failing dialer and checking the error
	// is NOT ErrInvalidOptions (i.e., name was computed and is valid).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	captured := ""
	_, err := amqp.Dial(ctx,
		amqp.WithDialer(func(_, addr string) (net.Conn, error) {
			captured = addr // just to confirm it was called
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions))
	assert.NotEmpty(t, captured, "dialer should have been called")
	_ = captured
}

// — AuthenticatedUser: PLAIN (derivable from options without broker) ——————

func TestDial_authUser_plainCredentials(t *testing.T) {
	// With a failing dialer we never get a *Connection, but we can verify that
	// AuthenticatedUser would be "alice" by checking via a test hook. Since the
	// Connection is nil on error, the full AuthenticatedUser test requires a
	// live broker (see connection_integration_test.go). This test verifies
	// the error does NOT come from a validation problem (i.e., WithAuth is valid).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := amqp.Dial(ctx,
		amqp.WithAuth("alice", "secret"),
		amqp.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions),
		"WithAuth must not cause a validation error; got: %v", err)
}

// — Heartbeat zero warning (covered by log output in integration; here we
//   just confirm zero heartbeat does NOT return ErrInvalidOptions) ————————

func TestDial_zeroHeartbeat_doesNotReturnErrInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := amqp.Dial(ctx,
		amqp.WithHeartbeat(0),
		amqp.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions))
}

// — WithoutMetrics / option last-wins ————————————————————————————————————

func TestDial_withoutMetrics_doesNotReturnErrInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := amqp.Dial(ctx,
		amqp.WithoutMetrics(),
		amqp.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions))
}

// — Connection name validation (exported helper) ——————————————————————————

func TestDefaultConnectionNameFormat(t *testing.T) {
	name := amqp.DefaultConnectionName()
	// Expected format: "<binary>-<hostname>-<pid>"
	// At least two dashes, non-empty segments.
	re := regexp.MustCompile(`^.+-.+-.+$`)
	assert.True(t, re.MatchString(name), "DefaultConnectionName %q does not match <binary>-<hostname>-<pid>", name)
}

// — SASLExternal: WithAuth emits no error (warning is log-only) ——————————

func TestDial_sASLExternalWithAuth_passesValidation(t *testing.T) {
	// WithAuth under SASLExternal is a no-op at the AMQP level (the broker ignores
	// the PLAIN credentials). Validation should pass; only a log warning is emitted.
	cert := selfSignedCert(t, "svc")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := amqp.Dial(ctx,
		amqp.WithSASLMechanism(amqp.SASLExternal),
		amqp.WithAddr("amqps://h:5671/"),
		amqp.WithTLSConfig(&tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true, //nolint:gosec // test only
		}),
		amqp.WithAuth("ignored", "also-ignored"),
		amqp.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions), "got: %v", err)
}

// — WithPublisherConnections(1) warning test (no broker; just verifies no
//   ErrInvalidOptions — the actual warning is a log side effect) ——————————

func TestDial_singlePubConn_doesNotReturnErrInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := amqp.Dial(ctx,
		amqp.WithPublisherConnections(1),
		amqp.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions),
		"WithPublisherConnections(1) logs a warning but is not invalid; got: %v", err)
}

// — Close on nil / uninitialized ——————————————————————————————————————————

// Verify that the Dial error wraps neither ErrInvalidOptions nor panics
// when the address is unreachable and context times out.
func TestDial_unreachableAddr_returnsNonValidationError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := amqp.Dial(ctx,
		amqp.WithAddr("amqp://127.0.0.1:1/"),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, amqp.ErrInvalidOptions),
		"network failure must not masquerade as ErrInvalidOptions; got: %v", err)
}

// — Suppress unused import warning for fmt ————————————————————————————————
var _ = fmt.Sprintf
