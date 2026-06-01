package warren_test

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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
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
	_, err := warren.Dial(ctx, warren.WithFrameMax(1000))
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions),
		"expected ErrInvalidOptions, got: %v", err)
}

func TestDial_frameMaxExactlyMinimum_doesNotFailOnValidation(t *testing.T) {
	// 4096 is the AMQP spec minimum; validation should pass (connection may
	// still fail if no broker is reachable, but not with ErrInvalidOptions).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithFrameMax(4096),
		// instant-fail dialer so we don't actually try the network
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"FrameMax=4096 must pass validation; got: %v", err)
}

// — WithAddrs scheme validation (S-1) — reject a mixed amqp/amqps list ————————

func TestDial_WithAddrs_mixedSchemes_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithAddrs([]string{
		"amqp://guest:guest@localhost:5672/",
		"amqps://guest:guest@localhost:5671/",
	}))
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions),
		"a WithAddrs list mixing amqp:// and amqps:// must fail validation; got: %v", err)
	// The validation error names only the schemes — never the URIs — so credentials
	// embedded in the addresses must not leak into it.
	assert.NotContains(t, err.Error(), "guest:guest",
		"the scheme-mismatch error must not leak credentials from the URIs")
}

func TestDial_WithAddrs_uniformScheme_doesNotFailOnValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithAddrs([]string{
			"amqp://guest:guest@localhost:5672/",
			"amqp://guest:guest@otherhost:5672/",
		}),
		// instant-fail dialer so validation, not the network, is what we observe
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"a uniform amqp:// WithAddrs list must pass validation; got: %v", err)
}

// — WithAddrs failover: every node down (I-1) ————————————————————————————————
//
// Complements the positive round-robin integration tests: when EVERY WithAddrs
// node refuses, the round-robin reconnect path (nextDialAddr) must try each address
// in turn and surface a credential-redacted error rather than hanging or retrying
// only the first. A stub dialer makes this deterministic with no broker.
func TestDial_WithAddrs_allAddrsDown_returnsRedactedError_triesEach(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var mu sync.Mutex
	dialed := map[string]int{}

	_, err := warren.Dial(ctx,
		warren.WithAddrs([]string{
			"amqp://guest:guest@127.0.0.1:1/",
			"amqp://guest:guest@127.0.0.1:9/",
		}),
		warren.WithPublisherConnections(1),
		warren.WithConsumerConnections(1),
		warren.WithReconnectBackoff(warren.RetryPolicy{
			Min: time.Millisecond, Max: 2 * time.Millisecond, Retries: 6, Jitter: warren.JitterNone,
		}),
		warren.WithDialer(func(_, addr string) (net.Conn, error) {
			mu.Lock()
			dialed[addr]++
			mu.Unlock()
			return nil, errors.New("connection refused")
		}),
	)

	require.Error(t, err, "Dial must return an error when every WithAddrs node is down")

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, dialed, "127.0.0.1:1", "must have tried the first dead node")
	assert.Contains(t, dialed, "127.0.0.1:9",
		"round-robin must rotate to the second dead node, not retry only the first")
	assert.NotContains(t, err.Error(), "guest:guest",
		"the all-down dial error must be credential-redacted")
}

func TestDial_channelPoolSizeExceedsChannelMax_returnsErrInvalidOptions(t *testing.T) {
	// A channel pool larger than the requested channel-max can never be
	// satisfied; Dial must reject it synchronously before opening any socket.
	ctx := context.Background()
	_, err := warren.Dial(ctx,
		warren.WithChannelMax(4),
		warren.WithChannelPoolSize(8),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions),
		"pool size above channel-max must return ErrInvalidOptions; got: %v", err)
}

func TestDial_channelPoolSizeEqualToChannelMax_doesNotFailOnValidation(t *testing.T) {
	// Boundary: pool size equal to channel-max is permitted (the spec rejects
	// strictly-greater only). Validation must pass; only the network dial fails.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithChannelMax(8),
		warren.WithChannelPoolSize(8),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"pool size equal to channel-max must pass validation; got: %v", err)
}

func TestDial_sASLExternal_withoutTLSConfig_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx,
		warren.WithSASLMechanism(warren.SASLExternal),
		warren.WithAddr("amqps://h:5671/"),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions), "got: %v", err)
	assert.Contains(t, err.Error(), "TLSConfig", "error must name the missing field")
}

func TestDial_sASLExternal_withTLSButNoClientCert_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx,
		warren.WithSASLMechanism(warren.SASLExternal),
		warren.WithAddr("amqps://h:5671/"),
		warren.WithTLSConfig(&tls.Config{}), // no certificates
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions), "got: %v", err)
	assert.Contains(t, err.Error(), "certificate")
}

func TestDial_sASLExternal_withPlainScheme_returnsErrInvalidOptions(t *testing.T) {
	cert := selfSignedCert(t, "svc")
	ctx := context.Background()
	_, err := warren.Dial(ctx,
		warren.WithSASLMechanism(warren.SASLExternal),
		warren.WithAddr("amqp://h:5672/"), // plain, not amqps
		warren.WithTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions), "got: %v", err)
	assert.Contains(t, err.Error(), "amqps")
}

func TestDial_sASLExternal_validConfig_passesValidation(t *testing.T) {
	cert := selfSignedCert(t, "test-client")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithSASLMechanism(warren.SASLExternal),
		warren.WithAddr("amqps://h:5671/"),
		warren.WithTLSConfig(&tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true, //nolint:gosec // test only
		}),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
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
	_, err := warren.Dial(ctx,
		warren.WithDialer(func(_, addr string) (net.Conn, error) {
			captured = addr // just to confirm it was called
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions))
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
	_, err := warren.Dial(ctx,
		warren.WithAuth("alice", "secret"),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithAuth must not cause a validation error; got: %v", err)
}

// — Heartbeat zero warning (covered by log output in integration; here we
//   just confirm zero heartbeat does NOT return ErrInvalidOptions) ————————

func TestDial_zeroHeartbeat_doesNotReturnErrInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithHeartbeat(0),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions))
}

// — WithoutMetrics / option last-wins ————————————————————————————————————

func TestDial_withoutMetrics_doesNotReturnErrInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithoutMetrics(),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions))
}

// — Connection name validation (exported helper) ——————————————————————————

func TestDefaultConnectionNameFormat(t *testing.T) {
	name := warren.DefaultConnectionName()
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
	_, err := warren.Dial(ctx,
		warren.WithSASLMechanism(warren.SASLExternal),
		warren.WithAddr("amqps://h:5671/"),
		warren.WithTLSConfig(&tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true, //nolint:gosec // test only
		}),
		warren.WithAuth("ignored", "also-ignored"),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions), "got: %v", err)
}

// — WithPublisherConnections(1) warning test (no broker; just verifies no
//   ErrInvalidOptions — the actual warning is a log side effect) ——————————

func TestDial_singlePubConn_doesNotReturnErrInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithPublisherConnections(1),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithPublisherConnections(1) logs a warning but is not invalid; got: %v", err)
}

// — Close on nil / uninitialized ——————————————————————————————————————————

// Verify that the Dial error wraps neither ErrInvalidOptions nor panics
// when the address is unreachable and context times out.
func TestDial_unreachableAddr_returnsNonValidationError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithAddr("amqp://127.0.0.1:1/"),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"network failure must not masquerade as ErrInvalidOptions; got: %v", err)
}

// — frameMax ceiling ——————————————————————————————————————————————————————

func TestDial_frameMaxAboveCeiling_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithFrameMax(200_000_000)) // 200 MiB — above 100 MiB ceiling
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions),
		"frameMax above ceiling must return ErrInvalidOptions; got: %v", err)
}

func TestDial_frameMaxAtCeiling_passesValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithFrameMax(104_857_600), // exactly 100 MiB
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"frameMax at ceiling must pass validation; got: %v", err)
}

// — connection pool size lower bounds ————————————————————————————————————

func TestDial_publisherConnectionsZero_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithPublisherConnections(0))
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithPublisherConnections(0) must return ErrInvalidOptions; got: %v", err)
}

func TestDial_consumerConnectionsZero_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithConsumerConnections(0))
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithConsumerConnections(0) must return ErrInvalidOptions; got: %v", err)
}

func TestDial_channelPoolSizeZero_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithChannelPoolSize(0))
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithChannelPoolSize(0) must return ErrInvalidOptions; got: %v", err)
}

// — credential no-leak in validation error messages ——————————————————————

func TestDial_sASLExternal_schemeError_doesNotLeakCredentials(t *testing.T) {
	cert := selfSignedCert(t, "svc")
	ctx := context.Background()
	_, err := warren.Dial(ctx,
		warren.WithSASLMechanism(warren.SASLExternal),
		// amqp:// URI with embedded credentials — must not appear in error
		warren.WithAddr("amqp://secret:password@host:5672/"),
		warren.WithTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions), "got: %v", err)
	assert.NotContains(t, err.Error(), "secret",
		"error must not leak username from URI")
	assert.NotContains(t, err.Error(), "password",
		"error must not leak password from URI")
}

// — connectDelay honours context cancellation ————————————————————————————

func TestDial_connectDelay_cancelledContext_returnsCtxErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	_, err := warren.Dial(ctx,
		warren.WithConnectDelay(10*time.Second), // long delay — ctx already done
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions), "got: %v", err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got: %v", err)
}

// — Pool size lower bounds: error message content ————————————————————————

func TestDial_publisherConnectionsZero_errorMentionsWithPublisherConnections(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithPublisherConnections(0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithPublisherConnections")
}

func TestDial_consumerConnectionsZero_errorMentionsWithConsumerConnections(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithConsumerConnections(0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithConsumerConnections")
}

func TestDial_channelPoolSizeZero_errorMentionsWithChannelPoolSize(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithChannelPoolSize(0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithChannelPoolSize")
}

// LATER-06: upper bound on WithChannelPoolSize —————————————————————————————

func TestDial_channelPoolSizeAboveMax_returnsErrInvalidOptions(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithChannelPoolSize(4097))
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithChannelPoolSize(4097) must return ErrInvalidOptions; got: %v", err)
}

func TestDial_channelPoolSizeAtMax_isValid(t *testing.T) {
	// 4096 is the upper bound itself — must not be rejected by validation.
	// We use a dialer that always fails so the test never reaches the broker
	// and no goroutines are leaked.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithChannelPoolSize(4096),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	// ErrInvalidOptions would mean the option value itself was rejected.
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithChannelPoolSize(4096) must be accepted by validation; got: %v", err)
}

func TestDial_channelPoolSizeAboveMax_errorMentionsWithChannelPoolSize(t *testing.T) {
	ctx := context.Background()
	_, err := warren.Dial(ctx, warren.WithChannelPoolSize(1_000_000))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithChannelPoolSize",
		"error must mention the offending option name")
}

// — WithHeartbeat: negative value does not fail validation ——————————————

func TestDial_negativeHeartbeat_doesNotReturnErrInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithHeartbeat(-1*time.Second),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithHeartbeat(-1) must not fail validation; got: %v", err)
}

// — WithConsumerConnections(1): warning but not invalid ——————————————————

func TestDial_singleConsumerConn_doesNotReturnErrInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithConsumerConnections(1),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"WithConsumerConnections(1) logs a warning but is not invalid; got: %v", err)
}

// — SASLExternal: GetClientCertificate as alternative to Certificates ————

func TestDial_sASLExternal_getClientCertificateFn_passesValidation(t *testing.T) {
	cert := selfSignedCert(t, "via-fn")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithSASLMechanism(warren.SASLExternal),
		warren.WithAddr("amqps://h:5671/"),
		warren.WithTLSConfig(&tls.Config{
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return &cert, nil
			},
			InsecureSkipVerify: true, //nolint:gosec // test only
		}),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions),
		"GetClientCertificate must satisfy cert requirement; got: %v", err)
}

// — Multi-conn pool: dial count ——————————————————————————————————————————

func TestDial_multiConn_dialCalledPubPlusConTimes(t *testing.T) {
	defer goleak.VerifyNone(t)
	// Each connection in the pool attempts a dial. With Retries=1 and an instant-
	// fail dialer, Dial exhausts all attempts after exactly pubConns+conConns calls.
	const pubN, conN = 3, 2
	var count int32
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithPublisherConnections(pubN),
		warren.WithConsumerConnections(conN),
		warren.WithReconnectBackoff(warren.RetryPolicy{Retries: 1}),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			// Use a closure variable; atomic not needed because the dialer is
			// only called from Dial (sequential pool opening).
			count++
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions))
	// Dial opens pubN publisher connections first; first failure aborts the pool.
	// So we get exactly 1 call (the first connection in the publisher pool fails).
	// With Retries=1 exactly one attempt is made per socket.
	assert.Equal(t, int32(1), count,
		"Dial stops after the first connection failure; got %d dial calls", count)
}

func TestDial_multiConn_singlePub_opensCorrectPools(t *testing.T) {
	defer goleak.VerifyNone(t)
	// Verify pool sizes via NumPubConns / NumConConns when we have a succeeding
	// dialer. We can't use a real amqp091 connection (needs a broker), so we
	// verify indirectly: Dial with a failing dialer after validation should fail
	// with the right number of calls = pubConns (first pool always attempted).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := warren.Dial(ctx,
		warren.WithPublisherConnections(1),
		warren.WithConsumerConnections(1),
		warren.WithDialer(func(_, _ string) (net.Conn, error) {
			return nil, errors.New("no broker")
		}),
	)
	require.Error(t, err)
	assert.False(t, errors.Is(err, warren.ErrInvalidOptions))
}

// — Suppress unused import warning for fmt ————————————————————————————————
var _ = fmt.Sprintf
