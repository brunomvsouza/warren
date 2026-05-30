//go:build integration

package amqptest

import (
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func applyOpts(opts ...Option) config {
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}
	return c
}

func TestDefaultConfig(t *testing.T) {
	c := defaultConfig()
	assert.Equal(t, "3.13", c.version)
	assert.Equal(t, "guest", c.adminUser)
	assert.Equal(t, "guest", c.adminPass)
	assert.Equal(t, []string{"rabbitmq_auth_mechanism_ssl"}, c.extraPlugins)
	assert.False(t, c.tls, "TLS is opt-in (the module makes it TLS-only)")
}

func TestWithTLS_Enables(t *testing.T) {
	assert.True(t, applyOpts(WithTLS()).tls)
}

func TestOptions_LastWins(t *testing.T) {
	c := applyOpts(WithRabbitMQVersion("3.13"), WithRabbitMQVersion("4.0"))
	assert.Equal(t, "4.0", c.version)
}

func TestWithEnabledPlugins_Replaces(t *testing.T) {
	c := applyOpts(WithEnabledPlugins("a", "b"))
	assert.Equal(t, []string{"a", "b"}, c.extraPlugins)

	c = applyOpts(WithEnabledPlugins())
	assert.Empty(t, c.extraPlugins)
}

func TestWithExtraConfig_MergesLastWins(t *testing.T) {
	c := applyOpts(
		WithExtraConfig(map[string]string{"k1": "v1", "k2": "v2"}),
		WithExtraConfig(map[string]string{"k2": "override"}),
	)
	assert.Equal(t, map[string]string{"k1": "v1", "k2": "override"}, c.extraConfig)
}

func stubEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolve_PrebakedImage(t *testing.T) {
	r := resolve(defaultConfig(), stubEnv(map[string]string{EnvImage: "my/rmq:custom"}))
	assert.Equal(t, modePrebakedImage, r.mode)
	assert.Equal(t, "my/rmq:custom", r.image)
	assert.True(t, r.delayedAvailable)
}

func TestResolve_ImageBeatsPluginFile(t *testing.T) {
	r := resolve(defaultConfig(), stubEnv(map[string]string{
		EnvImage:             "my/rmq:custom",
		EnvDelayedPluginFile: "/tmp/x.ez",
	}))
	assert.Equal(t, modePrebakedImage, r.mode, "AMQPTEST_IMAGE takes precedence")
}

func TestResolve_MountedPlugin(t *testing.T) {
	r := resolve(defaultConfig(), stubEnv(map[string]string{EnvDelayedPluginFile: "/tmp/delayed.ez"}))
	assert.Equal(t, modeMountedPlugin, r.mode)
	assert.Equal(t, "rabbitmq:3.13-management", r.image)
	assert.Equal(t, "/tmp/delayed.ez", r.delayedPluginFile)
	assert.True(t, r.delayedAvailable)
}

func TestResolve_NoDelayed(t *testing.T) {
	r := resolve(defaultConfig(), stubEnv(nil))
	assert.Equal(t, modeNoDelayed, r.mode)
	assert.Equal(t, "rabbitmq:3.13-management", r.image)
	assert.False(t, r.delayedAvailable)
}

func TestResolve_VersionDerivesBaseImage(t *testing.T) {
	r := resolve(applyOpts(WithRabbitMQVersion("4.0")), stubEnv(nil))
	assert.Equal(t, "rabbitmq:4.0-management", r.image)
}

func TestRenderConf_SortedDeterministic(t *testing.T) {
	got := renderConf(map[string]string{"zeta": "1", "alpha": "2", "mu": "3"})
	assert.Equal(t, "alpha = 2\nmu = 3\nzeta = 1\n", got)
	assert.Empty(t, renderConf(nil))
}

// TestRequireDelayedExchange_SkipsWhenUnset asserts the gate Goexits (skips)
// when no plugin source is configured: the line after the call is unreached.
func TestRequireDelayedExchange_SkipsWhenUnset(t *testing.T) {
	swapEnv(t, stubEnv(nil))
	reached := false
	t.Run("gated", func(t *testing.T) {
		RequireDelayedExchange(t)
		reached = true
	})
	assert.False(t, reached, "RequireDelayedExchange must skip when no plugin source is set")
}

func TestRequireDelayedExchange_ProceedsWhenConfigured(t *testing.T) {
	swapEnv(t, stubEnv(map[string]string{EnvDelayedPluginFile: "/tmp/x.ez"}))
	reached := false
	t.Run("gated", func(t *testing.T) {
		RequireDelayedExchange(t)
		reached = true
	})
	assert.True(t, reached, "RequireDelayedExchange must proceed when a plugin source is configured")
}

func TestRequireSSLAuth_SkipsWithoutImage(t *testing.T) {
	swapEnv(t, stubEnv(map[string]string{EnvDelayedPluginFile: "/tmp/x.ez"}))
	reached := false
	t.Run("gated", func(t *testing.T) {
		RequireSSLAuth(t)
		reached = true
	})
	assert.False(t, reached, "RequireSSLAuth must skip when AMQPTEST_IMAGE is unset")
}

func TestRequireSSLAuth_ProceedsWithImage(t *testing.T) {
	swapEnv(t, stubEnv(map[string]string{EnvImage: "my/rmq:custom"}))
	reached := false
	t.Run("gated", func(t *testing.T) {
		RequireSSLAuth(t)
		reached = true
	})
	assert.True(t, reached)
}

// swapEnv replaces the package getenv accessor for the duration of the test.
func swapEnv(t *testing.T, fn func(string) string) {
	t.Helper()
	old := getenv
	getenv = fn
	t.Cleanup(func() { getenv = old })
}

func TestEmbeddedCerts_ParseAndChain(t *testing.T) {
	caBlock, _ := pem.Decode(CACertPEM())
	require.NotNil(t, caBlock, "ca.pem decodes")
	ca, err := x509.ParseCertificate(caBlock.Bytes)
	require.NoError(t, err)
	assert.True(t, ca.IsCA)

	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(CACertPEM()))

	// Server cert verifies against the CA for hostname "localhost".
	srvBlock, _ := pem.Decode(ServerCertPEM())
	require.NotNil(t, srvBlock)
	srv, err := x509.ParseCertificate(srvBlock.Bytes)
	require.NoError(t, err)
	_, err = srv.Verify(x509.VerifyOptions{Roots: roots, DNSName: "localhost", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	assert.NoError(t, err, "server cert must verify against the embedded CA for localhost")

	// Client cert maps to CN "guest".
	cliBlock, _ := pem.Decode(ClientCertPEM())
	require.NotNil(t, cliBlock)
	cli, err := x509.ParseCertificate(cliBlock.Bytes)
	require.NoError(t, err)
	assert.Equal(t, "guest", cli.Subject.CommonName)
}

func TestAccessorsReturnCopies(t *testing.T) {
	a := CACertPEM()
	require.NotEmpty(t, a)
	a[0] ^= 0xFF
	assert.NotEqual(t, a, CACertPEM(), "accessor must return a defensive copy")
}
