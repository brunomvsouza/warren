package warren

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// T72 (R10-17 / SRE-09): the default dialer carries a TCP keepalive so a write
// to a half-open socket fails promptly rather than relying on ConfirmTimeout as
// the only backstop.
func TestDefaultNetDialer_carriesKeepAlive(t *testing.T) {
	d := defaultNetDialer()
	assert.Greater(t, d.KeepAlive, time.Duration(0), "default dialer must enable TCP keepalive")
	assert.Positive(t, d.Timeout, "default dialer must carry a connect timeout")
}

// TestApplyConnDefaults_installsDefaultDialer confirms a Dial without WithDialer
// gets the keepalive default dialer, and WithDialer still overrides it.
func TestApplyConnDefaults_installsDefaultDialer(t *testing.T) {
	t.Run("default installed", func(t *testing.T) {
		opts := &connOptions{}
		applyConnDefaults(opts)
		require.NotNil(t, opts.dialer, "applyConnDefaults must install the keepalive default dialer")
	})

	t.Run("WithDialer wins", func(t *testing.T) {
		sentinel := func(_, _ string) (net.Conn, error) { return nil, assert.AnError }
		opts := &connOptions{dialer: sentinel}
		applyConnDefaults(opts)
		_, err := opts.dialer("tcp", "x")
		assert.ErrorIs(t, err, assert.AnError, "an explicit WithDialer must not be overwritten by the default")
	})
}
