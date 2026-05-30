package amqptest

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

// RabbitMQ is a running RabbitMQ test broker. Construct it with [NewRabbitMQ];
// it is terminated automatically when the test ends.
type RabbitMQ struct {
	container *rabbitmq.RabbitMQContainer
	amqpURI   string
	amqpsURI  string
	once      sync.Once
}

// NewRabbitMQ starts a RabbitMQ broker in a container and returns a handle. The
// broker is registered for automatic termination via t.Cleanup, so most tests
// need only:
//
//	rmq := amqptest.NewRabbitMQ(t)
//
// It enables rabbitmq_auth_mechanism_ssl and (per the plugin mode — see
// [RequireDelayedExchange]) the delayed-message plugin. Pass [WithTLS] to also
// provision a TLS listener (the broker is then TLS-only). Any provisioning
// failure fails the test via t.
func NewRabbitMQ(t *testing.T, opts ...Option) *RabbitMQ {
	t.Helper()

	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	res := resolve(cfg, getenv)

	ctx := context.Background()
	customizers := []testcontainers.ContainerCustomizer{
		rabbitmq.WithAdminUsername(cfg.adminUser),
		rabbitmq.WithAdminPassword(cfg.adminPass),
	}

	if cfg.tls {
		dir := t.TempDir()
		caPath, certPath, keyPath, err := writeServerTLSFiles(dir)
		require.NoError(t, err, "amqptest: write TLS fixtures")
		customizers = append(customizers, rabbitmq.WithSSL(rabbitmq.SSLSettings{
			CACertFile:       caPath,
			CertFile:         certPath,
			KeyFile:          keyPath,
			VerificationMode: rabbitmq.SSLVerificationModeNone,
			FailIfNoCert:     false,
		}))
	}

	if res.mode == modeMountedPlugin {
		customizers = append(customizers, testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath:      res.delayedPluginFile,
			ContainerFilePath: "/opt/rabbitmq/plugins/" + filepath.Base(res.delayedPluginFile),
			FileMode:          0o644,
		}))
	}

	if len(cfg.extraConfig) > 0 {
		customizers = append(customizers, testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(renderConf(cfg.extraConfig)),
			ContainerFilePath: "/etc/rabbitmq/conf.d/20-warren-amqptest.conf",
			FileMode:          0o644,
		}))
	}

	container, err := rabbitmq.Run(ctx, res.image, customizers...)
	require.NoError(t, err, "amqptest: start RabbitMQ container (image %q)", res.image)

	r := &RabbitMQ{container: container}
	t.Cleanup(func() { r.terminate(t) })

	if res.mode != modePrebakedImage {
		plugins := append([]string(nil), cfg.extraPlugins...)
		if res.mode == modeMountedPlugin {
			plugins = append(plugins, "rabbitmq_delayed_message_exchange")
		}
		if len(plugins) > 0 {
			enablePlugins(ctx, t, container, plugins)
		}
	}

	r.amqpURI, err = container.AmqpURL(ctx)
	require.NoError(t, err, "amqptest: read AMQP URL")
	if cfg.tls {
		r.amqpsURI, err = container.AmqpsURL(ctx)
		require.NoError(t, err, "amqptest: read AMQPS URL")
	}
	return r
}

// URI returns the amqp:// connection string (with credentials) for the broker.
func (r *RabbitMQ) URI() string { return r.amqpURI }

// AMQPSURI returns the amqps:// connection string for the broker's TLS listener.
// It is empty when TLS provisioning was disabled.
func (r *RabbitMQ) AMQPSURI() string { return r.amqpsURI }

// Container returns the underlying testcontainers container for advanced cases
// (custom exec, copying files, reading logs).
func (r *RabbitMQ) Container() testcontainers.Container { return r.container }

// Cleanup terminates the broker. It is optional — [NewRabbitMQ] already registers
// termination via t.Cleanup — and is safe to call more than once.
func (r *RabbitMQ) Cleanup(t *testing.T) {
	t.Helper()
	r.terminate(t)
}

func (r *RabbitMQ) terminate(t *testing.T) {
	t.Helper()
	r.once.Do(func() {
		if err := r.container.Terminate(context.Background()); err != nil {
			t.Logf("amqptest: terminate RabbitMQ container: %v", err)
		}
	})
}

// enablePlugins enables plugins on a running broker via rabbitmq-plugins, which
// appends to the enabled set (preserving the image's defaults such as
// rabbitmq_management) rather than replacing it.
func enablePlugins(ctx context.Context, t *testing.T, c *rabbitmq.RabbitMQContainer, plugins []string) {
	t.Helper()
	cmd := append([]string{"rabbitmq-plugins", "enable"}, plugins...)
	code, _, err := c.Exec(ctx, cmd)
	require.NoError(t, err, "amqptest: exec rabbitmq-plugins enable %v", plugins)
	require.Zerof(t, code, "amqptest: rabbitmq-plugins enable %v exited %d", plugins, code)
}

// renderConf serialises extra rabbitmq.conf entries deterministically (sorted by
// key) as "key = value" lines.
func renderConf(kv map[string]string) string {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s = %s\n", k, kv[k])
	}
	return b.String()
}
