package amqptest

import (
	"maps"
	"slices"
)

// config is the resolved fixture configuration. It is built from defaultConfig
// then mutated by the functional Options, last-wins.
type config struct {
	// version selects the RabbitMQ minor used to derive the base image name
	// ("rabbitmq:<version>-management") when no AMQPTEST_IMAGE override is set.
	version string
	// adminUser/adminPass are the broker credentials baked into URI().
	adminUser string
	adminPass string
	// extraPlugins are enabled (appended, preserving the image defaults) after
	// start in the non-pre-baked modes. The delayed-message plugin is added
	// automatically in the mounted-.ez mode and is expected to be baked into the
	// AMQPTEST_IMAGE in the pre-baked mode.
	extraPlugins []string
	// extraConfig is written to /etc/rabbitmq/conf.d as additional
	// rabbitmq.conf key=value entries.
	extraConfig map[string]string
	// tls provisions the broker's TLS listener from the embedded server certs so
	// AMQPSURI() is usable. Off by default — the testcontainers RabbitMQ module
	// configures TLS as listeners.tcp=none (TLS-only), which would break the
	// common plain-AMQP case; opt in with WithTLS for amqps:// tests.
	tls bool
}

func defaultConfig() config {
	return config{
		version:      "3.13",
		adminUser:    "guest",
		adminPass:    "guest",
		extraPlugins: []string{"rabbitmq_auth_mechanism_ssl"},
		extraConfig:  map[string]string{},
		tls:          false,
	}
}

// Option configures [NewRabbitMQ]. Options apply last-wins.
type Option func(*config)

// WithRabbitMQVersion overrides the RabbitMQ minor used to derive the base
// image ("rabbitmq:<version>-management"), e.g. "4.0". Ignored when the
// AMQPTEST_IMAGE environment variable is set (that image is used as-is).
func WithRabbitMQVersion(version string) Option {
	return func(c *config) { c.version = version }
}

// WithEnabledPlugins replaces the set of extra plugins enabled after start in
// the non-pre-baked modes (default: rabbitmq_auth_mechanism_ssl). The
// delayed-message plugin is still added automatically when a plugin source is
// configured; pass an empty list to enable nothing extra.
func WithEnabledPlugins(plugins ...string) Option {
	return func(c *config) { c.extraPlugins = slices.Clone(plugins) }
}

// WithExtraConfig merges additional rabbitmq.conf key=value entries into the
// broker config (written under /etc/rabbitmq/conf.d). Repeated keys last-wins.
func WithExtraConfig(kv map[string]string) Option {
	return func(c *config) { maps.Copy(c.extraConfig, kv) }
}

// WithTLS provisions a TLS listener from the embedded server certificate so
// [RabbitMQ.AMQPSURI] is usable for amqps:// tests. The testcontainers RabbitMQ
// module configures TLS as listeners.tcp=none, so a WithTLS broker is TLS-only:
// [RabbitMQ.URI] (plain amqp://) will not connect on such a broker.
func WithTLS() Option {
	return func(c *config) { c.tls = true }
}
