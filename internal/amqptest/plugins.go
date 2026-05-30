package amqptest

import (
	"os"
	"testing"
)

// Environment variables that select how the delayed-message plugin is provided.
// See the package doc and README for the three-mode strategy.
const (
	// EnvImage names a pre-baked broker image (with the delayed-message plugin
	// and rabbitmq_auth_mechanism_ssl already enabled) to use as-is.
	EnvImage = "AMQPTEST_IMAGE"
	// EnvDelayedPluginFile names a local rabbitmq_delayed_message_exchange .ez
	// archive to mount into the broker's plugin directory.
	EnvDelayedPluginFile = "AMQPTEST_DELAYED_PLUGIN_FILE"
)

// getenv is the environment accessor used by NewRabbitMQ and the Require* gates;
// it is a package var so tests can substitute it.
var getenv = os.Getenv

// pluginMode is the resolved plugin-provisioning strategy.
type pluginMode int

const (
	// modePrebakedImage uses AMQPTEST_IMAGE as-is (preferred for CI).
	modePrebakedImage pluginMode = iota
	// modeMountedPlugin mounts the AMQPTEST_DELAYED_PLUGIN_FILE .ez and enables it.
	modeMountedPlugin
	// modeNoDelayed uses the base rabbitmq:<version>-management image; the
	// delayed-message plugin is unavailable and RequireDelayedExchange skips.
	modeNoDelayed
)

// resolution is the outcome of evaluating the three plugin-enablement modes.
type resolution struct {
	mode              pluginMode
	image             string
	delayedPluginFile string
	delayedAvailable  bool
}

// resolve picks the first available plugin mode in the order documented in
// SPEC §6: pre-baked image, then mounted .ez, then no-delayed fallback. getenv
// is injected so the decision is unit-testable.
func resolve(cfg config, getenv func(string) string) resolution {
	if img := getenv(EnvImage); img != "" {
		return resolution{mode: modePrebakedImage, image: img, delayedAvailable: true}
	}
	base := "rabbitmq:" + cfg.version + "-management"
	if f := getenv(EnvDelayedPluginFile); f != "" {
		return resolution{mode: modeMountedPlugin, image: base, delayedPluginFile: f, delayedAvailable: true}
	}
	return resolution{mode: modeNoDelayed, image: base, delayedAvailable: false}
}

// delayedConfigured reports whether a delayed-message plugin source is set.
func delayedConfigured(getenv func(string) string) bool {
	return getenv(EnvImage) != "" || getenv(EnvDelayedPluginFile) != ""
}

// RequireDelayedExchange gates a test on the rabbitmq_delayed_message_exchange
// plugin. When no plugin source is configured (neither AMQPTEST_IMAGE nor
// AMQPTEST_DELAYED_PLUGIN_FILE) it skips. When a source IS configured the test
// proceeds: a broker that was expected to carry the plugin but does not then
// fails on the x-delayed-message exchange declaration rather than skipping green
// (Lens-10 TV-08 — fail, not skip, when the plugin is expected but missing).
func RequireDelayedExchange(t *testing.T) {
	t.Helper()
	if !delayedConfigured(getenv) {
		t.Skip("delayed-message plugin not available; set AMQPTEST_IMAGE or AMQPTEST_DELAYED_PLUGIN_FILE")
	}
}

// RequireSSLAuth gates a test on SASL EXTERNAL (rabbitmq_auth_mechanism_ssl with
// the certificate-mapped user). A guaranteed setup — plugin enabled, TLS
// listener, and the external_auth user/ssl_cert_login config — is contracted
// only by the pre-baked AMQPTEST_IMAGE, so this skips when that image is unset.
func RequireSSLAuth(t *testing.T) {
	t.Helper()
	if getenv(EnvImage) == "" {
		t.Skip("SASL EXTERNAL not guaranteed; set AMQPTEST_IMAGE to a broker image with rabbitmq_auth_mechanism_ssl and the external_auth user baked in")
	}
}
