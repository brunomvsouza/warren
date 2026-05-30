// Package amqptest is an internal testcontainers-go helper that spins up a
// RabbitMQ broker for warren's own integration suites (the root and example
// *_integration_test.go files). It lives under internal/ so it is not part of
// warren's public API surface; the production packages never import it, so its
// testcontainers/docker dependencies stay out of consumers' builds.
//
// # Usage
//
//	func TestThing_integration(t *testing.T) {
//		rmq := amqptest.NewRabbitMQ(t)        // auto-terminated via t.Cleanup
//		conn, err := warren.Dial(t.Context(), rmq.URI())
//		// ...
//	}
//
// [NewRabbitMQ] enables rabbitmq_auth_mechanism_ssl and — depending on the
// plugin mode below — the rabbitmq_delayed_message_exchange plugin. Pass
// [WithTLS] to also provision a TLS listener from the embedded test
// certificates so [RabbitMQ.AMQPSURI] is usable; TLS is opt-in because the
// underlying module configures it as TLS-only (listeners.tcp=none).
//
// # Delayed-message plugin: three modes
//
// rabbitmq_delayed_message_exchange is a community plugin not bundled in the
// official rabbitmq:*-management images. The helper evaluates three modes in
// order (see [EnvImage], [EnvDelayedPluginFile]):
//
//  1. Pre-baked image — AMQPTEST_IMAGE is used as-is; CI publishes an image with
//     the .ez baked in (see docker/Dockerfile.amqptest). This is the mode a
//     required CI lane must use so the delayed-exchange criteria cannot silently
//     skip (Lens-10 TV-08).
//  2. Mounted .ez — AMQPTEST_DELAYED_PLUGIN_FILE points at a local .ez; the
//     helper mounts it and enables it after start. README.md lists tested plugin
//     versions per RabbitMQ minor with download URLs.
//  3. Skip fallback — neither is set; [RequireDelayedExchange] skips the test.
//
// # TLS fixtures
//
// certs/ holds long-lived test CA/server/client certificates (regenerate with
// `go run gen.go`). [CACertPEM], [ClientCertPEM] and [ClientKeyPEM] build an
// amqps:// client tls.Config; the server cert is mounted into the broker.
//
// # Build tag
//
// Every source file in this package except this doc carries //go:build
// integration, so the package — and the embedded TLS private keys in certs.go —
// compile only under the integration build tag (the lane that actually starts
// brokers). The default `go build` / `go test ./...` lane sees an empty package,
// keeping the committed test keys out of every non-integration build. The
// package's own option / plugin-mode / cert unit tests run on the integration
// lane as a result, alongside the helper they exercise.
package amqptest
