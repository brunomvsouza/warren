# amqptest

`amqptest` is an **internal** [testcontainers-go](https://golang.testcontainers.org/)
helper that spins up a RabbitMQ broker for warren's own integration suites (the
root and example `*_integration_test.go` files). It lives under `internal/` so it
is not part of warren's public API; the production packages never import it, so
its testcontainers/docker dependencies stay out of consumers' builds.

```go
import "github.com/brunomvsouza/warren/internal/amqptest"

func TestThing_integration(t *testing.T) {
    rmq := amqptest.NewRabbitMQ(t)              // auto-terminated via t.Cleanup
    conn, err := warren.Dial(t.Context(), rmq.URI())
    // ...
}
```

`NewRabbitMQ` enables `rabbitmq_auth_mechanism_ssl`, the delayed-message plugin
(see modes below), and returns a `*RabbitMQ` exposing:

| Method | Purpose |
| ------ | ------- |
| `URI() string` | `amqp://` connection string (with credentials). |
| `AMQPSURI() string` | `amqps://` connection string (only with `WithTLS`; empty otherwise). |
| `Container() testcontainers.Container` | underlying container for advanced cases. |
| `Cleanup(t)` | optional explicit terminate (already registered via `t.Cleanup`). |

Options (last-wins): `WithRabbitMQVersion("4.0")`, `WithEnabledPlugins(...)`,
`WithExtraConfig(map[string]string{...})`, `WithTLS()`.

## Delayed-message plugin: three modes

`rabbitmq_delayed_message_exchange` is a community plugin **not bundled** in the
official `rabbitmq:*-management` images. `NewRabbitMQ` evaluates three modes in
order and picks the first available:

1. **Pre-baked image (preferred for CI).** Set `AMQPTEST_IMAGE` to an image with
   the plugin baked in; it is used as-is. Build one from
   [`docker/Dockerfile.amqptest`](docker/Dockerfile.amqptest):

   ```sh
   docker build -f internal/amqptest/docker/Dockerfile.amqptest -t <registry>/warren-amqptest:3.13 .
   AMQPTEST_IMAGE=<registry>/warren-amqptest:3.13 go test -tags=integration ./...
   ```

   A required CI lane must use this mode so the delayed-exchange criteria run for
   real instead of skipping green (Lens-10 TV-08).

2. **Mounted `.ez` (works offline).** Set `AMQPTEST_DELAYED_PLUGIN_FILE` to a
   local plugin archive; it is mounted into the broker's plugin directory and
   enabled after start.

   | RabbitMQ minor | Plugin release | Download |
   | -------------- | -------------- | -------- |
   | 3.13.x | `rabbitmq_delayed_message_exchange-3.13.0.ez` | <https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/tag/v3.13.0> |
   | 4.0.x  | `rabbitmq_delayed_message_exchange-4.0.x.ez` (match your minor) | <https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases> |

3. **Skip fallback.** With neither set, `amqptest.RequireDelayedExchange(t)`
   skips the test. Tests that declare a non-delayed topology run normally.

Gate a test on the plugin by calling `amqptest.RequireDelayedExchange(t)` at the
top. When a plugin source *is* configured it does **not** skip: a broker that was
expected to carry the plugin but does not then fails on the
`x-delayed-message` exchange declaration rather than passing green.

## TLS (`amqps://`)

`WithTLS()` provisions a TLS listener from the certificates in
[`certs/`](certs/) (CA, server, client; CN/SAN cover `localhost`, `127.0.0.1`
and `::1`). The underlying module configures TLS as `listeners.tcp = none`, so a
`WithTLS` broker is **TLS-only** — use `AMQPSURI()`, not `URI()`.

Build an `amqps://` client `tls.Config` from the embedded fixtures:

```go
roots := x509.NewCertPool()
roots.AppendCertsFromPEM(amqptest.CACertPEM())
cert, _ := tls.X509KeyPair(amqptest.ClientCertPEM(), amqptest.ClientKeyPEM())
cfg := &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
```

The client certificate's CN is `guest`, so a broker running
`rabbitmq_auth_mechanism_ssl` with `ssl_cert_login_from = common_name` maps it to
the built-in `guest` user (used by warren's SASL EXTERNAL test). Regenerate the
certificates with `go run gen.go` in `certs/`. **These are test credentials with
no security value; never use them in production.**

`amqptest.RequireSSLAuth(t)` gates a test on guaranteed SASL EXTERNAL support,
which only the pre-baked `AMQPTEST_IMAGE` contracts; it skips otherwise.

## Ryuk

testcontainers' resource reaper (Ryuk) pulls `testcontainers/ryuk` on first use.
In sandboxes without registry access, set `TESTCONTAINERS_RYUK_DISABLED=true`.
