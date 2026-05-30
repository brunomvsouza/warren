# TEST FIXTURES — not for production

The `*.pem` / `*.key` files in this directory are **throwaway TLS test
fixtures** with **no security value**. They exist only so warren's own
integration suite can start a TLS-enabled RabbitMQ container (`WithTLS`) and
connect to it over `amqps://` + SASL EXTERNAL.

Do **not** use them anywhere real:

- The private keys are committed to the repository in the clear — they are
  public by definition.
- The server certificate's SAN list covers loopback only (`localhost`,
  `127.0.0.1`, `::1`), so it cannot validate for any real hostname.
- They carry a deliberately long (100-year) validity so downstream
  integration runs never fail on expiry.

This README is intended to mark the directory for humans and secret scanners.
If your secret scanner flags these files, add this path to its allowlist.

Regenerate with `go run gen.go` in this directory (see `gen.go`).
The Go embedding lives in `../certs.go`.
