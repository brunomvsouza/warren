package amqptest

import (
	_ "embed"
	"os"
	"path/filepath"
)

// Pre-generated TLS fixtures embedded from certs/. Regenerate with
// `go run gen.go` in certs/ (see certs/gen.go). They are TEST credentials with
// no security value and a deliberately long (100-year) validity so downstream
// integration suites never fail on expiry.

//go:embed certs/ca.pem
var caCertPEM []byte

//go:embed certs/server.pem
var serverCertPEM []byte

//go:embed certs/server.key
var serverKeyPEM []byte

//go:embed certs/client.pem
var clientCertPEM []byte

//go:embed certs/client.key
var clientKeyPEM []byte

// CACertPEM returns the PEM-encoded test CA certificate that signs the server
// and client leaves. An amqps:// client adds it to its tls.Config.RootCAs to
// verify a broker started by [NewRabbitMQ] with TLS enabled.
func CACertPEM() []byte { return clonePEM(caCertPEM) }

// ServerCertPEM and ServerKeyPEM return the broker's TLS server certificate and
// private key (CN/SAN cover localhost, 127.0.0.1 and ::1). [NewRabbitMQ] mounts
// them into the container; they are exported for callers that build their own
// broker fixture.
func ServerCertPEM() []byte { return clonePEM(serverCertPEM) }

// ServerKeyPEM returns the broker's TLS server private key. See [ServerCertPEM].
func ServerKeyPEM() []byte { return clonePEM(serverKeyPEM) }

// ClientCertPEM and ClientKeyPEM return the TLS client certificate (CN "guest")
// and key for amqps:// + SASL EXTERNAL tests (a broker with
// rabbitmq_auth_mechanism_ssl and ssl_cert_login_from=common_name maps it to
// the built-in "guest" user).
func ClientCertPEM() []byte { return clonePEM(clientCertPEM) }

// ClientKeyPEM returns the TLS client private key. See [ClientCertPEM].
func ClientKeyPEM() []byte { return clonePEM(clientKeyPEM) }

func clonePEM(b []byte) []byte { return append([]byte(nil), b...) }

// writeServerTLSFiles writes the CA cert, server cert and server key into dir
// and returns their paths, in the order rabbitmq.SSLSettings expects
// (caCertFile, certFile, keyFile). The module reads these host files to mount
// them and configure the broker's TLS listener.
func writeServerTLSFiles(dir string) (caPath, certPath, keyPath string, err error) {
	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "server.pem")
	keyPath = filepath.Join(dir, "server.key")
	for path, data := range map[string][]byte{
		caPath:   caCertPEM,
		certPath: serverCertPEM,
		keyPath:  serverKeyPEM,
	} {
		if err = os.WriteFile(path, data, 0o600); err != nil {
			return "", "", "", err
		}
	}
	return caPath, certPath, keyPath, nil
}
