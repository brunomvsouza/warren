//go:build ignore

// Command gen regenerates the warren amqptest TLS fixture certificates:
// ca.pem, server.pem, server.key, client.pem, client.key.
//
// Run it from this directory:
//
//	go run gen.go
//
// The certificates are committed to the repository (the import path embeds
// them with //go:embed) so a normal `go test` never regenerates them; run this
// only to rotate the fixtures. They are deliberately long-lived (100 years) so
// downstream integration suites never fail on expiry.
//
// Layout:
//   - A fresh self-signed CA (warren amqptest test CA) signs both leaves.
//   - server.pem is a TLS server certificate for "localhost"/127.0.0.1/::1
//     (testcontainers maps the broker's TLS port onto the loopback host), so an
//     amqps:// client that verifies the hostname against amqptest's CA succeeds.
//   - client.pem is a TLS client certificate with CN "guest"; a broker running
//     rabbitmq_auth_mechanism_ssl with ssl_cert_login_from=common_name maps it
//     to the built-in "guest" user, which is what T34b's SASL EXTERNAL test
//     relies on.
//
// These are TEST credentials with no security value; never use them in production.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"os"
	"time"
)

func main() {
	notBefore := time.Now().Add(-time.Hour)
	notAfter := notBefore.AddDate(100, 0, 0)

	// Certificate authority.
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	must(err)
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "warren amqptest test CA", Organization: []string{"warren"}},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	must(err)
	caCert, err := x509.ParseCertificate(caDER)
	must(err)
	writePEM("ca.pem", "CERTIFICATE", caDER)

	// Server leaf (TLS server auth, loopback SANs).
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	must(err)
	serverTmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "localhost", Organization: []string{"warren"}},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTmpl, caCert, &serverKey.PublicKey, caKey)
	must(err)
	writePEM("server.pem", "CERTIFICATE", serverDER)
	writeKey("server.key", serverKey)

	// Client leaf (TLS client auth, CN=guest for SASL EXTERNAL mapping).
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	must(err)
	clientTmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "guest", Organization: []string{"warren"}},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caCert, &clientKey.PublicKey, caKey)
	must(err)
	writePEM("client.pem", "CERTIFICATE", clientDER)
	writeKey("client.key", clientKey)

	log.Println("wrote ca.pem, server.pem, server.key, client.pem, client.key")
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	must(err)
	return n
}

func writePEM(path, blockType string, der []byte) {
	f, err := os.Create(path)
	must(err)
	defer f.Close()
	must(pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}))
}

func writeKey(path string, key *rsa.PrivateKey) {
	f, err := os.Create(path)
	must(err)
	defer f.Close()
	must(pem.Encode(f, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
