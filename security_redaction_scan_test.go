package warren_test

// Unit coverage for the credential-leak scanner used by the T45b security
// regression integration test (security_redaction_integration_test.go). The
// scanner lives in an un-tagged test file so its logic is verifiable on the fast
// unit lane — without a broker — and reused verbatim by the integration test.

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// scanForSecrets returns every credential leak found in text: an exact match of
// any known secret substring, plus any AMQP URI whose userinfo is neither empty
// nor the redacted "***" sentinel (which catches leaks even when the password is
// generic, and future code that emits a new un-redacted URI).
func scanForSecrets(text string, secrets []string) []string {
	var found []string
	for _, s := range secrets {
		if s != "" && strings.Contains(text, s) {
			found = append(found, "secret-substring:"+s)
		}
	}
	for _, m := range amqpUserinfoRe.FindAllStringSubmatch(text, -1) {
		if userinfo := m[1]; userinfo != "" && userinfo != "***" {
			found = append(found, "unredacted-userinfo:"+userinfo)
		}
	}
	return found
}

// amqpUserinfoRe captures the userinfo segment of any amqp(s):// URI.
var amqpUserinfoRe = regexp.MustCompile(`amqps?://([^@/?#]*)@`)

func TestScanForSecrets_flagsExactSecretSubstring(t *testing.T) {
	secrets := []string{"s3cret-pass", "user:s3cret-pass"}
	leaks := scanForSecrets("oops password is s3cret-pass in the log", secrets)
	assert.Contains(t, leaks, "secret-substring:s3cret-pass")
}

func TestScanForSecrets_flagsUnredactedAMQPUserinfo(t *testing.T) {
	// A leak even when the password is generic: the userinfo is not "***".
	leaks := scanForSecrets("connecting to amqp://guest:guest@broker:5672/", nil)
	assert.Equal(t, []string{"unredacted-userinfo:guest:guest"}, leaks)
}

func TestScanForSecrets_flagsUserOnlyUserinfo(t *testing.T) {
	leaks := scanForSecrets("amqps://admin@host:5671/v", nil)
	assert.Equal(t, []string{"unredacted-userinfo:admin"}, leaks)
}

func TestScanForSecrets_cleanWhenRedacted(t *testing.T) {
	// The redacted form must NOT be flagged: userinfo is exactly "***".
	text := "warren: connecting to amqp://***@broker:5672/prod (reconnecting)"
	leaks := scanForSecrets(text, []string{"s3cret-pass", "guest:guest"})
	assert.Empty(t, leaks)
}

func TestScanForSecrets_cleanWhenNoURIAtAll(t *testing.T) {
	leaks := scanForSecrets("publisher_retry_total role=publisher outcome=ok", []string{"s3cret-pass"})
	assert.Empty(t, leaks)
}

func TestScanForSecrets_emptySecretsAreIgnored(t *testing.T) {
	// An empty secret must never match (it would match every string otherwise).
	leaks := scanForSecrets("any text", []string{""})
	assert.Empty(t, leaks)
}

func TestScanForSecrets_multipleURIsAllScanned(t *testing.T) {
	// One redacted, one leaking: only the leaking one is reported.
	text := "node1 amqp://***@h1/ node2 amqp://app:hunter2@h2/"
	leaks := scanForSecrets(text, nil)
	assert.Equal(t, []string{"unredacted-userinfo:app:hunter2"}, leaks)
}
