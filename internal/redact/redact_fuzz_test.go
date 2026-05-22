package redact_test

import (
	"regexp"
	"testing"

	"github.com/brunomvsouza/warren/internal/redact"
)

// hasCredentials matches any URI-like string that still contains user:pass@ after redaction.
var hasCredentials = regexp.MustCompile(`://[^@]*:[^@]*@`)

func FuzzRedactURI(f *testing.F) {
	// Seed corpus: representative shapes.
	f.Add("amqp://user:password@host:5672/vhost")
	f.Add("amqps://user:p%40ss@host:5671/v?heartbeat=10")
	f.Add("amqp://user@host")
	f.Add("amqp://host")
	f.Add("amqp://")
	f.Add("")
	f.Add("not a uri")
	f.Add("amqp://u:p@h amqps://u2:p2@h2")
	f.Add("amqp://user:p@ss@host/v") // double @ (malformed)
	f.Add("amqp://[::1]:5672")
	f.Add("amqp://user:pass@[::1]:5672/v")
	f.Add("http://user:pass@host/path") // non-AMQP scheme
	f.Add("amqp://user:p%00ss@host")    // null byte in percent-encoded pass

	f.Fuzz(func(t *testing.T, s string) {
		// Must never panic regardless of input.
		result := redact.URI(s)

		// If the result still contains user:pass@ pattern, the input must
		// have already contained it (i.e., it was malformed and the redactor
		// couldn't fix it) — the redactor must not INTRODUCE credentials.
		if hasCredentials.MatchString(result) {
			if !hasCredentials.MatchString(s) {
				t.Errorf("redact.URI introduced credentials: input=%q output=%q", s, result)
			}
		}
	})
}
