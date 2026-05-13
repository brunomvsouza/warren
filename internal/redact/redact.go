// Package redact is the mandatory credential-redaction choke-point for the
// amqp library. Every string handed to logs, metric labels, span attributes,
// or error messages that may contain an AMQP URI passes through URI before
// emission, satisfying the SPEC §8 "Always: redact credentials" rule.
package redact

import "regexp"

// amqpURIRe matches the scheme + authority prefix of an AMQP URI up to and
// including the "@" separator, capturing only the userinfo portion for
// replacement. The pattern handles amqp:// and amqps://, percent-encoded
// passwords, and IPv6 literal hosts (no literal "@" inside brackets).
var amqpURIRe = regexp.MustCompile(`(amqps?://)([^@/?#]*)@`)

// URI replaces the userinfo component (user / user:password) of any AMQP URI
// embedded in s with "***", preserving scheme, host, port, vhost, and query
// string. Both amqp:// and amqps:// schemes are handled. Percent-encoded
// passwords (e.g. "p%40ss" for "p@ss") are handled correctly because the
// encoded form contains no literal "@". Multiple URIs in one string are all
// redacted. Malformed or non-AMQP URIs (e.g. http://) are returned unchanged.
func URI(s string) string {
	return amqpURIRe.ReplaceAllString(s, "${1}***@")
}

// URIs redacts credentials in each element and returns a new slice.
// The input slice is not modified.
func URIs(uris []string) []string {
	if len(uris) == 0 {
		return uris
	}
	out := make([]string, len(uris))
	for i, u := range uris {
		out[i] = URI(u)
	}
	return out
}

// Error returns a new error whose message is the redacted form of err.Error().
// The original error chain is preserved so errors.Is and errors.As still work.
func Error(err error) error {
	if err == nil {
		return nil
	}
	return &redactedError{msg: URI(err.Error()), cause: err}
}

type redactedError struct {
	msg   string
	cause error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.cause }
