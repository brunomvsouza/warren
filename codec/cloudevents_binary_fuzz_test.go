package codec_test

import (
	"testing"

	"github.com/brunomvsouza/warren/codec"
)

// FuzzCodecCloudEventsBinary feeds arbitrary body + cloudEvents: header values
// into the binary decoder. DecodeWithHeaders must never panic — an error is
// acceptable — and re-encoding a successfully decoded event must not panic.
func FuzzCodecCloudEventsBinary(f *testing.F) {
	hc, ok := codec.NewCloudEventsBinary().(codec.HeaderCodec)
	if !ok {
		f.Fatal("binary codec must implement codec.HeaderCodec")
	}

	f.Add([]byte(`{"k":1}`), "1.0", "id-1", "/svc", "2026-05-28T12:00:00Z", "tenant", "acme")
	f.Add([]byte{}, "", "", "", "", "", "")
	f.Add([]byte("data"), "1.0", "x", "s", "not-a-time", "id", "shadow-attempt")
	f.Add([]byte{0xff, 0xfe, 0x00}, "0.3", "ï", "🤖", "2026-13-99T99:99:99Z", "k", "v")
	f.Add([]byte("x"), "garbage-version", "i", "s", "", "ext", "val")

	f.Fuzz(func(t *testing.T, body []byte, specversion, id, source, ceTime, extKey, extVal string) {
		headers := map[string]any{
			"cloudEvents:id":     id,
			"cloudEvents:source": source,
			"cloudEvents:type":   "t",
			// []byte form exercises the amqp091 long/short-string coercion path.
			"cloudEvents:subject": []byte("subj"),
		}
		if specversion != "" {
			headers["cloudEvents:specversion"] = specversion
		}
		if ceTime != "" {
			headers["cloudEvents:time"] = ceTime
		}
		if extKey != "" {
			headers["cloudEvents:"+extKey] = extVal
		}

		var ev codec.CloudEvent
		if err := hc.DecodeWithHeaders(body, headers, "application/json", &ev); err == nil {
			// Re-encoding a successfully decoded event must also never panic.
			_, _, _, _ = hc.EncodeWithHeaders(&ev)
		}
	})
}
