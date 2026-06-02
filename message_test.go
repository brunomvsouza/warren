package warren

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/codec"
)

// applyDefaults fills MessageID, Timestamp, and ContentType if not set.

func TestMessage_applyDefaults_setsMessageID(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	require.NoError(t, m.applyDefaults(c))
	assert.NotEmpty(t, m.MessageID)
}

func TestMessage_applyDefaults_messageIDIsUUIDv7(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	require.NoError(t, m.applyDefaults(c))
	parsed, err := uuid.Parse(m.MessageID)
	require.NoError(t, err, "MessageID must be a valid UUID")
	assert.Equal(t, uuid.Version(7), parsed.Version(), "MessageID must be UUID v7")
}

func TestMessage_applyDefaults_doesNotOverwriteMessageID(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{MessageID: "my-id"}
	require.NoError(t, m.applyDefaults(c))
	assert.Equal(t, "my-id", m.MessageID)
}

func TestMessage_applyDefaults_setsTimestamp(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	before := time.Now()
	require.NoError(t, m.applyDefaults(c))
	after := time.Now()
	assert.False(t, m.Timestamp.IsZero())
	assert.True(t, !m.Timestamp.Before(before) && !m.Timestamp.After(after))
}

func TestMessage_applyDefaults_doesNotOverwriteTimestamp(t *testing.T) {
	c := codec.NewJSON()
	fixed := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	m := Message[struct{}]{Timestamp: fixed}
	require.NoError(t, m.applyDefaults(c))
	assert.Equal(t, fixed, m.Timestamp)
}

func TestMessage_applyDefaults_setsContentTypeFromCodec(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	require.NoError(t, m.applyDefaults(c))
	assert.Equal(t, "application/json", m.ContentType)
}

func TestMessage_applyDefaults_doesNotOverwriteContentType(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{ContentType: "application/protobuf"}
	require.NoError(t, m.applyDefaults(c))
	assert.Equal(t, "application/protobuf", m.ContentType)
}

func TestMessage_applyDefaults_doesNotTouchContentEncoding(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	require.NoError(t, m.applyDefaults(c))
	assert.Empty(t, m.ContentEncoding)
}

func TestMessage_applyDefaults_customCodecContentType(t *testing.T) {
	c := &fakeCodec{contentType: "application/protobuf"}
	m := Message[struct{}]{}
	require.NoError(t, m.applyDefaults(c))
	assert.Equal(t, "application/protobuf", m.ContentType)
}

// DeliveryMode zero value is Persistent.

func TestMessage_DeliveryModePersistentIsZeroValue(t *testing.T) {
	m := Message[struct{}]{}
	assert.Equal(t, DeliveryModePersistent, m.DeliveryMode)
}

// Regression: DeliveryMode constant values must never change.
func TestDeliveryModeValues_neverChange(t *testing.T) {
	assert.Equal(t, DeliveryMode(0), DeliveryModePersistent, "reordering breaks zero-value contract")
	assert.Equal(t, DeliveryMode(1), DeliveryModeTransient)
}

// validateHeaders accepts supported AMQP field-table types.

func TestMessage_validateHeaders_happy(t *testing.T) {
	cases := []struct {
		name string
		v    any
	}{
		{"bool", true},
		{"int8", int8(1)},
		{"int16", int16(2)},
		{"int32", int32(3)},
		{"int64", int64(4)},
		{"uint8", uint8(5)},
		{"uint16", uint16(6)},
		{"uint32", uint32(7)},
		{"uint64", uint64(8)},
		{"float32", float32(1.0)},
		{"float64", float64(2.0)},
		{"string", "hello"},
		{"bytes", []byte("world")},
		{"time.Time", time.Now()},
		{"nil", nil},
		{"int auto-coerce", int(9)},
		{"uint auto-coerce", uint(10)},
		{"nested Headers", Headers{"k": "v"}},
		{"[]any", []any{1, "two"}},
		{"empty Headers", Headers{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := Message[struct{}]{Headers: Headers{"k": tc.v}}
			err := m.validateHeaders()
			assert.NoError(t, err)
		})
	}
}

func TestMessage_validateHeaders_emptyHeaders(t *testing.T) {
	m := Message[struct{}]{Headers: Headers{}}
	assert.NoError(t, m.validateHeaders())
}

func TestMessage_validateHeaders_rejectsUnsupportedType(t *testing.T) {
	cases := []struct {
		name string
		v    any
	}{
		{"chan", make(chan int)},
		{"func", func() {}},
		{"struct", struct{ X int }{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := Message[struct{}]{Headers: Headers{"k": tc.v}}
			err := m.validateHeaders()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidMessage)
		})
	}
}

func TestMessage_validateHeaders_rejectsInvalidElementInSlice(t *testing.T) {
	m := Message[struct{}]{Headers: Headers{"k": []any{1, make(chan int), "str"}}}
	err := m.validateHeaders()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMessage)
}

// LATER-02: int/uint are coerced to int64/uint64 in-place during validation.

func TestMessage_validateHeaders_coercesIntToInt64(t *testing.T) {
	h := Headers{"count": int(42)}
	m := Message[struct{}]{Headers: h}
	require.NoError(t, m.validateHeaders())
	// After validation the value must have been coerced to int64.
	got, ok := h["count"]
	require.True(t, ok)
	assert.IsType(t, int64(0), got, "int must be coerced to int64 during validation")
	assert.EqualValues(t, int64(42), got)
}

func TestMessage_validateHeaders_coercesUintToUint64(t *testing.T) {
	h := Headers{"count": uint(77)}
	m := Message[struct{}]{Headers: h}
	require.NoError(t, m.validateHeaders())
	got, ok := h["count"]
	require.True(t, ok)
	assert.IsType(t, uint64(0), got, "uint must be coerced to uint64 during validation")
	assert.EqualValues(t, uint64(77), got)
}

func TestMessage_validateHeaders_coercesIntInNestedHeaders(t *testing.T) {
	inner := Headers{"x": int(5)}
	h := Headers{"nested": inner}
	m := Message[struct{}]{Headers: h}
	require.NoError(t, m.validateHeaders())
	got, ok := inner["x"]
	require.True(t, ok)
	assert.IsType(t, int64(0), got, "int must be coerced to int64 in nested Headers")
}

func TestMessage_validateHeaders_coercesIntAndUintInSlice(t *testing.T) {
	// int and uint inside []any must be coerced in-place to int64/uint64,
	// matching the map-level coercion in validateHeadersDepth.
	s := []any{int(99), uint(7), "unchanged"}
	m := Message[struct{}]{Headers: Headers{"k": s}}
	require.NoError(t, m.validateHeaders())
	assert.IsType(t, int64(0), s[0], "int element must be coerced to int64")
	assert.EqualValues(t, int64(99), s[0])
	assert.IsType(t, uint64(0), s[1], "uint element must be coerced to uint64")
	assert.EqualValues(t, uint64(7), s[1])
	assert.Equal(t, "unchanged", s[2], "string element must not be modified")
}

// TestMessage_validateHeaders_coercesIntInHeadersInsideSlice verifies that
// int values nested inside a Headers map that is itself inside a []any element
// are also coerced in-place to int64. This exercises the recursive delegation
// in validateHeaderValue's default branch.
func TestMessage_validateHeaders_coercesIntInHeadersInsideSlice(t *testing.T) {
	inner := Headers{"x": int(5)}
	s := []any{inner}
	m := Message[struct{}]{Headers: Headers{"k": s}}
	require.NoError(t, m.validateHeaders())
	// inner["x"] must have been coerced to int64 in-place.
	got, ok := inner["x"]
	require.True(t, ok)
	assert.IsType(t, int64(0), got, "int in Headers inside []any must be coerced to int64")
	assert.EqualValues(t, int64(5), got)
}

func TestMessage_validateHeaders_rejectsExcessiveNesting(t *testing.T) {
	// Build a Headers nested maxHeaderDepth+2 levels deep to exceed the limit.
	deepest := Headers{"leaf": "value"}
	current := deepest
	for range maxHeaderDepth + 1 {
		current = Headers{"nested": current}
	}
	m := Message[struct{}]{Headers: current}
	err := m.validateHeaders()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMessage)
	assert.True(t, strings.Contains(err.Error(), "exceeds maximum depth"))
}

// fakeCodec is a minimal Codec stub for testing applyDefaults with non-JSON content types.
type fakeCodec struct {
	contentType string
}

func (f *fakeCodec) Encode(v any) ([]byte, error)    { return nil, nil }
func (f *fakeCodec) Decode(data []byte, v any) error { return nil }
func (f *fakeCodec) ContentType() string             { return f.contentType }

func TestMetricsTypeName(t *testing.T) {
	type sampleEvent struct{ ID int }
	assert.Equal(t, "sampleEvent", metricsTypeName[sampleEvent](), "named type → bare name")
	assert.Equal(t, "string", metricsTypeName[string](), "builtin named type")
	// Unnamed types have no reflect Name(); the helper falls back to String().
	assert.Equal(t, "[]uint8", metricsTypeName[[]byte](), "unnamed slice → String() fallback")
	assert.Equal(t, "map[string]int", metricsTypeName[map[string]int](), "unnamed map → String() fallback")
}

// TestMessageID_UUIDv7_NoPerCallEntropyAlloc is the Lens-09 (PC-09) allocation
// guard. The package init must call uuid.EnableRandPool() so the per-publish
// crypto/rand read is batched into a process-global pool. Without it, every
// uuid.NewV7() — and thus every applyDefaults on a Message with an empty
// MessageID — allocates one entropy buffer (measured at 1.00 alloc/op). With the
// pool active, the per-call entropy buffer is gone (0 alloc/op). This guard fails
// if the EnableRandPool call is ever removed. It is the T10-local guard
// coordinated with T148's combined hot-path AllocsPerRun guard.
func TestMessageID_UUIDv7_NoPerCallEntropyAlloc(t *testing.T) {
	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = uuid.NewV7()
	})
	assert.Zero(t, allocs,
		"uuid.NewV7 must not allocate a per-call entropy buffer; "+
			"package init must call uuid.EnableRandPool() (Lens-09 PC-09)")
}

// TestPublisher_encodeMsg_RejectsSubMillisecondExpiration asserts a non-zero
// Expiration shorter than 1ms is rejected client-side with ErrInvalidMessage
// (RMQ-26). The AMQP shortstr Expiration is serialised as integer milliseconds, so
// a sub-millisecond TTL rounds to "0", which the broker interprets as "expire
// immediately" — a silent footgun that would discard the message on arrival. Reject
// it at publish time rather than emit a TTL that means the opposite of the caller's
// intent. Mirrors the symmetric Delay guard.
func TestPublisher_encodeMsg_RejectsSubMillisecondExpiration(t *testing.T) {
	p := &Publisher[int]{codec: codec.NewJSON()}
	body := 7

	_, _, err := p.encodeMsg(Message[int]{Body: &body, Expiration: 500 * time.Microsecond})

	require.ErrorIs(t, err, ErrInvalidMessage)
	assert.Contains(t, err.Error(), "Expiration", "the error must name the offending field")
}

// TestPublisher_encodeMsg_AllowsOneMillisecondExpiration asserts the smallest TTL
// that survives the millisecond rounding (exactly 1ms) is accepted — the boundary
// is inclusive, so a 1ms expiry round-trips without tripping the sub-ms guard.
func TestPublisher_encodeMsg_AllowsOneMillisecondExpiration(t *testing.T) {
	p := &Publisher[int]{codec: codec.NewJSON()}
	body := 7

	_, _, err := p.encodeMsg(Message[int]{Body: &body, Expiration: time.Millisecond})

	require.NoError(t, err)
}

// TestPublisher_encodeMsg_AllowsZeroExpiration asserts a zero Expiration (no TTL)
// passes — only a non-zero duration that rounds to "0" is the footgun the guard
// targets; the explicit zero value means "no per-message TTL" and is left alone.
func TestPublisher_encodeMsg_AllowsZeroExpiration(t *testing.T) {
	p := &Publisher[int]{codec: codec.NewJSON()}
	body := 7

	_, _, err := p.encodeMsg(Message[int]{Body: &body, Expiration: 0})

	require.NoError(t, err)
}

// TestPublisher_encodeMsg_RejectsNegativeExpiration asserts a negative Expiration
// of ≥1ms magnitude is rejected with ErrInvalidMessage rather than silently
// published with no TTL at all. The sub-ms guard alone misses it (a -2ms duration
// has .Milliseconds() == -2, not 0, so it would slip past buildPublishing's
// `Expiration > 0` gate), so the guard rejects any non-zero, non-positive
// Expiration — symmetric with the Delay guard.
func TestPublisher_encodeMsg_RejectsNegativeExpiration(t *testing.T) {
	p := &Publisher[int]{codec: codec.NewJSON()}
	body := 7

	_, _, err := p.encodeMsg(Message[int]{Body: &body, Expiration: -2 * time.Millisecond})

	require.ErrorIs(t, err, ErrInvalidMessage)
	assert.Contains(t, err.Error(), "Expiration", "the error must name the offending field")
}
