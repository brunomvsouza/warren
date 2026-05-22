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

func TestMessage_validateHeaders_rejectsExcessiveNesting(t *testing.T) {
	// Build a Headers nested maxHeaderDepth+2 levels deep to exceed the limit.
	deepest := Headers{"leaf": "value"}
	current := deepest
	for i := 0; i < maxHeaderDepth+1; i++ {
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
