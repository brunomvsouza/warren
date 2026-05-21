package amqp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/amqp/codec"
)

// applyDefaults fills MessageID, Timestamp, and ContentType if not set.

func TestMessage_applyDefaults_setsMessageID(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	m.applyDefaults(c)
	assert.NotEmpty(t, m.MessageID)
}

func TestMessage_applyDefaults_doesNotOverwriteMessageID(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{MessageID: "my-id"}
	m.applyDefaults(c)
	assert.Equal(t, "my-id", m.MessageID)
}

func TestMessage_applyDefaults_setsTimestamp(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	before := time.Now()
	m.applyDefaults(c)
	after := time.Now()
	assert.False(t, m.Timestamp.IsZero())
	assert.True(t, !m.Timestamp.Before(before) && !m.Timestamp.After(after))
}

func TestMessage_applyDefaults_doesNotOverwriteTimestamp(t *testing.T) {
	c := codec.NewJSON()
	fixed := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	m := Message[struct{}]{Timestamp: fixed}
	m.applyDefaults(c)
	assert.Equal(t, fixed, m.Timestamp)
}

func TestMessage_applyDefaults_setsContentTypeFromCodec(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	m.applyDefaults(c)
	assert.Equal(t, "application/json", m.ContentType)
}

func TestMessage_applyDefaults_doesNotOverwriteContentType(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{ContentType: "application/protobuf"}
	m.applyDefaults(c)
	assert.Equal(t, "application/protobuf", m.ContentType)
}

func TestMessage_applyDefaults_doesNotTouchContentEncoding(t *testing.T) {
	c := codec.NewJSON()
	m := Message[struct{}]{}
	m.applyDefaults(c)
	assert.Empty(t, m.ContentEncoding)
}

// DeliveryMode zero value is Persistent.

func TestMessage_DeliveryModePersistentIsZeroValue(t *testing.T) {
	m := Message[struct{}]{}
	assert.Equal(t, DeliveryModePersistent, m.DeliveryMode)
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := Message[struct{}]{Headers: Headers{"k": tc.v}}
			err := m.validateHeaders()
			assert.NoError(t, err)
		})
	}
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
