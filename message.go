package warren

import (
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"

	"github.com/brunomvsouza/warren/codec"
)

// init enables the google/uuid process-global random pool (Lens-09 PC-09).
//
// MessageID defaults to a UUID v7 (see applyDefaults), generated on every publish
// whose Message leaves MessageID empty. Without the pool, each uuid.NewV7 reads
// entropy from crypto/rand and allocates a per-call buffer (1 alloc/op). The pool
// batches those reads into a single process-global buffer refilled in bulk,
// eliminating the per-publish entropy allocation on the hot path.
//
// Note the cost this does NOT remove: google/uuid takes a process-global timeMu
// lock inside every NewV7 to keep the embedded timestamp monotonic. At the
// billions/day bar that lock is a process-wide serialization point shared by
// every publisher goroutine. MessageID is load-bearing for the at-least-once
// dedupe contract (consumers dedupe by MessageID — see SPEC §6.2.1), so this
// per-publish UUID generation cannot be skipped to avoid the lock.
func init() {
	uuid.EnableRandPool()
}

// Message is a typed AMQP message. M is the payload type; Body holds a pointer
// to the decoded or to-be-encoded value.
//
// Zero-value defaults applied by applyDefaults:
//   - MessageID is a UUID v7 (RFC 9562) when left empty.
//   - Timestamp is time.Now() when zero.
//   - ContentType is set from the codec when empty.
//   - DeliveryMode zero value is DeliveryModePersistent (durable).
type Message[M any] struct {
	Body *M

	// basic.properties — one-to-one mapping with AMQP 0-9-1.

	// MessageID identifies the message for at-least-once dedupe. Left empty, it
	// defaults to a UUID v7 (RFC 9562) generated at publish time. It is
	// load-bearing: PublishRetry, the reconnect barrier, and confirm timeouts can
	// all redeliver, so consumers MUST dedupe by MessageID (SPEC §6.2.1). Do not
	// disable it to save the per-publish UUID generation.
	MessageID     string
	CorrelationID string
	ReplyTo       string
	Type          string
	AppID         string
	// UserID, when set, must equal the connection's authenticated user: RabbitMQ
	// closes the channel with a 406 (PRECONDITION_FAILED) if it does not. To turn
	// that footgun into a local error, Publish validates UserID client-side — a
	// non-empty value that differs from the authenticated user returns
	// ErrInvalidMessage without writing the publish frame. Leave it empty, or use
	// the publisher's StampUserID() option to stamp the authenticated user for you.
	UserID string
	// ContentType is the MIME type of the body (e.g. "application/json").
	// Default: set from codec.ContentType() when empty.
	// See ContentEncoding for the transfer-encoding counterpart.
	ContentType string
	// ContentEncoding is the transfer encoding applied on top of the codec output
	// (e.g. "gzip", "deflate"). Default: "" (identity). Set only when you wrap the
	// codec's output with a compressor or similar transform.
	ContentEncoding string
	// Headers is the AMQP field-table. Supported value types:
	// bool, int8/16/32/64, uint8/16/32/64, float32/64, string, []byte,
	// time.Time, nil, Headers (nested), []any.
	// int and uint literals auto-coerce to int64/uint64.
	// Any other Go type returns ErrInvalidMessage at publish time.
	Headers Headers
	// Priority is the AMQP basic.properties.priority octet (wire range 0–255).
	// RabbitMQ priority queues use 0–9 by convention; values above the queue's
	// x-max-priority are silently clamped by the broker. Priority on a
	// non-priority queue has no effect.
	Priority  uint8
	Timestamp time.Time
	// Expiration is the per-message TTL. The publisher serialises it as ASCII
	// milliseconds in the AMQP shortstr wire format. Sub-millisecond durations
	// round to 0; the broker interprets "0" as "expire immediately".
	Expiration time.Duration

	// DeliveryMode controls AMQP delivery persistence. The zero value is
	// DeliveryModePersistent so Message[M]{} defaults to durable delivery.
	DeliveryMode DeliveryMode

	// RabbitMQ extensions.
	// Delay requires the rabbitmq_delayed_message_exchange plugin.
	Delay time.Duration
}

// applyDefaults fills MessageID, Timestamp, and ContentType if they are not set.
// ContentEncoding is intentionally left untouched.
// Returns ErrInvalidMessage if a UUID v7 cannot be generated (OS entropy failure).
func (m *Message[M]) applyDefaults(c codec.Codec) error {
	if m.MessageID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("%w: failed to generate message ID: %w", ErrInvalidMessage, err)
		}
		m.MessageID = id.String()
	}
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}
	if m.ContentType == "" {
		m.ContentType = c.ContentType()
	}
	return nil
}

// validateHeaders checks that every value in m.Headers is an AMQP field-table
// compatible type. Returns ErrInvalidMessage on the first unsupported value.
func (m *Message[M]) validateHeaders() error {
	return validateHeaders(m.Headers)
}

const maxHeaderDepth = 10

func validateHeaders(h Headers) error {
	return validateHeadersDepth(h, 0)
}

func validateHeadersDepth(h Headers, depth int) error {
	if depth > maxHeaderDepth {
		return fmt.Errorf("%w: header nesting exceeds maximum depth %d", ErrInvalidMessage, maxHeaderDepth)
	}
	for k, v := range h {
		// Coerce platform-width integers to their fixed-width AMQP equivalents
		// in-place so that amqp091-go's table serializer never encounters
		// unsupported types. int is encoded as int32 by amqp091-go, which would
		// silently truncate values > math.MaxInt32; coercing to int64 preserves
		// the full range. uint is not handled by amqp091-go at all and would
		// return ErrFieldType at publish time without this coercion.
		switch typed := v.(type) {
		case int:
			h[k] = int64(typed)
			continue
		case uint:
			h[k] = uint64(typed)
			continue
		}
		if err := validateHeaderValue(k, v, depth); err != nil {
			return err
		}
	}
	return nil
}

func validateHeaderValue(key string, v any, depth int) error {
	switch typed := v.(type) {
	case bool,
		int8, int16, int32, int64,
		uint8, uint16, uint32, uint64,
		float32, float64,
		string, []byte,
		time.Time,
		nil:
		return nil
	case Headers:
		return validateHeadersDepth(typed, depth+1)
	case []any:
		for i, elem := range typed {
			// Coerce int/uint elements in-place, matching the map-level coercion
			// in validateHeadersDepth, so that []any{int(1)} is also normalized.
			switch e := elem.(type) {
			case int:
				typed[i] = int64(e)
			case uint:
				typed[i] = uint64(e)
			default:
				if err := validateHeaderValue(fmt.Sprintf("%s[%d]", key, i), elem, depth); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: header %q has unsupported type %s", ErrInvalidMessage, key, reflect.TypeOf(v))
	}
}

// metricsTypeName returns a stable label value identifying the message type M,
// used for the opt-in message_type metrics label. It prefers the bare type name
// (e.g. "OrderPlaced") and falls back to the fully-qualified string for unnamed
// types. Computed once per Publisher/Consumer at build time, never per message.
func metricsTypeName[M any]() string {
	t := reflect.TypeFor[M]()
	if name := t.Name(); name != "" {
		return name
	}
	return t.String()
}
