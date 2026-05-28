package codec

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// CloudEvent is the payload type for the CloudEvents codecs. It models a
// CloudEvents v1.0 event (https://github.com/cloudevents/spec). Use it as the
// message type M with NewCloudEventsStructured or NewCloudEventsBinary.
//
// Data holds the raw event payload bytes. In structured mode JSON data is
// inlined under the "data" member and any other data is base64-encoded under
// "data_base64". In binary mode Data is the AMQP message body and the context
// attributes travel as ce-* headers.
type CloudEvent struct {
	// Required context attributes.
	ID          string
	Source      string
	SpecVersion string
	Type        string

	// Optional context attributes.
	DataContentType string
	DataSchema      string
	Subject         string
	Time            time.Time

	// Data is the raw event payload.
	Data []byte

	// Extensions are non-standard context attributes. Values are strings: in
	// binary mode each maps to a ce-<name> header, in structured mode to a
	// top-level JSON member. Non-string structured extensions are read as their
	// string form.
	Extensions map[string]string
}

const ceDefaultSpecVersion = "1.0"

// asCloudEvent accepts *CloudEvent or CloudEvent (the publisher passes *M).
func asCloudEvent(v any) (*CloudEvent, error) {
	switch e := v.(type) {
	case *CloudEvent:
		if e == nil {
			return nil, fmt.Errorf("%w: Encode requires a non-nil *codec.CloudEvent", ErrInvalidMessage)
		}
		return e, nil
	case CloudEvent:
		return &e, nil
	default:
		return nil, fmt.Errorf("%w: value of type %T is not a codec.CloudEvent", ErrInvalidMessage, v)
	}
}

// asCloudEventDest requires a settable *CloudEvent for Decode.
func asCloudEventDest(v any) (*CloudEvent, error) {
	e, ok := v.(*CloudEvent)
	if !ok || e == nil {
		return nil, fmt.Errorf("%w: Decode requires a non-nil *codec.CloudEvent destination", ErrInvalidMessage)
	}
	return e, nil
}

// isJSONDataContentType reports whether data with this content type is itself
// JSON and may be inlined under the "data" member. Empty defaults to JSON.
func isJSONDataContentType(ct string) bool {
	if ct == "" {
		return true
	}
	ct = strings.ToLower(ct)
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct == "application/json" || strings.HasSuffix(ct, "+json")
}

func (ev *CloudEvent) specVersionOrDefault() string {
	if ev.SpecVersion == "" {
		return ceDefaultSpecVersion
	}
	return ev.SpecVersion
}

// ceStructuredCodec encodes the full CloudEvent JSON envelope into the body.
type ceStructuredCodec struct{}

// NewCloudEventsStructured returns a codec that serialises a codec.CloudEvent as
// a full CloudEvents JSON envelope in the message body. ContentType is
// "application/cloudevents+json".
//
// JSON data (datacontenttype application/json, a +json suffix, or unset) is
// inlined under the "data" member; any other data is base64-encoded under
// "data_base64", per the CloudEvents JSON format.
func NewCloudEventsStructured() Codec {
	return &ceStructuredCodec{}
}

func (c *ceStructuredCodec) ContentType() string { return "application/cloudevents+json" }

func (c *ceStructuredCodec) Encode(v any) ([]byte, error) {
	ev, err := asCloudEvent(v)
	if err != nil {
		return nil, err
	}
	m, err := ev.toStructuredMap()
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return out, nil
}

func (c *ceStructuredCodec) Decode(data []byte, v any) error {
	ev, err := asCloudEventDest(v)
	if err != nil {
		return err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return ev.fromStructuredMap(raw)
}

// toStructuredMap builds the JSON envelope as a map so extensions can be merged
// as top-level members alongside the standard attributes.
func (ev *CloudEvent) toStructuredMap() (map[string]any, error) {
	m := make(map[string]any, 8+len(ev.Extensions))

	// Extensions first so a standard attribute can never be shadowed.
	for k, val := range ev.Extensions {
		m[k] = val
	}

	m["specversion"] = ev.specVersionOrDefault()
	m["id"] = ev.ID
	m["source"] = ev.Source
	m["type"] = ev.Type
	if ev.DataContentType != "" {
		m["datacontenttype"] = ev.DataContentType
	}
	if ev.DataSchema != "" {
		m["dataschema"] = ev.DataSchema
	}
	if ev.Subject != "" {
		m["subject"] = ev.Subject
	}
	if !ev.Time.IsZero() {
		m["time"] = ev.Time.UTC().Format(time.RFC3339Nano)
	}
	if len(ev.Data) > 0 {
		if isJSONDataContentType(ev.DataContentType) {
			if !json.Valid(ev.Data) {
				return nil, fmt.Errorf("%w: data is not valid JSON for content-type %q", ErrInvalidMessage, ev.DataContentType)
			}
			m["data"] = json.RawMessage(ev.Data)
		} else {
			m["data_base64"] = base64.StdEncoding.EncodeToString(ev.Data)
		}
	}
	return m, nil
}

func (ev *CloudEvent) fromStructuredMap(raw map[string]json.RawMessage) error {
	*ev = CloudEvent{}

	_, hasData := raw["data"]
	_, hasData64 := raw["data_base64"]
	if hasData && hasData64 {
		return fmt.Errorf("%w: envelope carries both data and data_base64", ErrInvalidMessage)
	}

	for k, rv := range raw {
		switch k {
		case "specversion":
			if err := decodeJSONString(k, rv, &ev.SpecVersion); err != nil {
				return err
			}
		case "id":
			if err := decodeJSONString(k, rv, &ev.ID); err != nil {
				return err
			}
		case "source":
			if err := decodeJSONString(k, rv, &ev.Source); err != nil {
				return err
			}
		case "type":
			if err := decodeJSONString(k, rv, &ev.Type); err != nil {
				return err
			}
		case "datacontenttype":
			if err := decodeJSONString(k, rv, &ev.DataContentType); err != nil {
				return err
			}
		case "dataschema":
			if err := decodeJSONString(k, rv, &ev.DataSchema); err != nil {
				return err
			}
		case "subject":
			if err := decodeJSONString(k, rv, &ev.Subject); err != nil {
				return err
			}
		case "time":
			var s string
			if err := decodeJSONString(k, rv, &s); err != nil {
				return err
			}
			ts, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return fmt.Errorf("%w: invalid time %q: %w", ErrInvalidMessage, s, err)
			}
			ev.Time = ts
		case "data":
			ev.Data = append([]byte(nil), rv...)
		case "data_base64":
			var s string
			if err := decodeJSONString(k, rv, &s); err != nil {
				return err
			}
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return fmt.Errorf("%w: invalid data_base64: %w", ErrInvalidMessage, err)
			}
			ev.Data = b
		default:
			if ev.Extensions == nil {
				ev.Extensions = make(map[string]string)
			}
			var s string
			if err := json.Unmarshal(rv, &s); err != nil {
				// Non-string extension: keep its raw JSON form.
				s = string(rv)
			}
			ev.Extensions[k] = s
		}
	}
	return nil
}

func decodeJSONString(key string, rv json.RawMessage, dst *string) error {
	if err := json.Unmarshal(rv, dst); err != nil {
		return fmt.Errorf("%w: attribute %q must be a JSON string: %w", ErrInvalidMessage, key, err)
	}
	return nil
}

// ceHeaderPrefix is the AMQP-header namespace for CloudEvents binary mode.
const ceHeaderPrefix = "ce-"

// ceStandardHeaders are the ce-* headers that map to typed CloudEvent fields
// rather than to Extensions.
var ceStandardHeaders = map[string]struct{}{
	"ce-specversion":     {},
	"ce-id":              {},
	"ce-source":          {},
	"ce-type":            {},
	"ce-datacontenttype": {},
	"ce-dataschema":      {},
	"ce-subject":         {},
	"ce-time":            {},
}

// ceBinaryCodec implements CloudEvents binary content mode: the event data is
// the AMQP body and the context attributes are ce-* headers.
type ceBinaryCodec struct{}

// NewCloudEventsBinary returns a codec for CloudEvents binary content mode. The
// event data is carried as the AMQP message body and the context attributes are
// mapped to AMQP headers prefixed "ce-" (ce-id, ce-source, ce-type,
// ce-specversion, and the optional ce-subject, ce-time, ce-datacontenttype,
// ce-dataschema, plus ce-<extension>).
//
// It implements HeaderCodec and is meant to be used through the library's
// publisher and consumer, which route the headers automatically. Its plain
// Encode/Decode reject use (with ErrInvalidMessage) so the ce-* attributes can
// never be silently dropped by a caller that bypasses the header-aware path.
//
// ContentType returns "" because the event's datacontenttype travels in the
// ce-datacontenttype header, not the AMQP content-type property.
func NewCloudEventsBinary() Codec {
	return &ceBinaryCodec{}
}

func (c *ceBinaryCodec) ContentType() string { return "" }

func (c *ceBinaryCodec) Encode(any) ([]byte, error) {
	return nil, fmt.Errorf("%w: CloudEvents binary mode requires a header-aware publisher; the ce-* attributes cannot be carried by Encode alone", ErrInvalidMessage)
}

func (c *ceBinaryCodec) Decode([]byte, any) error {
	return fmt.Errorf("%w: CloudEvents binary mode requires a header-aware consumer; the ce-* attributes cannot be read by Decode alone", ErrInvalidMessage)
}

func (c *ceBinaryCodec) EncodeWithHeaders(v any) ([]byte, map[string]any, error) {
	ev, err := asCloudEvent(v)
	if err != nil {
		return nil, nil, err
	}

	headers := make(map[string]any, 8+len(ev.Extensions))

	// Extensions first so a standard attribute can never be shadowed.
	for k, val := range ev.Extensions {
		headers[ceHeaderPrefix+k] = val
	}

	headers["ce-specversion"] = ev.specVersionOrDefault()
	headers["ce-id"] = ev.ID
	headers["ce-source"] = ev.Source
	headers["ce-type"] = ev.Type
	if ev.DataContentType != "" {
		headers["ce-datacontenttype"] = ev.DataContentType
	}
	if ev.DataSchema != "" {
		headers["ce-dataschema"] = ev.DataSchema
	}
	if ev.Subject != "" {
		headers["ce-subject"] = ev.Subject
	}
	if !ev.Time.IsZero() {
		headers["ce-time"] = ev.Time.UTC().Format(time.RFC3339Nano)
	}

	return ev.Data, headers, nil
}

func (c *ceBinaryCodec) DecodeWithHeaders(body []byte, headers map[string]any, v any) error {
	ev, err := asCloudEventDest(v)
	if err != nil {
		return err
	}
	*ev = CloudEvent{}

	// specversion presence is what distinguishes a binary CloudEvent from any
	// other message: a structured envelope (no ce-* headers) fails here.
	specVersion, ok := ceHeaderString(headers, "ce-specversion")
	if !ok {
		return fmt.Errorf("%w: missing ce-specversion header; not a binary CloudEvent", ErrInvalidMessage)
	}
	ev.SpecVersion = specVersion

	if s, ok := ceHeaderString(headers, "ce-id"); ok {
		ev.ID = s
	}
	if s, ok := ceHeaderString(headers, "ce-source"); ok {
		ev.Source = s
	}
	if s, ok := ceHeaderString(headers, "ce-type"); ok {
		ev.Type = s
	}
	if s, ok := ceHeaderString(headers, "ce-datacontenttype"); ok {
		ev.DataContentType = s
	}
	if s, ok := ceHeaderString(headers, "ce-dataschema"); ok {
		ev.DataSchema = s
	}
	if s, ok := ceHeaderString(headers, "ce-subject"); ok {
		ev.Subject = s
	}
	if s, ok := ceHeaderString(headers, "ce-time"); ok {
		ts, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return fmt.Errorf("%w: invalid ce-time %q: %w", ErrInvalidMessage, s, err)
		}
		ev.Time = ts
	}

	for k := range headers {
		if !strings.HasPrefix(k, ceHeaderPrefix) {
			continue
		}
		if _, std := ceStandardHeaders[k]; std {
			continue
		}
		s, _ := ceHeaderString(headers, k)
		if ev.Extensions == nil {
			ev.Extensions = make(map[string]string)
		}
		ev.Extensions[strings.TrimPrefix(k, ceHeaderPrefix)] = s
	}

	if len(body) > 0 {
		ev.Data = body
	}
	return nil
}

// ceHeaderString reads a ce-* header value, coercing string and []byte (the two
// forms amqp091 produces for AMQP short/long strings) to string.
func ceHeaderString(headers map[string]any, key string) (string, bool) {
	v, ok := headers[key]
	if !ok {
		return "", false
	}
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

// ensure interfaces are satisfied at compile time.
var (
	_ Codec       = (*ceStructuredCodec)(nil)
	_ Codec       = (*ceBinaryCodec)(nil)
	_ HeaderCodec = (*ceBinaryCodec)(nil)
)
