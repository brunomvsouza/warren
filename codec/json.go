package codec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type jsonCodec struct {
	strict bool
	// observer, when non-nil on a lax codec, is called once per unknown top-level
	// field seen during Decode (schema-drift signal, T56). nil on strict codecs.
	observer func(path string)
}

// JSONOption configures the lax JSON codec returned by NewJSON.
type JSONOption func(*jsonCodec)

// WithUnknownFieldObserver registers fn to be called once per unknown top-level
// field encountered while decoding into a struct, WITHOUT failing the lax decode
// (T56). It is the schema-drift hook: a v2 producer adds a field, v1 lax consumers
// keep working (Postel's Law), and the observer makes the otherwise-silent drift
// visible. The canonical wiring increments a codec_unknown_fields_total{type}
// Prometheus counter from fn so an operator can alert on drift before it matters.
//
// fn receives the wire field name (the JSON key). It fires only for struct targets
// (a map or interface{} has no fixed schema, so nothing is "unknown") and only on
// the lax codec — NewJSONStrict rejects unknown fields outright. Detection adds one
// extra json.Unmarshal pass per Decode and only when an observer is set, so the
// default (no observer) path is unchanged; nested-object drift is not reported.
// fn must be safe for concurrent use (one codec may serve many consumer goroutines)
// and must not block or panic.
func WithUnknownFieldObserver(fn func(path string)) JSONOption {
	return func(c *jsonCodec) { c.observer = fn }
}

// NewJSON returns the default JSON codec.
//
// The codec follows Postel's Law: it is conservative in what it sends (Encode
// emits exactly the fields declared on M) and liberal in what it accepts
// (Decode tolerates unknown fields on the wire). Producer-first deploys — a v2
// service publishing a new field alongside v1 services that have not yet
// rolled — therefore do not poison v1 consumers' DLQs.
//
// Pass WithUnknownFieldObserver to surface that otherwise-silent drift without
// failing the decode. For hard consumer-side schema enforcement (e.g. compliance
// pipelines where every drift must error), use NewJSONStrict instead.
func NewJSON(opts ...JSONOption) Codec {
	c := &jsonCodec{strict: false}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// NewJSONStrict returns a JSON codec that rejects unknown fields on Decode.
// Unknown fields surface as ErrInvalidMessage wrapping the json decoder error.
//
// Use this only when consumer-side schema drift MUST be a hard error (e.g.
// regulated pipelines). For the common case prefer NewJSON, which is liberal
// in what it receives so producer-first deploys do not break v1 consumers.
func NewJSONStrict() Codec {
	return &jsonCodec{strict: true}
}

func (c *jsonCodec) ContentType() string { return "application/json" }

func (c *jsonCodec) Encode(v any) (out []byte, err error) {
	out, err = json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return out, nil
}

func (c *jsonCodec) Decode(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if c.strict {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: payload contains trailing data after first JSON value", ErrInvalidMessage)
	}
	if c.observer != nil {
		c.reportUnknownFields(data, v)
	}
	return nil
}

// reportUnknownFields calls the observer once per wire field that does not match a
// known field on the struct pointed to by v. It is a best-effort drift signal, so
// any condition it cannot reason about (non-struct target, non-object payload) is
// silently skipped rather than reported.
func (c *jsonCodec) reportUnknownFields(data []byte, v any) {
	rt := reflect.TypeOf(v)
	for rt != nil && rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt == nil || rt.Kind() != reflect.Struct {
		return // no fixed schema → nothing is "unknown"
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return // not a JSON object; the lax decode above already handled it
	}
	known := knownJSONFields(rt)
	for key := range raw {
		// encoding/json matches names case-insensitively, so compare lowercased.
		if _, ok := known[strings.ToLower(key)]; !ok {
			c.observer(key)
		}
	}
}

// knownJSONFields returns the set of JSON field names (lowercased for the
// case-insensitive match encoding/json performs) that rt accepts on decode,
// following the same rules as the stdlib: an explicit `json:"name"` tag wins, a
// `json:"-"` tag drops the field, unexported fields are ignored, and the fields of
// an untagged embedded struct are promoted.
func knownJSONFields(rt reflect.Type) map[string]struct{} {
	out := make(map[string]struct{}, rt.NumField())
	for i := range rt.NumField() {
		f := rt.Field(i)
		if f.PkgPath != "" && !f.Anonymous {
			continue // unexported
		}
		tag := f.Tag.Get("json")
		tagName, _, _ := strings.Cut(tag, ",")
		if tagName == "-" {
			continue // explicitly excluded
		}
		// Untagged embedded struct: its fields are promoted onto the parent.
		if f.Anonymous && tagName == "" {
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				for k := range knownJSONFields(ft) {
					out[k] = struct{}{}
				}
				continue
			}
		}
		name := f.Name
		if tagName != "" {
			name = tagName
		}
		out[strings.ToLower(name)] = struct{}{}
	}
	return out
}

// ensure interface is satisfied at compile time.
var _ Codec = (*jsonCodec)(nil)
