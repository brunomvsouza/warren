package warren

import (
	"context"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/codec"
)

func newBinaryEvent(t *testing.T) cloudevents.Event {
	t.Helper()
	ev := cloudevents.NewEvent()
	ev.SetID("id-1")
	ev.SetSource("/svc")
	ev.SetType("t")
	require.NoError(t, ev.SetData(cloudevents.ApplicationJSON, map[string]any{"k": 1}))
	return ev
}

func TestPublisher_encodeMsg_HeaderCodecRoutesCEHeadersAndContentType(t *testing.T) {
	p := &Publisher[codec.CloudEvent]{codec: codec.NewCloudEventsBinary()}
	ev := newBinaryEvent(t)

	msg, body, err := p.encodeMsg(Message[codec.CloudEvent]{Body: &ev})
	require.NoError(t, err)

	// Body carries data; attributes become cloudEvents: headers; datacontenttype
	// becomes the content-type property.
	assert.Equal(t, ev.Data(), body)
	assert.Equal(t, "id-1", msg.Headers["cloudEvents:id"])
	assert.Equal(t, "1.0", msg.Headers["cloudEvents:specversion"])
	assert.Equal(t, "/svc", msg.Headers["cloudEvents:source"])
	assert.Equal(t, "t", msg.Headers["cloudEvents:type"])
	assert.Equal(t, "application/json", msg.ContentType)
	assert.NotContains(t, msg.Headers, "cloudEvents:datacontenttype")
}

func TestPublisher_encodeMsg_DoesNotMutateCallerHeaders(t *testing.T) {
	p := &Publisher[codec.CloudEvent]{codec: codec.NewCloudEventsBinary()}
	ev := newBinaryEvent(t)
	callerHeaders := Headers{"x-custom": "keep"}

	msg, _, err := p.encodeMsg(Message[codec.CloudEvent]{Body: &ev, Headers: callerHeaders})
	require.NoError(t, err)

	assert.Equal(t, "keep", msg.Headers["x-custom"])
	assert.Equal(t, "id-1", msg.Headers["cloudEvents:id"])

	// The caller's original map must not be mutated.
	assert.NotContains(t, callerHeaders, "cloudEvents:id")
	assert.Len(t, callerHeaders, 1)
}

func TestPublisher_encodeMsg_PlainCodecUnchanged(t *testing.T) {
	p := &Publisher[string]{codec: codec.NewJSON()}
	s := "hello"
	msg, body, err := p.encodeMsg(Message[string]{Body: &s})
	require.NoError(t, err)
	assert.Equal(t, []byte(`"hello"`), body)
	assert.NotContains(t, msg.Headers, "cloudEvents:id")
}

// unsafeHeaderCodec is a HeaderCodec whose EncodeWithHeaders returns a header
// value type amqp091 cannot serialise, exercising the publisher's validation of
// codec-returned headers.
type unsafeHeaderCodec struct{}

func (unsafeHeaderCodec) Encode(any) ([]byte, error) { return nil, codec.ErrInvalidMessage }
func (unsafeHeaderCodec) Decode([]byte, any) error   { return codec.ErrInvalidMessage }
func (unsafeHeaderCodec) ContentType() string        { return "" }
func (unsafeHeaderCodec) EncodeWithHeaders(any) ([]byte, map[string]any, string, error) {
	return []byte("body"), map[string]any{"x-bad": struct{ X int }{X: 1}}, "", nil
}
func (unsafeHeaderCodec) DecodeWithHeaders([]byte, map[string]any, string, any) error { return nil }

var _ codec.HeaderCodec = unsafeHeaderCodec{}

func TestPublisher_encodeMsg_RejectsUnsafeHeaderCodecHeaders(t *testing.T) {
	p := &Publisher[string]{codec: unsafeHeaderCodec{}}
	s := "x"
	_, _, err := p.encodeMsg(Message[string]{Body: &s})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMessage)
}

func TestPublisher_encodeMsg_NoContentTypeWhenCodecOmitsIt(t *testing.T) {
	p := &Publisher[codec.CloudEvent]{codec: codec.NewCloudEventsBinary()}
	ev := cloudevents.NewEvent()
	ev.SetID("id")
	ev.SetSource("/s")
	ev.SetType("t")

	msg, body, err := p.encodeMsg(Message[codec.CloudEvent]{Body: &ev})
	require.NoError(t, err)

	// No datacontenttype on the event -> empty body and no content-type property.
	assert.Empty(t, body)
	assert.Empty(t, msg.ContentType)
	assert.Equal(t, "id", msg.Headers["cloudEvents:id"])
}

func TestConsumer_dispatch_HeaderCodecDecodesCE(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[codec.CloudEvent](conn).Queue("q").Codec(codec.NewCloudEventsBinary()).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	var got codec.CloudEvent
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, ev codec.CloudEvent) error {
			got = ev
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`{"k":1}`),
		ContentType: "application/json",
		Headers: amqp091.Table{
			"cloudEvents:specversion": "1.0",
			"cloudEvents:id":          "id-1",
			"cloudEvents:source":      "/svc",
			"cloudEvents:type":        "t",
		},
	}
	<-done

	assert.Equal(t, "id-1", got.ID())
	assert.Equal(t, "1.0", got.SpecVersion())
	assert.Equal(t, "/svc", got.Source())
	assert.Equal(t, "application/json", got.DataContentType())
	assert.Equal(t, []byte(`{"k":1}`), got.Data())
}

func TestCloudEventsBinary_EndToEnd_PublishToConsume(t *testing.T) {
	defer goleak.VerifyNone(t)

	p := &Publisher[codec.CloudEvent]{codec: codec.NewCloudEventsBinary()}
	original := cloudevents.NewEvent()
	original.SetID("evt-7")
	original.SetSource("/orders")
	original.SetType("com.example.created")
	original.SetSubject("order/7")
	original.SetTime(time.Date(2026, 5, 28, 9, 30, 0, 0, time.UTC))
	original.SetExtension("tenant", "acme")
	require.NoError(t, original.SetData(cloudevents.ApplicationJSON, map[string]any{"order": 7}))

	msg, body, err := p.encodeMsg(Message[codec.CloudEvent]{Body: &original})
	require.NoError(t, err)
	pub := buildPublishing(msg, body)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[codec.CloudEvent](conn).Queue("q").Codec(codec.NewCloudEventsBinary()).Build()
	require.NoError(t, err)
	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	var got codec.CloudEvent
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, ev codec.CloudEvent) error {
			got = ev
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{Body: pub.Body, Headers: pub.Headers, ContentType: pub.ContentType}
	<-done

	assert.Equal(t, original.ID(), got.ID())
	assert.Equal(t, original.Source(), got.Source())
	assert.Equal(t, original.Type(), got.Type())
	assert.Equal(t, original.Subject(), got.Subject())
	assert.Equal(t, original.DataContentType(), got.DataContentType())
	assert.True(t, original.Time().Equal(got.Time()))
	assert.Equal(t, original.Data(), got.Data())
	assert.Equal(t, "acme", got.Extensions()["tenant"])
}
