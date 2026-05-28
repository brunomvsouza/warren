package warren

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/codec"
)

func TestPublisher_encodeMsg_HeaderCodecRoutesCEHeaders(t *testing.T) {
	p := &Publisher[codec.CloudEvent]{codec: codec.NewCloudEventsBinary()}
	ev := codec.CloudEvent{
		ID:              "id-1",
		Source:          "/svc",
		SpecVersion:     "1.0",
		Type:            "t",
		DataContentType: "application/json",
		Data:            []byte(`{"k":1}`),
	}

	msg, body, err := p.encodeMsg(Message[codec.CloudEvent]{Body: &ev})
	require.NoError(t, err)

	// Body carries data only; attributes become ce-* headers.
	assert.Equal(t, ev.Data, body)
	assert.Equal(t, "id-1", msg.Headers["ce-id"])
	assert.Equal(t, "1.0", msg.Headers["ce-specversion"])
	assert.Equal(t, "/svc", msg.Headers["ce-source"])
	assert.Equal(t, "t", msg.Headers["ce-type"])
	assert.Equal(t, "application/json", msg.Headers["ce-datacontenttype"])
}

func TestPublisher_encodeMsg_DoesNotMutateCallerHeaders(t *testing.T) {
	p := &Publisher[codec.CloudEvent]{codec: codec.NewCloudEventsBinary()}
	ev := codec.CloudEvent{ID: "id", Source: "s", SpecVersion: "1.0", Type: "t"}
	callerHeaders := Headers{"x-custom": "keep"}

	msg, _, err := p.encodeMsg(Message[codec.CloudEvent]{Body: &ev, Headers: callerHeaders})
	require.NoError(t, err)

	// Merged result carries both the user header and the ce-* headers.
	assert.Equal(t, "keep", msg.Headers["x-custom"])
	assert.Equal(t, "id", msg.Headers["ce-id"])

	// The caller's original map must not be mutated.
	assert.NotContains(t, callerHeaders, "ce-id")
	assert.Len(t, callerHeaders, 1)
}

func TestPublisher_encodeMsg_PlainCodecUnchanged(t *testing.T) {
	// A non-HeaderCodec must keep using the plain Encode path with no header injection.
	p := &Publisher[string]{codec: codec.NewJSON()}
	s := "hello"
	msg, body, err := p.encodeMsg(Message[string]{Body: &s})
	require.NoError(t, err)
	assert.Equal(t, []byte(`"hello"`), body)
	assert.NotContains(t, msg.Headers, "ce-id")
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
		Body: []byte(`{"k":1}`),
		Headers: amqp091.Table{
			"ce-specversion":     "1.0",
			"ce-id":              "id-1",
			"ce-source":          "/svc",
			"ce-type":            "t",
			"ce-datacontenttype": "application/json",
		},
	}
	<-done

	assert.Equal(t, "id-1", got.ID)
	assert.Equal(t, "1.0", got.SpecVersion)
	assert.Equal(t, "/svc", got.Source)
	assert.Equal(t, "application/json", got.DataContentType)
	assert.Equal(t, []byte(`{"k":1}`), got.Data)
}

func TestCloudEventsBinary_EndToEnd_PublishToConsume(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Encode through the publisher choke-point.
	p := &Publisher[codec.CloudEvent]{codec: codec.NewCloudEventsBinary()}
	original := codec.CloudEvent{
		ID:              "evt-7",
		Source:          "/orders",
		SpecVersion:     "1.0",
		Type:            "com.example.created",
		Subject:         "order/7",
		DataContentType: "application/json",
		Time:            time.Date(2026, 5, 28, 9, 30, 0, 0, time.UTC),
		Data:            []byte(`{"order":7}`),
		Extensions:      map[string]string{"tenant": "acme"},
	}
	msg, body, err := p.encodeMsg(Message[codec.CloudEvent]{Body: &original})
	require.NoError(t, err)
	pub := buildPublishing(msg, body)

	// Feed the produced frame into a consumer as a delivery.
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

	assert.Equal(t, original.ID, got.ID)
	assert.Equal(t, original.Source, got.Source)
	assert.Equal(t, original.Type, got.Type)
	assert.Equal(t, original.Subject, got.Subject)
	assert.Equal(t, original.DataContentType, got.DataContentType)
	assert.True(t, original.Time.Equal(got.Time))
	assert.Equal(t, original.Data, got.Data)
	assert.Equal(t, original.Extensions, got.Extensions)
}
