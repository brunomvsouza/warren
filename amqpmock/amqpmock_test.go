package amqpmock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/amqpmock"
	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/log"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

type order struct{ ID int }

// Compile-time guarantees that every generated mock satisfies its interface.
var (
	_ codec.Codec              = (*amqpmock.MockCodec)(nil)
	_ codec.HeaderCodec        = (*amqpmock.MockHeaderCodec)(nil)
	_ log.Logger               = (*amqpmock.MockLogger)(nil)
	_ metrics.ClientMetrics    = (*amqpmock.MockClientMetrics)(nil)
	_ metrics.PublisherMetrics = (*amqpmock.MockPublisherMetrics)(nil)
	_ metrics.ConsumerMetrics  = (*amqpmock.MockConsumerMetrics)(nil)
	_ otel.Tracer              = (*amqpmock.MockTracer)(nil)
	_ otel.Span                = (*amqpmock.MockSpan)(nil)
	_ otel.LinkingTracer       = (*amqpmock.MockLinkingTracer)(nil)
)

func TestNewDelivery_FixtureRoundTrips(t *testing.T) {
	ts := time.Now().Truncate(time.Second)
	d := amqpmock.NewDelivery(amqpmock.DeliveryFixture[order]{
		Body:          &order{ID: 5},
		Queue:         "orders",
		Headers:       warren.Headers{"k": "v"},
		MessageID:     "m-1",
		CorrelationID: "c-1",
		Timestamp:     ts,
		Redelivered:   true,
		DeliveryTag:   3,
	})

	require.NotNil(t, d)
	assert.Equal(t, 5, d.Body().ID)
	assert.Equal(t, warren.Headers{"k": "v"}, d.Headers())
	assert.Equal(t, "m-1", d.MessageID())
	assert.Equal(t, "c-1", d.CorrelationID())
	assert.Equal(t, ts, d.Timestamp())
	assert.True(t, d.Redelivered())
	assert.Equal(t, uint64(3), d.DeliveryTag())
}

func TestNewBatch_FixtureRoundTrips(t *testing.T) {
	b := amqpmock.NewBatch(amqpmock.BatchFixture[order]{
		Deliveries: []amqpmock.DeliveryFixture[order]{
			{Body: &order{ID: 1}, Queue: "q", DeliveryTag: 1},
			{Body: &order{ID: 2}, Queue: "q", DeliveryTag: 2},
		},
	})

	require.NotNil(t, b)
	msgs := b.Messages()
	require.Len(t, msgs, 2)
	assert.Equal(t, 1, msgs[0].ID)
	assert.Equal(t, 2, msgs[1].ID)
	require.Len(t, b.Deliveries(), 2)
}

// TestMockConsumerMetrics_Usage exercises a generated mock end to end through a
// gomock.Controller: it records an expectation, invokes the method, and lets
// the controller's auto-registered cleanup assert the expectation was met.
func TestMockConsumerMetrics_Usage(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := amqpmock.NewMockConsumerMetrics(ctrl)

	m.EXPECT().RecordCancelled("orders", "broker_initiated").Times(1)
	m.RecordCancelled("orders", "broker_initiated")
}

// TestMockTracer_ReturnsMockSpan shows the Tracer/Span mock pair wiring: a
// MockTracer can be programmed to return a MockSpan, the shape Publisher and
// Consumer depend on.
func TestMockTracer_ReturnsMockSpan(t *testing.T) {
	ctrl := gomock.NewController(t)
	tr := amqpmock.NewMockTracer(ctrl)
	span := amqpmock.NewMockSpan(ctrl)

	ctx := t.Context()
	tr.EXPECT().Start(ctx, "publish").Return(ctx, span)
	span.EXPECT().End().Times(1)

	_, got := tr.Start(ctx, "publish")
	got.End()
}
