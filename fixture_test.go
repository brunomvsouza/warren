package warren

import (
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixturePayload struct {
	ID   int
	Name string
}

func TestNewDeliveryFixture_RoundTrips(t *testing.T) {
	ts := time.Now().Truncate(time.Second)
	body := &fixturePayload{ID: 7, Name: "seven"}
	d := NewDeliveryFixture(DeliveryFixture[fixturePayload]{
		Body:          body,
		Queue:         "orders",
		Headers:       Headers{"x-custom": "v"},
		MessageID:     "msg-1",
		CorrelationID: "corr-1",
		ContentType:   "application/json",
		Timestamp:     ts,
		Redelivered:   true,
		DeliveryTag:   99,
	})

	require.NotNil(t, d)
	assert.Equal(t, body, d.Body())
	assert.Equal(t, Headers{"x-custom": "v"}, d.Headers())
	assert.Equal(t, "msg-1", d.MessageID())
	assert.Equal(t, "corr-1", d.CorrelationID())
	assert.Equal(t, ts, d.Timestamp())
	assert.True(t, d.Redelivered())
	assert.Equal(t, uint64(99), d.DeliveryTag())
}

func TestNewDeliveryFixture_NilBodyAllowed(t *testing.T) {
	d := NewDeliveryFixture(DeliveryFixture[fixturePayload]{Queue: "q"})
	require.NotNil(t, d)
	assert.Nil(t, d.Body())
}

func TestNewDeliveryFixture_XDeathScopedToQueue(t *testing.T) {
	d := NewDeliveryFixture(DeliveryFixture[fixturePayload]{
		Queue: "myqueue",
		Headers: Headers{
			"x-death": []any{
				amqp091.Table{"queue": "myqueue", "reason": "rejected", "count": int64(4)},
				amqp091.Table{"queue": "other", "reason": "rejected", "count": int64(9)},
			},
		},
	})
	assert.Equal(t, 4, d.DeathCount(), "x-death must be parsed against the fixture's queue")
	assert.Equal(t, 4, d.DeathCountByReason("rejected"))
	assert.Equal(t, []string{"rejected"}, d.DeathReasons())
}

func TestNewBatchFixture_RoundTrips(t *testing.T) {
	b := NewBatchFixture(BatchFixture[fixturePayload]{
		Deliveries: []DeliveryFixture[fixturePayload]{
			{Body: &fixturePayload{ID: 1}, Queue: "q", DeliveryTag: 1},
			{Body: &fixturePayload{ID: 2}, Queue: "q", DeliveryTag: 2},
			{Body: &fixturePayload{ID: 3}, Queue: "q", DeliveryTag: 3},
		},
	})

	require.NotNil(t, b)
	msgs := b.Messages()
	require.Len(t, msgs, 3)
	assert.Equal(t, 1, msgs[0].ID)
	assert.Equal(t, 3, msgs[2].ID)

	ds := b.Deliveries()
	require.Len(t, ds, 3)
	assert.Equal(t, uint64(2), ds[1].DeliveryTag())
}

func TestNewBatchFixture_Empty(t *testing.T) {
	b := NewBatchFixture(BatchFixture[fixturePayload]{})
	require.NotNil(t, b)
	assert.Empty(t, b.Messages())
	assert.Empty(t, b.Deliveries())
}
