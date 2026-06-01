package warren

import (
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ebRecorder records ExchangeBind calls (and satisfies topologyChannel).
type ebRecorder struct {
	binds []string // "src->dst:key"
}

func (r *ebRecorder) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp091.Table) error {
	return nil
}

func (r *ebRecorder) QueueDeclare(string, bool, bool, bool, bool, amqp091.Table) (amqp091.Queue, error) {
	return amqp091.Queue{}, nil
}
func (r *ebRecorder) QueueBind(string, string, string, bool, amqp091.Table) error { return nil }
func (r *ebRecorder) ExchangeBind(destination, key, source string, _ bool, _ amqp091.Table) error {
	r.binds = append(r.binds, source+"->"+destination+":"+key)
	return nil
}
func (r *ebRecorder) Close() error { return nil }

// T69 (R10-14 / EDA-03): exchange-to-exchange bindings declare via a separate
// Topology.ExchangeBindings slice (Binding is not reshaped, GA-05).
func TestTopology_declareOnChannel_exchangeBindings(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{
			{Name: "ingest", Kind: ExchangeFanout},
			{Name: "orders", Kind: ExchangeTopic},
		},
		ExchangeBindings: []ExchangeBinding{
			{Source: "ingest", Destination: "orders", RoutingKey: "order.#"},
		},
	}
	rec := &ebRecorder{}
	require.NoError(t, declareOnChannel(topo.expand(), rec))
	assert.Equal(t, []string{"ingest->orders:order.#"}, rec.binds)
}

func TestTopology_validate_exchangeBindings(t *testing.T) {
	cases := []struct {
		name    string
		eb      ExchangeBinding
		wantErr bool
	}{
		{"valid", ExchangeBinding{Source: "a", Destination: "b"}, false},
		{"empty source", ExchangeBinding{Destination: "b"}, true},
		{"empty destination", ExchangeBinding{Source: "a"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topo := &Topology{ExchangeBindings: []ExchangeBinding{tc.eb}}
			err := topo.validate()
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidOptions)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestTopology_expand_exchangeBindingsDeepCopied confirms the declare-once /
// deep-snapshot semantics extend to ExchangeBindings (AttachTo snapshot safety).
func TestTopology_expand_exchangeBindingsDeepCopied(t *testing.T) {
	topo := &Topology{
		ExchangeBindings: []ExchangeBinding{{Source: "a", Destination: "b", Args: map[string]any{"k": "v"}}},
	}
	snap := topo.expand()
	topo.ExchangeBindings[0].Source = "mutated"
	topo.ExchangeBindings[0].Args["k"] = "mutated"

	require.Len(t, snap.ExchangeBindings, 1)
	assert.Equal(t, "a", snap.ExchangeBindings[0].Source, "snapshot must not see post-expand mutations")
	assert.Equal(t, "v", snap.ExchangeBindings[0].Args["k"], "snapshot Args must be deep-copied")
}
