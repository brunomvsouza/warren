package warren

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// T68 (R10-13 / EDA-01): Exchange.AlternateExchange injects the broker's
// `alternate-exchange` argument additively — the zero value preserves today's
// behaviour, and a raw Args["alternate-exchange"] is rejected in favour of the field.
func TestTopology_expand_alternateExchange(t *testing.T) {
	t.Run("field injects the alternate-exchange arg", func(t *testing.T) {
		topo := &Topology{
			Exchanges: []Exchange{
				{Name: "ingest", Kind: ExchangeTopic, AlternateExchange: "unrouted"},
				{Name: "unrouted", Kind: ExchangeFanout},
			},
		}
		ex := findExchange(t, topo.expand(), "ingest")
		assert.Equal(t, "unrouted", ex.Args["alternate-exchange"])
		// The AE exchange itself carries no alternate-exchange arg.
		ae := findExchange(t, topo.expand(), "unrouted")
		assert.NotContains(t, ae.Args, "alternate-exchange")
	})

	t.Run("zero value injects nothing", func(t *testing.T) {
		topo := &Topology{Exchanges: []Exchange{{Name: "plain", Kind: ExchangeTopic}}}
		ex := findExchange(t, topo.expand(), "plain")
		assert.NotContains(t, ex.Args, "alternate-exchange")
	})
}

func TestTopology_validate_rejectsRawAlternateExchangeArg(t *testing.T) {
	// Both the canonical key and the historical x- alias must be rejected so the
	// AlternateExchange field stays the single source of truth (review T68).
	for _, key := range []string{"alternate-exchange", "x-alternate-exchange"} {
		t.Run(key, func(t *testing.T) {
			topo := &Topology{
				Exchanges: []Exchange{
					{Name: "ingest", Kind: ExchangeTopic, Args: map[string]any{key: "unrouted"}},
				},
			}
			err := topo.validate()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidOptions)
		})
	}
}

// findExchange returns the named exchange from an expanded topology.
func findExchange(t *testing.T, topo *Topology, name string) Exchange {
	t.Helper()
	for _, e := range topo.Exchanges {
		if e.Name == name {
			return e
		}
	}
	t.Fatalf("exchange %q not found", name)
	return Exchange{}
}
