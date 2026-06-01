package warren

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// T64 (R10-9): quorum-queue structural validation. RabbitMQ requires a quorum
// queue to be Durable and forbids Exclusive / AutoDelete / x-max-priority;
// reject these locally as ErrInvalidOptions instead of letting the broker close
// the channel. Stream queues must also be Durable.
func TestTopology_validate_quorumStructural(t *testing.T) {
	cases := []struct {
		name    string
		queue   Queue
		wantErr bool
	}{
		{
			name:    "valid durable quorum",
			queue:   Queue{Name: "q", Type: QueueTypeQuorum, Durable: true},
			wantErr: false,
		},
		{
			name:    "non-durable quorum rejected",
			queue:   Queue{Name: "q", Type: QueueTypeQuorum, Durable: false},
			wantErr: true,
		},
		{
			name:    "exclusive quorum rejected",
			queue:   Queue{Name: "q", Type: QueueTypeQuorum, Durable: true, Exclusive: true},
			wantErr: true,
		},
		{
			name:    "auto-delete quorum rejected",
			queue:   Queue{Name: "q", Type: QueueTypeQuorum, Durable: true, AutoDelete: true},
			wantErr: true,
		},
		{
			name:    "quorum with MaxPriority field rejected",
			queue:   Queue{Name: "q", Type: QueueTypeQuorum, Durable: true, MaxPriority: 5},
			wantErr: true,
		},
		{
			name:    "quorum with raw x-max-priority rejected",
			queue:   Queue{Name: "q", Type: QueueTypeQuorum, Durable: true, Args: map[string]any{"x-max-priority": 5}},
			wantErr: true,
		},
		{
			name:    "valid durable stream",
			queue:   Queue{Name: "s", Type: QueueTypeStream, Durable: true},
			wantErr: false,
		},
		{
			name:    "non-durable stream rejected",
			queue:   Queue{Name: "s", Type: QueueTypeStream, Durable: false},
			wantErr: true,
		},
		{
			name:    "non-durable classic allowed",
			queue:   Queue{Name: "c", Type: QueueTypeClassic, Durable: false},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topo := &Topology{Queues: []Queue{tc.queue}}
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
