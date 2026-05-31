package amqptest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParseClusterNodes pins the WARREN_CLUSTER_NODES parsing rules that drive
// ClusterNodes' fail-not-skip behaviour. It runs in the default lane (no `cluster`
// tag, no live cluster): a nil result is exactly what ClusterNodes turns into a
// t.Fatal, so these cases prove both the happy parse and the two empty-input
// branches that must fail the cluster lane rather than pass green.
func TestParseClusterNodes(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{
			name: "empty string yields nil (unset → ClusterNodes fatals)",
			raw:  "",
			want: nil,
		},
		{
			name: "only separators and whitespace yields nil (set-but-empty → fatals)",
			raw:  " , ,\t,",
			want: nil,
		},
		{
			name: "single node",
			raw:  "amqp://guest:guest@localhost:5680/",
			want: []string{"amqp://guest:guest@localhost:5680/"},
		},
		{
			name: "three nodes, comma-separated",
			raw:  "amqp://localhost:5680/,amqp://localhost:5681/,amqp://localhost:5682/",
			want: []string{"amqp://localhost:5680/", "amqp://localhost:5681/", "amqp://localhost:5682/"},
		},
		{
			name: "surrounding whitespace is trimmed and empty entries dropped",
			raw:  " amqp://localhost:5680/ , , amqp://localhost:5681/ ",
			want: []string{"amqp://localhost:5680/", "amqp://localhost:5681/"},
		},
		{
			name: "trailing comma does not produce an empty entry",
			raw:  "amqp://localhost:5680/,amqp://localhost:5681/,",
			want: []string{"amqp://localhost:5680/", "amqp://localhost:5681/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseClusterNodes(tt.raw))
		})
	}
}
