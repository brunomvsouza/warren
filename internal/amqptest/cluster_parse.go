package amqptest

import "strings"

// parseClusterNodes splits a WARREN_CLUSTER_NODES value into per-node amqp:// URIs,
// trimming surrounding whitespace and dropping empty entries. It returns nil when
// raw is empty or yields no non-empty entry; ClusterNodes turns that nil into a
// t.Fatal. This is kept free of *testing.T — and out of the `cluster`-tagged file —
// so the parsing/empty-input rules can be unit-tested in the default test lane
// without a live cluster or the build tag.
func parseClusterNodes(raw string) []string {
	parts := strings.Split(raw, ",")
	nodes := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			nodes = append(nodes, s)
		}
	}
	if len(nodes) == 0 {
		return nil
	}
	return nodes
}
