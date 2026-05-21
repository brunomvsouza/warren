// Package connpool provides pure helper functions for the multi-TCP-connection
// pool used by Connection (T07d).  The heavier supervisor and barrier logic
// lives in connection.go to avoid import cycles.
package connpool

import (
	"fmt"
	"hash/fnv"
)

// PinIndex returns the stable connection index for a consumer tag within a
// pool of size n.  The mapping is deterministic — the same tag always yields
// the same index — so a consumer follows its connection across reconnects.
// When n ≤ 1 the result is always 0.
func PinIndex(tag string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(tag))
	return int(h.Sum32()) % n
}

// ConnName returns the AMQP connection_name property for the idx-th connection
// in a pool of the given role.  Publishers use the suffix "-pub-<idx>",
// consumers use "-con-<idx>".
//
// Example: ConnName("myapp-host-1234", "publisher", 0) → "myapp-host-1234-pub-0"
func ConnName(baseName, role string, idx int) string {
	suffix := "pub"
	if role == "consumer" {
		suffix = "con"
	}
	return fmt.Sprintf("%s-%s-%d", baseName, suffix, idx)
}
