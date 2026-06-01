//go:build cluster

package warren

// Test-only seams exported to the cluster lane (package warren_test, build tag
// `cluster`). These live in an internal test file so they never reach the public
// API, and behind the `cluster` tag so they compile only for the cluster lane.

// WithAddrShuffleSeedForTest pins the process-level address-shuffle base (T66) to a
// fixed value, so a cluster campaign can assert a DETERMINISTIC initial connection
// distribution instead of the production per-process-random one. The seed must be
// non-zero (zero is the "unset → randomise" sentinel applyConnDefaults fills in).
// Exported for the cluster lane's deterministic-spread campaigns (the rotation and
// reconnect-storm tests), which pin it so their ≥2-node spread assertions never flake.
func WithAddrShuffleSeedForTest(seed int64) Option {
	return func(o *connOptions) { o.addrShuffleSeed = seed }
}
