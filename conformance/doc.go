// Package conformance holds AMQP 0-9-1 wire-protocol conformance tests for
// warren. Per Lens-10 TV-06 these are REAL-BROKER-ONLY for v0.1: a test AMQP
// server stub cannot prove the contracts that matter here — broker-nack on
// x-overflow=reject-publish, the quorum x-delivery-limit poison bound,
// basic.cancel on queue deletion, and the mandatory return/ack ordering — all of
// which are emergent broker behaviours, not frame-encoding details.
//
// The tests are guarded by the `conformance` build tag and run via
// `make test-conformance` against the broker named by AMQP_TEST_URL (the same
// pinned broker the integration lane provisions; Docker required). This file has
// no build tag so the package is a valid (empty) build target under `go build
// ./...` when the tag is absent.
package conformance
