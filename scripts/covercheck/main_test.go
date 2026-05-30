package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const mod = "github.com/brunomvsouza/warren"

func TestAnalyze_aggregatesByPackageAndFile(t *testing.T) {
	// Two files in the same package: 3 of 4 statements covered → 75%.
	profile := "mode: atomic\n" +
		mod + "/internal/redact/redact.go:10.1,12.2 2 1\n" + // 2 stmts, covered
		mod + "/internal/redact/uri.go:20.1,21.2 1 1\n" + // 1 stmt, covered
		mod + "/internal/redact/uri.go:30.1,31.2 1 0\n" // 1 stmt, NOT covered

	a, err := analyze(strings.NewReader(profile))
	require.NoError(t, err)

	pkg := a.packages[mod+"/internal/redact"]
	require.NotNil(t, pkg)
	assert.Equal(t, 4, pkg.total)
	assert.Equal(t, 3, pkg.covered)
	assert.InDelta(t, 75.0, pkg.pct(), 0.01)

	// File-level aggregation for uri.go: 1 of 2 covered → 50%.
	uri := a.files[mod+"/internal/redact/uri.go"]
	require.NotNil(t, uri)
	assert.InDelta(t, 50.0, uri.pct(), 0.01)
}

func TestCheck_flagsPackageBelowDefaultFloor(t *testing.T) {
	// A non-critical package at 79% (below the 80% default floor). channelpool.go
	// is included at 100% because the real gate always profiles the root package;
	// a critical file absent from the profile is itself a failure (fail-closed).
	profile := "mode: atomic\n" +
		mod + "/channelpool.go:1.1,2.2 1 1\n" +
		mod + "/codec/json.go:1.1,2.2 79 1\n" +
		mod + "/codec/json.go:3.1,4.2 21 0\n" // 79/100 = 79%

	a, err := analyze(strings.NewReader(profile))
	require.NoError(t, err)

	violations, _ := a.check()
	require.Len(t, violations, 1)
	assert.Equal(t, "codec", violations[0].name)
	assert.InDelta(t, 79.0, violations[0].pct, 0.01)
	assert.Equal(t, floorDefault, violations[0].floor)
}

func TestCheck_criticalPackageNeeds95(t *testing.T) {
	// reconnect at 90% passes the 80% default but fails the 95% critical floor.
	// channelpool.go at 100% keeps the profile realistic (root package present).
	profile := "mode: atomic\n" +
		mod + "/channelpool.go:1.1,2.2 1 1\n" +
		mod + "/internal/reconnect/loop.go:1.1,2.2 90 1\n" +
		mod + "/internal/reconnect/loop.go:3.1,4.2 10 0\n" // 90%

	a, err := analyze(strings.NewReader(profile))
	require.NoError(t, err)

	violations, _ := a.check()
	require.Len(t, violations, 1)
	assert.Equal(t, "internal/reconnect", violations[0].name)
	assert.Equal(t, floorCritical, violations[0].floor)
}

func TestCheck_channelpoolFileFloor(t *testing.T) {
	// channelpool.go is a critical FILE inside the root package: 90% fails 95%,
	// even though the root package as a whole could pass at 80%.
	profile := "mode: atomic\n" +
		mod + "/channelpool.go:1.1,2.2 90 1\n" +
		mod + "/channelpool.go:3.1,4.2 10 0\n" + // channelpool.go 90%
		mod + "/publisher.go:1.1,2.2 100 1\n" // rest of root package 100%

	a, err := analyze(strings.NewReader(profile))
	require.NoError(t, err)

	violations, _ := a.check()
	// Only channelpool.go violates (root package overall is 95%).
	require.Len(t, violations, 1)
	assert.Equal(t, "channelpool.go", violations[0].name)
	assert.Equal(t, floorCritical, violations[0].floor)
}

func TestCheck_allPass(t *testing.T) {
	profile := "mode: atomic\n" +
		mod + "/internal/redact/redact.go:1.1,2.2 100 1\n" +
		mod + "/channelpool.go:1.1,2.2 100 1\n" +
		mod + "/codec/json.go:1.1,2.2 85 1\n" +
		mod + "/codec/json.go:3.1,4.2 15 0\n" // codec 85% (>= 80)

	a, err := analyze(strings.NewReader(profile))
	require.NoError(t, err)

	violations, report := a.check()
	assert.Empty(t, violations)
	assert.NotEmpty(t, report)
}

func TestAnalyze_rejectsMalformedLine(t *testing.T) {
	_, err := analyze(strings.NewReader("mode: atomic\nthis is not a valid profile line\n"))
	require.Error(t, err)
}
