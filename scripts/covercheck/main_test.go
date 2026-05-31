package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const mod = "github.com/brunomvsouza/warren"

// criticalFiller returns coverage-profile lines placing every critical package
// and the critical file (channelpool.go) at 100%, except those listed in `skip`.
// The real gate (scripts/coverage.sh) always profiles all of them, so a unit
// profile must too: check() fails closed when a declared critical package OR file
// is absent from the profile (the two rules mirror each other), so a test that
// does not mean to exercise a given choke-point still has to present it. A test
// driving a specific choke-point to a non-100% value lists it in `skip` and
// supplies its own block.
func criticalFiller(skip ...string) string {
	skipped := make(map[string]bool, len(skip))
	for _, s := range skip {
		skipped[s] = true
	}
	var b strings.Builder
	for _, pkg := range []string{
		"internal/reconnect",
		"internal/confirms",
		"internal/amqperror",
		"internal/redact",
	} {
		if !skipped[pkg] {
			b.WriteString(mod + "/" + pkg + "/x.go:1.1,2.2 1 1\n")
		}
	}
	if !skipped["channelpool.go"] {
		b.WriteString(mod + "/channelpool.go:1.1,2.2 1 1\n")
	}
	return b.String()
}

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
	// A non-critical package at 79% (below the 80% default floor). criticalFiller
	// satisfies the fail-closed presence checks for every choke-point this test is
	// not exercising, so the only violation is codec.
	profile := "mode: atomic\n" + criticalFiller() +
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
	// The other choke-points are filled at 100% so only reconnect violates.
	profile := "mode: atomic\n" + criticalFiller("internal/reconnect") +
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
	profile := "mode: atomic\n" + criticalFiller("channelpool.go") +
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
	profile := "mode: atomic\n" + criticalFiller() +
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

func TestAnalyze_rejectsFilePathWithoutSlash(t *testing.T) {
	// A well-formed block spec whose file part contains no '/' must return an
	// error, NOT panic. Before the guard, file[:strings.LastIndex(file, "/")]
	// indexed with -1 and panicked. The path is trusted (go test-generated and
	// always import-path-qualified), so this is defense-in-depth for the gate.
	_, err := analyze(strings.NewReader("mode: atomic\nnoslashfile:1.1,2.2 1 1\n"))
	require.Error(t, err)
}

func TestCheck_missingCriticalFileFailsClosed(t *testing.T) {
	// A profile that contains NO channelpool.go block must fail closed: the
	// critical file absent from the profile is itself a violation at 0%, not a
	// silent pass. The critical packages are present (filler) so channelpool.go is
	// the only miss. Guards the regression where dropping channelpool.go from
	// coverage would let the gate go green on the choke-point file.
	profile := "mode: atomic\n" + criticalFiller("channelpool.go") +
		mod + "/codec/json.go:1.1,2.2 100 1\n" // channelpool.go deliberately absent

	a, err := analyze(strings.NewReader(profile))
	require.NoError(t, err)

	violations, _ := a.check()
	require.Len(t, violations, 1)
	assert.Equal(t, "channelpool.go", violations[0].name)
	assert.Equal(t, floorCritical, violations[0].floor)
	assert.Equal(t, 0.0, violations[0].pct)
}

func TestCheck_missingCriticalPackageFailsClosed(t *testing.T) {
	// Symmetric to the file rule: a critical PACKAGE absent from the profile must
	// fail closed at 0%, not silently void its 95% floor. Guards the regression
	// where dropping a choke-point package from coverage.sh's package list (a
	// rename, a widened grep exclusion, or a build-tag change that zeroes its test
	// files) would let the gate go green with the floor unenforced. Here
	// internal/redact is omitted while every other choke-point is present.
	profile := "mode: atomic\n" + criticalFiller("internal/redact") +
		mod + "/codec/json.go:1.1,2.2 100 1\n" // internal/redact deliberately absent

	a, err := analyze(strings.NewReader(profile))
	require.NoError(t, err)

	violations, _ := a.check()
	require.Len(t, violations, 1)
	assert.Equal(t, "internal/redact", violations[0].name)
	assert.Equal(t, floorCritical, violations[0].floor)
	assert.Equal(t, 0.0, violations[0].pct)
}

func TestCheck_exactlyAtFloorPasses(t *testing.T) {
	// Boundary case: a default package at exactly 80.0% and a critical package at
	// exactly 95.0% must NOT be flagged. This is what the +1e-9 epsilon in check()
	// guarantees; an off-by-one there would silently fail packages sitting on the
	// floor. reconnect is skipped in the filler so the test pins it to exactly
	// 95.0% (the filler's 100% would otherwise mask the boundary).
	profile := "mode: atomic\n" + criticalFiller("internal/reconnect") +
		mod + "/codec/json.go:1.1,2.2 80 1\n" + // 80 covered
		mod + "/codec/json.go:3.1,4.2 20 0\n" + // 20 not → exactly 80.0%
		mod + "/internal/reconnect/loop.go:1.1,2.2 95 1\n" + // 95 covered
		mod + "/internal/reconnect/loop.go:3.1,4.2 5 0\n" // 5 not → exactly 95.0% (critical)

	a, err := analyze(strings.NewReader(profile))
	require.NoError(t, err)

	violations, _ := a.check()
	assert.Empty(t, violations)
}

// writeProfile writes content to a temp file and returns its path.
func writeProfile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "coverage.out")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestRun_exitCodes(t *testing.T) {
	allPass := "mode: atomic\n" + criticalFiller() +
		mod + "/codec/json.go:1.1,2.2 85 1\n" +
		mod + "/codec/json.go:3.1,4.2 15 0\n" // codec 85% (>= 80)
	belowFloor := "mode: atomic\n" + criticalFiller() +
		mod + "/codec/json.go:1.1,2.2 79 1\n" +
		mod + "/codec/json.go:3.1,4.2 21 0\n" // codec 79% (< 80)

	tests := []struct {
		name string
		args []string
		want int
	}{
		{"too few args", []string{"covercheck"}, 2},
		{"too many args", []string{"covercheck", "a", "b"}, 2},
		{"nonexistent profile", []string{"covercheck", filepath.Join(t.TempDir(), "missing.out")}, 2},
		{"malformed profile", []string{"covercheck", writeProfile(t, "mode: atomic\ngarbage line\n")}, 2},
		{"below floor", []string{"covercheck", writeProfile(t, belowFloor)}, 1},
		{"all pass", []string{"covercheck", writeProfile(t, allPass)}, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			got := run(tc.args, &stdout, &stderr)
			assert.Equal(t, tc.want, got, "stdout=%q stderr=%q", stdout.String(), stderr.String())
		})
	}
}

func TestRun_failurePrintsViolationToStderr(t *testing.T) {
	belowFloor := "mode: atomic\n" + criticalFiller() +
		mod + "/codec/json.go:1.1,2.2 79 1\n" +
		mod + "/codec/json.go:3.1,4.2 21 0\n"

	var stdout, stderr bytes.Buffer
	got := run([]string{"covercheck", writeProfile(t, belowFloor)}, &stdout, &stderr)

	require.Equal(t, 1, got)
	assert.Contains(t, stderr.String(), "codec")
	assert.Contains(t, stderr.String(), "FAILED")
	assert.Contains(t, stdout.String(), "coverage by package:")
}
