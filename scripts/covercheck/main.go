// Command covercheck enforces per-package and critical-path coverage floors
// against a Go coverage profile, failing (exit 1) when any package drops below
// its threshold. It is the enforcement half of SPEC §9 / Rev 7: the coverage
// floor is a CI gate, not a reported number.
//
// Usage:
//
//	covercheck coverage.out
//
// Thresholds:
//   - Every package in the profile must be >= 80% statement coverage.
//   - The critical-path packages (reconnect, confirms, amqperror, redact) and
//     the root-package file channelpool.go must be >= 95% — they are the
//     choke-points for AMQP correctness and credential safety.
package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	floorDefault  = 80.0
	floorCritical = 95.0
	modulePath    = "github.com/brunomvsouza/warren"
)

// criticalPackages require floorCritical instead of floorDefault. Keyed by the
// package import path.
//
// The 95% set is deliberately scoped to the smallest core whose blast radius on
// an uncovered branch is silent message loss or a credential leak: the reconnect
// barrier (drops/duplicates), the confirm tracker (at-least-once), error
// classification (transient/permanent retry decisions), and URI redaction
// (secrets). internal/connpool is a correctness choke-point too (the role-split
// TCP pool, CLAUDE.md invariant #1), but it is held to the 80% default on
// purpose: its lifecycle is additionally exercised end-to-end by the
// reconnect-chaos and integration lanes, whereas the four packages above are
// unit-verifiable in isolation and a unit gap there is not caught elsewhere.
// connpool comfortably clears the 95% critical floor today; it stays at the
// default by the rationale above, not because it cannot pass.
var criticalPackages = map[string]bool{
	modulePath + "/internal/reconnect": true,
	modulePath + "/internal/confirms":  true,
	modulePath + "/internal/amqperror": true,
	modulePath + "/internal/redact":    true,
}

// criticalFiles require floorCritical at file granularity (for choke-point code
// that lives inside a larger package). Keyed by the file's import path + base
// name, e.g. ".../warren/channelpool.go".
var criticalFiles = map[string]bool{
	modulePath + "/channelpool.go": true,
}

// stat accumulates covered and total statements.
type stat struct {
	covered int
	total   int
}

func (s stat) pct() float64 {
	if s.total == 0 {
		return 100.0
	}
	return 100 * float64(s.covered) / float64(s.total)
}

// analysis is the per-package and per-file coverage computed from a profile.
type analysis struct {
	packages map[string]*stat
	files    map[string]*stat
}

// analyze parses a Go coverage profile and aggregates statement coverage by
// package (the directory of each file) and by file. The profile format is:
//
//	mode: <mode>
//	<importpath>/<file>:<startLine>.<col>,<endLine>.<col> <numStmts> <count>
func analyze(r io.Reader) (*analysis, error) {
	a := &analysis{
		packages: make(map[string]*stat),
		files:    make(map[string]*stat),
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("line %d: expected 3 fields, got %d: %q", i+1, len(fields), line)
		}
		numStmts, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, fmt.Errorf("line %d: bad numStmts: %w", i+1, err)
		}
		count, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, fmt.Errorf("line %d: bad count: %w", i+1, err)
		}
		// fields[0] is "<file>:<range>"; the file is everything before the last ':'.
		colon := strings.LastIndex(fields[0], ":")
		if colon < 0 {
			return nil, fmt.Errorf("line %d: no ':' in block spec: %q", i+1, fields[0])
		}
		file := fields[0][:colon]
		// The package is the directory of the file (everything before the last
		// '/'). Guard the no-slash case: without it, LastIndex returns -1 and
		// file[:-1] panics on a malformed profile line.
		slash := strings.LastIndex(file, "/")
		if slash < 0 {
			return nil, fmt.Errorf("line %d: no '/' in file path: %q", i+1, file)
		}
		pkg := file[:slash]

		addStat(a.packages, pkg, numStmts, count)
		addStat(a.files, file, numStmts, count)
	}
	return a, nil
}

func addStat(m map[string]*stat, key string, numStmts, count int) {
	s := m[key]
	if s == nil {
		s = &stat{}
		m[key] = s
	}
	s.total += numStmts
	if count > 0 {
		s.covered += numStmts
	}
}

// violation is a package or file that fell below its threshold.
type violation struct {
	name  string
	pct   float64
	floor float64
}

// check returns the threshold violations and the full sorted report lines.
func (a *analysis) check() (violations []violation, report []string) {
	type row struct {
		name  string
		pct   float64
		floor float64
	}
	var rows []row

	for pkg, s := range a.packages {
		floor := floorDefault
		if criticalPackages[pkg] {
			floor = floorCritical
		}
		rows = append(rows, row{trimModule(pkg), s.pct(), floor})
		if s.pct()+1e-9 < floor {
			violations = append(violations, violation{trimModule(pkg), s.pct(), floor})
		}
	}
	for file := range criticalFiles {
		s := a.files[file]
		if s == nil {
			violations = append(violations, violation{trimModule(file), 0, floorCritical})
			continue
		}
		rows = append(rows, row{trimModule(file), s.pct(), floorCritical})
		if s.pct()+1e-9 < floorCritical {
			violations = append(violations, violation{trimModule(file), s.pct(), floorCritical})
		}
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	for _, r := range rows {
		mark := "ok"
		if r.pct+1e-9 < r.floor {
			mark = "FAIL"
		}
		report = append(report, fmt.Sprintf("  %-4s %6.1f%% (floor %.0f%%)  %s", mark, r.pct, r.floor, r.name))
	}
	return violations, report
}

func trimModule(p string) string {
	if p == modulePath {
		return "(root)"
	}
	return strings.TrimPrefix(p, modulePath+"/")
}

func main() {
	os.Exit(run(os.Args, os.Stdout, os.Stderr))
}

// run does the work and returns the process exit code. main is a thin wrapper so
// that deferred cleanup (file close) runs before the process exits. args, stdout,
// and stderr are injected so the exit-code contract the CI gate relies on is
// directly testable without spawning a subprocess.
func run(args []string, stdout, stderr io.Writer) int {
	// outln/outf write to the gate's output streams. A failed write to stdout or
	// stderr is unrecoverable for a CLI, so the error is intentionally ignored —
	// centralized here so the call sites stay readable.
	outln := func(w io.Writer, a ...any) { _, _ = fmt.Fprintln(w, a...) }
	outf := func(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }

	if len(args) != 2 {
		outln(stderr, "usage: covercheck <coverage-profile>")
		return 2
	}
	// The profile path is an operator-supplied CLI argument to a local dev/CI
	// tool, not untrusted input.
	f, err := os.Open(args[1]) //nolint:gosec // G703: operator-supplied CLI path
	if err != nil {
		outf(stderr, "covercheck: %v\n", err)
		return 2
	}
	defer f.Close() //nolint:errcheck

	a, err := analyze(f)
	if err != nil {
		outf(stderr, "covercheck: parse: %v\n", err)
		return 2
	}

	violations, report := a.check()
	outln(stdout, "coverage by package:")
	for _, line := range report {
		outln(stdout, line)
	}
	if len(violations) > 0 {
		outf(stderr, "\ncovercheck: FAILED — %d package(s)/file(s) below floor:\n", len(violations))
		for _, v := range violations {
			outf(stderr, "  %s: %.1f%% < %.0f%%\n", v.name, v.pct, v.floor)
		}
		return 1
	}
	outln(stdout, "\ncovercheck: PASS — all packages meet their coverage floor")
	return 0
}
