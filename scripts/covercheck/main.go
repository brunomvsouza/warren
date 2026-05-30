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
		pkg := file[:strings.LastIndex(file, "/")]

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
	os.Exit(run())
}

// run does the work and returns the process exit code. main is a thin wrapper so
// that deferred cleanup (file close) runs before the process exits.
func run() int {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: covercheck <coverage-profile>")
		return 2
	}
	// The profile path is an operator-supplied CLI argument to a local dev/CI
	// tool, not untrusted input.
	f, err := os.Open(os.Args[1]) //nolint:gosec // G703: operator-supplied CLI path
	if err != nil {
		fmt.Fprintf(os.Stderr, "covercheck: %v\n", err)
		return 2
	}
	defer f.Close() //nolint:errcheck

	a, err := analyze(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "covercheck: parse: %v\n", err)
		return 2
	}

	violations, report := a.check()
	fmt.Println("coverage by package:")
	for _, line := range report {
		fmt.Println(line)
	}
	if len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "\ncovercheck: FAILED — %d package(s)/file(s) below floor:\n", len(violations))
		for _, v := range violations {
			fmt.Fprintf(os.Stderr, "  %s: %.1f%% < %.0f%%\n", v.name, v.pct, v.floor)
		}
		return 1
	}
	fmt.Println("\ncovercheck: PASS — all packages meet their coverage floor")
	return 0
}
