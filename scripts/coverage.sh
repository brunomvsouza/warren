#!/usr/bin/env bash
#
# Generate a Go coverage profile across the library packages and enforce the
# per-package (>= 80%) and critical-path (>= 95%) coverage floors via
# scripts/covercheck. Exits non-zero when any package/file is below its floor.
#
# Excluded from the floor:
#   - examples/*   — main packages, smoke-tested on the integration lane, not unit
#   - scripts/*    — tooling (this checker)
#   - internal/amqptest — testcontainers helper, no unit tests by design
#
# Usage: scripts/coverage.sh [profile-path]   (default: coverage.out)
# Env:   GO (default: go), GOTESTFLAGS (e.g. "-race" to add the race detector)
set -euo pipefail

GO="${GO:-go}"
PROFILE="${1:-coverage.out}"

pkgs=$("$GO" list ./... | grep -vE '/(examples|scripts)/|/internal/amqptest$')

# shellcheck disable=SC2086
"$GO" test ${GOTESTFLAGS:-} -covermode=atomic -coverprofile="$PROFILE" $pkgs

"$GO" run ./scripts/covercheck "$PROFILE"
