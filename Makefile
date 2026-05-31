.DEFAULT_GOAL := help

GO        ?= go
GOLANGCI  ?= golangci-lint
PKG       := ./...

# govulncheck is pinned so the analysis logic is reproducible across runs and CI
# (the vulnerability DB itself is always fetched fresh at invocation time). Bump
# this deliberately in a dedicated commit, like any other tool version.
GOVULNCHECK_VERSION ?= v1.3.0

# Read the 'go X.Y.Z' line from go.mod and derive the toolchain selector.
GO_MOD_VERSION := $(shell awk '/^go [0-9]/{print $$2}' go.mod)
GOTOOLCHAIN     ?= go$(GO_MOD_VERSION)

# Repetition depth for the stress lane. The set of scheduling-sensitive tests is declared
# in stress_test.go behind the 'stress' build tag (TestStress), not as a -run regex here,
# so membership stays next to the code and a rename breaks the build instead of going stale.
STRESS_COUNT ?= 200

# Cluster lane (Phase 9.5) endpoints. Defaults match the host port mapping in
# docker-compose.cluster.yml (Toxiproxy-fronted AMQP ports 5680/5681/5682, rmq0
# management on 15672, Toxiproxy control API on 8474). Override to point the lane
# at a standing cluster (LATER-49).
WARREN_CLUSTER_NODES ?= amqp://guest:guest@localhost:5680/,amqp://guest:guest@localhost:5681/,amqp://guest:guest@localhost:5682/
WARREN_CLUSTER_MGMT  ?= http://guest:guest@localhost:15672
WARREN_TOXIPROXY_URL ?= http://localhost:8474

.PHONY: help build test test-stress test-integration test-conformance test-all lint vuln tidy doc hooks clean examples-build examples-smoke integration-up integration-down cluster-up cluster-down test-cluster cover bench

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Compile every package.
	$(GO) build $(PKG)

test: ## Run unit tests with race detector and coverage.
	$(GO) test -race -cover $(PKG)

cover: ## Generate coverage.out and enforce per-package (>=80%) + critical-path (>=95%) floors (CI gate).
	GO=$(GO) ./scripts/coverage.sh coverage.out

integration-up: ## Start RabbitMQ broker locally via Docker Compose (waits for healthy).
	docker compose -f docker-compose.integration.yml up -d --wait

integration-down: ## Stop and remove the local RabbitMQ broker.
	docker compose -f docker-compose.integration.yml down

test-integration: ## Run integration tests (requires AMQP_TEST_URL + AMQP_TEST_MANAGEMENT_URL; 'make integration-up' starts the broker).
	$(GO) test -race -tags=integration $(PKG)

test-conformance: ## Run AMQP 0-9-1 conformance tests (requires Docker).
	$(GO) test -race -tags=conformance $(PKG)

test-all: ## Run unit + integration + conformance tests.
	$(GO) test -race -cover -tags='integration conformance' $(PKG)

test-stress: ## Hammer scheduling-sensitive tests (stress build tag) under -race to guard determinism (override STRESS_COUNT).
	$(GO) test -race -tags=stress -count=$(STRESS_COUNT) -run '^TestStress$$' .

cluster-up: ## Start the 3-node RabbitMQ cluster + Toxiproxy via Docker Compose (waits for healthy).
	docker compose -f docker-compose.cluster.yml up -d --wait

cluster-down: ## Stop and remove the local RabbitMQ cluster + Toxiproxy.
	docker compose -f docker-compose.cluster.yml down

# Cluster lane: runs the cluster build-tag tests against the compose cluster, with
# a zero-run guard implementing the TV-13 pattern (the planned integration TV-13
# CI gate, T151 — note `test-integration` above has no inline guard yet, so this is
# the first concrete instance): if no Test*_cluster function actually executed, the
# broker-required lane asserted nothing and the target must FAIL rather than pass
# green. The cluster helpers t.Fatal when the
# WARREN_CLUSTER_* vars are unset, so this guards the regression where a test
# starts to t.Skip instead. The go test exit status is preserved through the log
# capture (no pipefail dependency, so it stays /bin/sh-portable).
test-cluster: ## Run cluster tests (requires the compose cluster; 'make cluster-up' starts it) + zero-run guard.
	@WARREN_CLUSTER_NODES="$(WARREN_CLUSTER_NODES)" \
	WARREN_CLUSTER_MGMT="$(WARREN_CLUSTER_MGMT)" \
	WARREN_TOXIPROXY_URL="$(WARREN_TOXIPROXY_URL)" \
	$(GO) test -race -v -tags=cluster $(PKG) > cluster.log 2>&1; status=$$?; \
	cat cluster.log; \
	ran=$$(grep -cE '^[[:space:]]*--- (PASS|FAIL): Test[A-Za-z0-9_/]*_cluster' cluster.log) || true; \
	echo "cluster tests executed: $${ran}"; \
	if [ "$${ran:-0}" -eq 0 ]; then \
		echo "zero-run guard: zero cluster tests executed against the cluster." >&2; \
		exit 1; \
	fi; \
	exit $$status

bench: ## Run throughput benchmarks (bench build tag; requires broker via AMQP_TEST_URL). Reports msg/s per classic+quorum.
	$(GO) test -tags=bench -run='^$$' -bench=. -benchmem -timeout=30m $(PKG)

tidy: ## Tidy go.mod/go.sum using the Go version declared in go.mod (prevents toolchain drift).
	GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) mod tidy

lint: ## Run golangci-lint.
	$(GOLANGCI) run $(PKG)

vuln: ## Scan dependencies for known vulnerabilities (govulncheck; fails only on a vuln warren actually calls).
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) $(PKG)

doc: ## Serve godoc locally on :6060.
	$(GO) run golang.org/x/tools/cmd/godoc@latest -http=:6060

hooks: ## Install pre-commit hook running lint+test (opt-in).
	@if [ ! -d .git ]; then echo "Not a git repo — nothing to install." && exit 1; fi
	@printf '#!/bin/sh\nexec make lint test\n' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Installed .git/hooks/pre-commit (runs 'make lint test')."

examples-build: ## Build all examples (unit lane; no broker required).
	$(GO) build ./examples/...

examples-smoke: ## Smoke-run example integration tests (requires broker via AMQP_TEST_URL).
	$(GO) test -race -tags=integration ./examples/...

clean: ## Remove build and test artifacts.
	$(GO) clean -testcache
	rm -f coverage.out coverage.html coverage.txt cluster.log integration.log conformance.log
