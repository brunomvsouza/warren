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

.PHONY: help build test test-stress test-integration test-conformance test-all lint vuln tidy doc hooks clean examples-build examples-smoke integration-up integration-down

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Compile every package.
	$(GO) build $(PKG)

test: ## Run unit tests with race detector and coverage.
	$(GO) test -race -cover $(PKG)

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
	rm -f coverage.out coverage.html coverage.txt
