.DEFAULT_GOAL := help

GO        ?= go
GOLANGCI  ?= golangci-lint
PKG       := ./...

.PHONY: help build test test-integration test-conformance test-all lint mocks doc hooks clean

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Compile every package.
	$(GO) build $(PKG)

test: ## Run unit tests with race detector and coverage.
	$(GO) test -race -cover $(PKG)

test-integration: ## Run integration tests (requires Docker).
	$(GO) test -race -tags=integration $(PKG)

test-conformance: ## Run AMQP 0-9-1 conformance tests (requires Docker).
	$(GO) test -race -tags=conformance $(PKG)

test-all: ## Run unit + integration + conformance tests.
	$(GO) test -race -cover -tags='integration conformance' $(PKG)

lint: ## Run golangci-lint.
	$(GOLANGCI) run $(PKG)

mocks: ## Regenerate gomock mocks.
	$(GO) generate $(PKG)

doc: ## Serve godoc locally on :6060.
	$(GO) run golang.org/x/tools/cmd/godoc@latest -http=:6060

hooks: ## Install pre-commit hook running lint+test (opt-in).
	@if [ ! -d .git ]; then echo "Not a git repo — nothing to install." && exit 1; fi
	@printf '#!/bin/sh\nexec make lint test\n' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Installed .git/hooks/pre-commit (runs 'make lint test')."

clean: ## Remove build and test artifacts.
	$(GO) clean -testcache
	rm -f coverage.out coverage.html coverage.txt
