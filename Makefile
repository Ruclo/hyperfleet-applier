.DEFAULT_GOAL := help

GO ?= go
GOFMT ?= gofmt

ENVTEST_K8S_VERSION ?= 1.36.2

BIN_DIR := bin
BINARY_NAME := $(BIN_DIR)/hyperfleet-applier

BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
APP_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "0.0.0-dev")

GOFLAGS ?= -trimpath
LDFLAGS := -s -w \
	-X main.version=$(APP_VERSION) \
	-X main.commit=$(GIT_SHA) \
	-X main.date=$(BUILD_DATE)

CONFIG ?= configs/applier.yaml
KUBE_CONFIG_PATH ?= $(if $(KUBECONFIG),$(KUBECONFIG),$(HOME)/.kube/config)

# Invoke a pinned tool: $(call gotool,name)
# All tools share tools/go.mod with Go 1.24+ tool directives.
TOOL_MOD := tools/go.mod
gotool = "$(GO)" tool -modfile="$(TOOL_MOD)" $(1)

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: build
build: ## Build the applier binary
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY_NAME) ./cmd

.PHONY: run
run: build ## Run the applier service
	./$(BINARY_NAME) serve \
		--config "$(CONFIG)" \
		--kubernetes-kube-config-path "$(KUBE_CONFIG_PATH)"

.PHONY: test
test: ## Run unit tests
	$(GO) test -v -race -coverprofile=coverage.out ./...

.PHONY: test-envtest
test-envtest: ## Run envtest-backed integration tests against a real kube-apiserver
	@assets=$$($(call gotool,setup-envtest) use -i -p path $(ENVTEST_K8S_VERSION)); \
	if [ -z "$$assets" ]; then \
		echo "setup-envtest: failed to resolve installed assets for $(ENVTEST_K8S_VERSION)"; \
		exit 1; \
	fi; \
	KUBEBUILDER_ASSETS="$$assets" $(GO) test -race -tags envtest ./... -run Envtest -v

.PHONY: fmt
fmt: ## Format Go code
	$(GOFMT) -s -w .

.PHONY: gofmt
gofmt: fmt ## Alias for fmt

.PHONY: fmt-check
fmt-check: ## Check if code is formatted
	@diff=$$($(GOFMT) -s -d .); \
	if [ -n "$$diff" ]; then \
		echo "Code is not formatted. Run 'make fmt' to fix:"; \
		echo "$$diff"; \
		exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: go-vet
go-vet: vet ## Alias for vet

.PHONY: lint
lint: ## Run golangci-lint
	$(call gotool,golangci-lint) run

.PHONY: verify
verify: fmt-check vet ## Run all verification checks

.PHONY: lint-check
lint-check: fmt-check vet ## Run static code analysis (alias for verify, follows architecture naming)

##@ Dependencies

.PHONY: tidy
tidy: ## Tidy go.mod
	$(GO) mod tidy

.PHONY: tools
tools: ## Ensure tool dependencies are up to date
	cd tools && "$(GO)" mod tidy

.PHONY: verify-tools
verify-tools: tools ## Fail in CI if tool module drifted
	@git diff --exit-code HEAD -- tools/go.mod tools/go.sum || (echo "tool modules out of date; run 'make tools'" && exit 1)

.PHONY: download
download: ## Download dependencies
	$(GO) mod download
