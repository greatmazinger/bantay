GOBIN      := $(shell go env GOPATH)/bin
PROTO_ROOT := proto
PROTO_SRC  := jobmanager/v1/jobmanager.proto

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

.PHONY: tools
tools: ## Install the protoc plugins into GOPATH/bin
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

.PHONY: proto
proto: ## Regenerate Go code from the .proto definitions
	PATH="$(GOBIN):$$PATH" protoc \
	  --proto_path=$(PROTO_ROOT) \
	  --go_out=$(PROTO_ROOT)      --go_opt=paths=source_relative \
	  --go-grpc_out=$(PROTO_ROOT) --go-grpc_opt=paths=source_relative \
	  $(PROTO_SRC)

.PHONY: certs
certs: ## Generate the CA, server, and client certificates into ./certs
	go run ./cmd/gencerts

.PHONY: build
build: ## Build the binaries into ./bin
	go build -o bin/ ./cmd/jobworker ./cmd/jobctl ./cmd/gencerts

.PHONY: test
test: ## Run the full suite with the race detector
	go test -race -count=1 ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: check
check: vet test ## Vet and test

.PHONY: harness
harness: ## Run the concurrent-reader harness
	go run ./cmd/harness

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	go mod tidy

.PHONY: clean
clean: ## Remove build artifacts
	go clean
	rm -rf bin/
