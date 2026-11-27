GO_BIN_DIR ?= $(or $(shell go env GOBIN),$(shell go env GOPATH)/bin)
SERVER_PACKAGE ?= ./internal/cmd/server
CONF ?= configs
APP_ENV ?= dev
BINARY ?= bin/gateway
RUN_ARGS ?=

.PHONY: gen proto wire run run-only build test check

gen: proto
	$(MAKE) wire

proto:
	cd api && PATH="$(GO_BIN_DIR):$$PATH" buf generate

wire:
	PATH="$(GO_BIN_DIR):$$PATH" wire $(SERVER_PACKAGE)

run: run-only

run-only:
	go run $(SERVER_PACKAGE) -c "$(CONF)" -e "$(APP_ENV)" $(RUN_ARGS)

build:
	go build -trimpath -o "$(BINARY)" $(SERVER_PACKAGE)

test:
	go test ./...

check:
	go vet ./...
	$(MAKE) test
