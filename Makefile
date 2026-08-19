GO      ?= go
BIN     ?= bin
PKGS    := ./...

.PHONY: all build test cover lint fmt vet clean tidy

all: build

build:
	$(GO) build -o $(BIN)/raftlited ./cmd/raftlited
	$(GO) build -o $(BIN)/raftctl   ./cmd/raftctl

test:
	$(GO) test -race -count=1 $(PKGS)

cover:
	$(GO) test -covermode=atomic -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -1

fmt:
	$(GO) fmt $(PKGS)

vet:
	$(GO) vet $(PKGS)

lint: fmt vet

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN) coverage.out coverage/ data/
