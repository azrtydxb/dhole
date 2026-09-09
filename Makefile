# Dhole build and quality gate. Every task ends with `make check test` green.

BINARY      := dhole
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
VERSION_PKG := github.com/azrtydxb/dhole/internal/version
LDFLAGS     := -X $(VERSION_PKG).version=$(VERSION) -X $(VERSION_PKG).commit=$(COMMIT)

.PHONY: check test build clean

## check: the commit gate — formatting, vet, lint. Fails on the first problem.
check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt: these files are not formatted:"; echo "$$unformatted"; exit 1; \
	fi
	go vet ./...
	golangci-lint run
	buf lint
	@# Breaking-change detection needs a main to compare against; on a branch
	@# whose main has no proto tree yet there is nothing to break.
	buf breaking --against '.git#branch=main' || \
		{ git rev-parse --verify main:proto >/dev/null 2>&1 && exit 1 || \
		  echo "buf breaking: main has no proto tree yet — skipped"; }

## test: the whole suite.
test:
	go test ./...

## build: the single binary, stamped with its version and commit.
build:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/$(BINARY)

clean:
	rm -f $(BINARY)
