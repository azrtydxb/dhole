# Dhole build and quality gate. Every task ends with `make check test` green.

BINARY      := dhole
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
VERSION_PKG := github.com/azrtydxb/dhole/internal/version

# Endpoints for `make test-integration`, matching docker-compose.test.yml.
# Overridable so the suite can be pointed at services running elsewhere.
DHOLE_TEST_S3_ENDPOINT    ?= http://127.0.0.1:59000
DHOLE_TEST_S3_ACCESS_KEY  ?= dholetest
DHOLE_TEST_S3_SECRET_KEY  ?= dholetestsecret
DHOLE_TEST_POSTGRES_DSN   ?= postgres://dhole:dholetestsecret@127.0.0.1:55432/dhole?sslmode=disable
DHOLE_TEST_OCI_REGISTRY   ?= 127.0.0.1:55000
LDFLAGS     := -X $(VERSION_PKG).version=$(VERSION) -X $(VERSION_PKG).commit=$(COMMIT)

.PHONY: check test test-integration build clean

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

## test-integration: the suite with the services in docker-compose.test.yml
## reachable. Start them first with `docker compose -f docker-compose.test.yml
## up -d`. Every integration test skips itself when its endpoint variable is
## unset, so plain `make test` stays green with nothing running.
test-integration:
	DHOLE_TEST_S3_ENDPOINT=$(DHOLE_TEST_S3_ENDPOINT) \
	DHOLE_TEST_S3_ACCESS_KEY=$(DHOLE_TEST_S3_ACCESS_KEY) \
	DHOLE_TEST_S3_SECRET_KEY=$(DHOLE_TEST_S3_SECRET_KEY) \
	DHOLE_TEST_POSTGRES_DSN='$(DHOLE_TEST_POSTGRES_DSN)' \
	DHOLE_TEST_OCI_REGISTRY=$(DHOLE_TEST_OCI_REGISTRY) \
	go test ./... -tags=integration

## build: the single binary, stamped with its version and commit.
build:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/$(BINARY)

clean:
	rm -f $(BINARY)
