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
# The Go packages that are OURS. `web/node_modules` can contain vendored Go
# files (some npm packages ship a Go port beside the JavaScript), and the go
# tool walks into them; they are neither ours to format nor ours to lint.
GO_PKGS     := $(shell go list ./... | grep -v '/web/')
GO_DIRS     := $(shell find . -name '*.go' -not -path './web/*' -exec dirname {} \; | sort -u)
# A kubeconfig for a cluster the Kubernetes executor may create and delete pods
# in. It creates and destroys its own namespace; unset, its tests skip.
DHOLE_TEST_KUBECONFIG     ?=
LDFLAGS     := -X $(VERSION_PKG).version=$(VERSION) -X $(VERSION_PKG).commit=$(COMMIT)

.PHONY: check web-check web-e2e test test-race test-integration build clean

## check: the commit gate — formatting, vet, lint. Fails on the first problem.
check:
	@unformatted=$$(gofmt -l $(GO_DIRS)); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt: these files are not formatted:"; echo "$$unformatted"; exit 1; \
	fi
	go vet $(GO_PKGS)
	golangci-lint run $(GO_DIRS)
	buf lint
	@# Breaking-change detection needs a main to compare against; on a branch
	@# whose main has no proto tree yet there is nothing to break.
	buf breaking --against '.git#branch=main' || \
		{ git rev-parse --verify main:proto >/dev/null 2>&1 && exit 1 || \
		  echo "buf breaking: main has no proto tree yet — skipped"; }
	$(MAKE) web-check

## web-check: the web app's half of the gate — typecheck, lint, unit tests.
## It SKIPS when web/node_modules is absent rather than failing: `make check`
## runs on every Go-only task, and a Go change must not be blocked by a
## JavaScript install nobody in that task needs. Run `npm ci` in web/ to
## turn it on; CI always installs, so the checks always run there.
web-check:
	@if [ ! -d web/node_modules ]; then \
		echo "web-check: web/node_modules is absent — skipped (run 'cd web && npm ci' to enable)"; \
	else \
		set -e; \
		npm --prefix web run typecheck; \
		npm --prefix web run lint; \
		npm --prefix web test; \
	fi

## web-e2e: the Playwright suite. NOT part of `make check`: it starts a real
## `dhole serve` and a browser, so it belongs beside test-integration.
web-e2e:
	npm --prefix web run test:e2e

## test-race: the whole suite under the race detector.
##
## Separate from `test` because it is several times slower, and wired into CI
## so a race is caught by the pipeline. A data race in the SSE log stream
## survived review and the gate for two tasks, and was found only because an
## unrelated agent happened to run the suite this way.
test-race:
	go test -race $(GO_PKGS)

## test: the whole suite.
test:
	go test $(GO_PKGS)

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
	go test $(GO_PKGS) -tags=integration
	DHOLE_TEST_KUBECONFIG='$(DHOLE_TEST_KUBECONFIG)' \
	go test ./... -tags=integration

## build: the single binary, stamped with its version and commit.
build:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/$(BINARY)

clean:
	rm -f $(BINARY)
