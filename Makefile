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
# A containerd socket the containerd executor may create and delete containers
# on, in its own containerd namespace. Unset, its tests skip. The process
# running the tests needs write access to the socket AND to the FIFO directory
# containerd creates exec streams in (/run/containerd/fifo), which on a
# packaged containerd means root.
DHOLE_TEST_CONTAINERD_SOCK ?=
LDFLAGS     := -X $(VERSION_PKG).version=$(VERSION) -X $(VERSION_PKG).commit=$(COMMIT)
# The golangci-lint CI installs, and the one `make check` expects to find. Two
# versions of the same linter do not report the same findings — gosec's G602
# fired in CI on v2.6.2 over code a developer's v2.13.2 passed — so a gate run
# on another version is a different gate, and `make check` says so.
GOLANGCI_LINT_VERSION := v2.13.2
# Every operating system the tree carries build-tagged files for. The linter
# only sees the files that build for the GOOS it runs under, so a gate run on
# a Mac never linted the Linux-only VM guest agent, and one run on Linux never
# lints the Windows Job Object engine: both had findings that were red only
# on the machine nobody was sitting at. Each pass cross-typechecks; nothing is
# compiled or executed for the target, so all three run on any host.
LINT_GOOS   ?= linux darwin windows
# The buf CI installs for `buf lint` and `buf breaking`. The gate had never
# reached its buf steps in CI, because lint failed first; when it did, buf was
# not installed at all.
BUF_VERSION := v1.72.0
# go test's own default is ten minutes PER PACKAGE, and internal/server — a
# whole plane started over and over — takes about nine on an amd64 runner and
# more on arm64, where it was killed by that default mid-test.
TEST_TIMEOUT ?= 30m

.PHONY: check lint golangci-lint-version buf-version web-check web-build web-e2e test test-race test-integration conformance build clean
.PHONY: acceptance acceptance-ci acceptance-automation acceptance-agent

## check: the commit gate — formatting, vet, lint. Fails on the first problem.
check:
	@unformatted=$$(gofmt -l $(GO_DIRS)); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt: these files are not formatted:"; echo "$$unformatted"; exit 1; \
	fi
	go vet $(GO_PKGS)
	$(MAKE) lint
	buf lint
	@# Breaking-change detection needs a main to compare against; on a branch
	@# whose main has no proto tree yet there is nothing to break.
	buf breaking --against '.git#branch=main' || \
		{ git rev-parse --verify main:proto >/dev/null 2>&1 && exit 1 || \
		  echo "buf breaking: main has no proto tree yet — skipped"; }
	$(MAKE) web-check

## lint: golangci-lint over every package, once per GOOS in LINT_GOOS.
## A version other than GOLANGCI_LINT_VERSION is WARNED about rather than
## refused, so a machine with a newer linter can still commit — but the warning
## is the reason a finding CI reports can be absent here.
##
## --allow-parallel-runners because the linter's lock is one file in the
## system temp directory, shared by every checkout on the machine: without it a
## second worktree's gate fails outright while the first one lints. Nothing
## here runs --fix, which is the only thing the lock protects against.
lint:
	@want=$(GOLANGCI_LINT_VERSION); have=$$(golangci-lint --version 2>/dev/null | sed -n 's/.*version v\{0,1\}\([^ ]*\).*/v\1/p'); \
	if [ "$$have" != "$$want" ]; then \
		echo "lint: WARNING golangci-lint is $${have:-missing}, CI runs $$want — findings may differ from CI's"; \
		echo "lint:   go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$$want"; \
	fi
	@set -e; for goos in $(LINT_GOOS); do \
		echo "golangci-lint (GOOS=$$goos)"; \
		GOOS=$$goos golangci-lint run --allow-parallel-runners $(GO_DIRS); \
	done

## golangci-lint-version: the pinned linter version, for CI to install.
golangci-lint-version:
	@echo $(GOLANGCI_LINT_VERSION)

## buf-version: the pinned buf version, for CI to install.
buf-version:
	@echo $(BUF_VERSION)

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
	go test -race -timeout $(TEST_TIMEOUT) $(GO_PKGS)

## test: the whole suite.
test:
	go test -timeout $(TEST_TIMEOUT) $(GO_PKGS)

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
	DHOLE_TEST_CONTAINERD_SOCK='$(DHOLE_TEST_CONTAINERD_SOCK)' \
	go test ./... -tags=integration

## acceptance-*: the three acceptance pipelines — the definition of done for
## v1 (ADR 0016). One target per profile, because they are three separate
## claims and a single target that ran all three would report one verdict for
## three questions.
##
## They are NOT part of `make test`. Each one starts a control plane, talks to
## a live Postgres and creates and destroys pods in a real Kubernetes cluster;
## with nothing running they skip themselves, naming what is missing.
##
## -count=1 because a cached PASS is not a run, and these are exactly the
## tests whose value is that they ran today.
##
## Set DHOLE_TEST_KUBECONFIG to a kubeconfig for a cluster the tests may create
## and delete their OWN namespace in. They never touch a namespace they did not
## create.
ACCEPTANCE_FLAGS ?= -count=1 -v -timeout 30m

acceptance-ci:
	DHOLE_TEST_KUBECONFIG='$(DHOLE_TEST_KUBECONFIG)' \
	go test ./acceptance $(ACCEPTANCE_FLAGS) -run TestAcceptanceCICacheHit

acceptance-automation:
	DHOLE_TEST_KUBECONFIG='$(DHOLE_TEST_KUBECONFIG)' \
	DHOLE_TEST_POSTGRES_DSN='$(DHOLE_TEST_POSTGRES_DSN)' \
	go test ./acceptance $(ACCEPTANCE_FLAGS) -run TestAcceptanceAutomationTriggersAndWait

acceptance-agent:
	DHOLE_TEST_POSTGRES_DSN='$(DHOLE_TEST_POSTGRES_DSN)' \
	go test ./acceptance $(ACCEPTANCE_FLAGS) -run TestAcceptanceAgentLoopAndApproval

## acceptance: all three, in the order the profiles were designed in. It stops
## at the first failure, which is what you want from a definition of done.
acceptance: acceptance-ci acceptance-automation acceptance-agent

## conformance: run the engine conformance suite against ANY engine command.
## This is the executable half of docs/wire-contract.md: it starts its own NATS
## and object store, plays the control plane, and runs one case per obligation.
## An engine author in any language runs exactly this to find out whether their
## engine complies:
##
##   make conformance ENGINE="python3 testdata/engines/minimal-python/engine.py"
##
## ENGINE is the command to launch, split on spaces. Add -v to CONFORMANCE_FLAGS
## to see the engine's own stdout and stderr.
CONFORMANCE_FLAGS ?=
conformance:
	@if [ -z "$(ENGINE)" ]; then \
		echo 'make conformance: set ENGINE, e.g. ENGINE="python3 testdata/engines/minimal-python/engine.py"'; \
		exit 2; \
	fi
	go run ./conformance/cmd/dhole-conformance $(CONFORMANCE_FLAGS) $(ENGINE)

## web-build: build the GUI and stage it for embedding into the control plane.
##
## The plane serves the app from its own binary and its own origin. Every call
## the GUI makes — Connect RPCs and two long-lived SSE streams — is a
## cross-origin request when the app is hosted elsewhere, so a separately
## served GUI does not work until someone sets controlPlane.allowedOrigins and
## fails silently in the browser until they work out that is why.
##
## It SKIPS when web/node_modules is absent, like web-check, so a Go-only
## checkout still builds: the result is a plane with no GUI, which is a
## perfectly good plane (ADR 0013 — the GUI is one API client among several).
web-build:
	@if [ ! -d web/node_modules ]; then \
		echo "web-build: skipped (web/node_modules absent; run npm ci in web/)"; \
		exit 0; \
	fi
	cd web && npm run build
	rm -rf internal/webui/dist
	mkdir -p internal/webui/dist
	cp -R web/dist/. internal/webui/dist/
	# Restored because the rm above takes it with the stale build, and a fresh
	# checkout needs the directory to exist or `go:embed all:dist` cannot
	# compile — which would make a Go-only checkout unbuildable.
	cp internal/webui/gitkeep.txt internal/webui/dist/.gitkeep
	@echo "web-build: staged web/dist into internal/webui/dist"

## build: the single binary, stamped with its version and commit.
##
## Depends on web-build so a release binary carries the GUI. The dependency is
## one-way and skippable: no npm, no GUI, still a binary.
build: web-build
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/$(BINARY)

clean:
	rm -f $(BINARY)
