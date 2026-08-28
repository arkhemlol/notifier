SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

FUZZTIME ?= 90s
TEST_TIMEOUT ?= 30s
BENCH_COUNT ?= 10
BENCH_QUERY ?= *
BENCH_FILE ?= .bench/latest.txt
BENCH_BASE ?= .bench/base.txt
BENCH_SERIES_FILE ?= .bench/series.csv
BENCH_SERVER ?=
PROFILE_PACKAGE ?= .
PROFILE_BENCH ?= .
PROFILE_CPU ?= .bench/cpu.pprof
PROFILE_MEM ?= .bench/mem.pprof
PROFILE_BINARY ?= .bench/profile.test

MODULE ?= github.com/arkhemlol/notifier
SIZE_DIR ?= .size
SIZE_PACKAGES ?= $(MODULE) $(MODULE)/email $(MODULE)/telegram $(MODULE)/store/memory $(MODULE)/store/sqlite
# No -ldflags="-s -w" here on purpose: gsa attributes bytes to packages from the
# symbol table and DWARF, and stripping both leaves it guessing from
# disassembly alone.
SIZE_BUILD_FLAGS ?= -trimpath -buildvcs=false
GSA_FLAGS ?= -f text

.PHONY: lint test test\:fuzz deadcode build-size bench bench-filter benchstat bench-series bench-upload deps profile pprof-cpu pprof-mem loc loc-all

lint:
	gofumpt -l -w .
	go vet ./...
	go fix ./...
	golangci-lint run --config golangci.yml --fix
	govulncheck -show verbose ./...

test:
	go test -cover -race -run '^Test' -timeout "$(TEST_TIMEOUT)" -v ./...

test\:fuzz:
	go test -run '^$$' -fuzz '^FuzzBuildMessage$$' -fuzztime "$(FUZZTIME)" -v ./email
	go test -run '^$$' -fuzz '^FuzzResolveChatID$$' -fuzztime "$(FUZZTIME)" -v ./telegram

deadcode:
	deadcode -test ./...

# The target links a probe binary that imports every package and hands it to
# gsa, which breaks the result down per package and section
build-size:
	@command -v gsa >/dev/null || { printf 'gsa not found: go install github.com/Zxilly/go-size-analyzer/cmd/gsa@latest\n' >&2; exit 1; }
	@mkdir -p -- "$(SIZE_DIR)"
	@work=$$(mktemp -d "$(SIZE_DIR)/probe.XXXXXX"); \
	trap 'rm -rf -- "$$work"' EXIT; \
	{ printf 'package main\n\nimport (\n'; \
	  for pkg in $(SIZE_PACKAGES); do printf '\t_ "%s"\n' "$$pkg"; done; \
	  printf ')\n\nfunc main() {}\n'; } > "$$work/main.go"; \
	CGO_ENABLED=0 go build $(SIZE_BUILD_FLAGS) -o "$$work/probe.bin" "./$$work"; \
	gsa $(GSA_FLAGS) "$$work/probe.bin"

bench:
	mkdir -p .bench
	go test -run '^$$' -bench . -benchmem -count "$(BENCH_COUNT)" ./... > "$(BENCH_FILE)"
	cat "$(BENCH_FILE)"

bench-filter:
	benchfilter '$(BENCH_QUERY)' "$(BENCH_FILE)"

benchstat:
	benchstat "$(BENCH_BASE)" "$(BENCH_FILE)"

bench-series:
	mkdir -p .bench
	benchseries -csv "$(BENCH_FILE)" > "$(BENCH_SERIES_FILE)"
	cat "$(BENCH_SERIES_FILE)"

bench-upload:
	@test -n "$(BENCH_SERVER)" || (echo "BENCH_SERVER is required; this target uploads benchmark data over the network" >&2; exit 2)
	benchsave -server "$(BENCH_SERVER)" "$(BENCH_FILE)"

deps:
	goda tree ./...

# Go profile flags write one package's output. Override both variables when the
# benchmark is outside the root package, for example PROFILE_PACKAGE=./email.
profile:
	mkdir -p .bench
	go test -run '^$$' -bench "$(PROFILE_BENCH)" -cpuprofile "$(PROFILE_CPU)" -memprofile "$(PROFILE_MEM)" -o "$(PROFILE_BINARY)" "$(PROFILE_PACKAGE)"

pprof-cpu:
	go tool pprof -top "$(PROFILE_CPU)"

pprof-mem:
	go tool pprof -top "$(PROFILE_MEM)"

loc-all:
	scc
loc:
	scc --exclude-file "_test.go"
