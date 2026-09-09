VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  = -s -w -X main.version=$(VERSION)

.PHONY: build test e2e lint vet cover qa clean

build:
	mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/doorbell-pm ./cmd/doorbell-pm

test:
	go test -race ./...

e2e:
	go test -race -tags e2e ./test/e2e/ -count=1

vet:
	go vet ./...

lint: vet
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run --build-tags e2e ./... || echo "golangci-lint not installed, skipped"

cover:
	go test -race -coverprofile=cover.out $$(go list ./... | grep -v /tools/)

# CRAP = cyclo^2 * (1-cov)^3 + cyclo per function; fails above 6 or below 85% total coverage.
qa: cover
	go run ./tools/crap -profile cover.out -max 6 -min-cov 85

clean:
	rm -rf bin cover.out
