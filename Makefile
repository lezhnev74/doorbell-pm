VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  = -s -w -X main.version=$(VERSION)
IMAGE   ?= ghcr.io/lezhnev74/doorbell-pm
PLATFORMS ?= linux/amd64,linux/arm64
DIST_TARGETS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

.PHONY: build dist test e2e lint vet cover qa clean image image-push release

build:
	mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/doorbell-pm ./cmd/doorbell-pm

# Static cross-compiled tarballs + sha256 in dist/. CI attaches them to the GitHub Release.
dist:
	rm -rf dist && mkdir -p dist
	@for t in $(DIST_TARGETS); do \
		os=$${t%/*}; arch=$${t#*/}; name=doorbell-pm_$(VERSION)_$${os}_$${arch}; \
		echo "building $$name"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags "$(LDFLAGS)" -o dist/$$name/doorbell-pm ./cmd/doorbell-pm || exit 1; \
		tar -C dist/$$name -czf dist/$$name.tar.gz doorbell-pm && rm -rf dist/$$name; \
	done
	cd dist && sha256sum *.tar.gz > checksums.txt

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
	rm -rf bin dist cover.out

# Local single-arch image, tagged with the current version.
image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

# Manual multi-arch push; needs `docker login ghcr.io` first. CI does this on tags.
image-push:
	docker buildx build --platform $(PLATFORMS) --build-arg VERSION=$(VERSION) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest --push .

# Tag and push; the Release workflow publishes binaries and the image.
# Usage: make release VERSION=v1.2.3
release:
	@case "$(VERSION)" in v[0-9]*) ;; *) echo "usage: make release VERSION=vX.Y.Z"; exit 1;; esac
	@test -z "$$(git status --porcelain)" || { echo "working tree is dirty"; exit 1; }
	git tag -a $(VERSION) -m "$(VERSION)"
	git push origin $(VERSION)
