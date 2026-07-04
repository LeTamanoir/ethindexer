_default:
    @just --list

mod examples './examples/justfile'

# Format code
fmt:
    go fmt ./...

# Run tests
test:
    go test -v -race

# Run benchmarks
bench:
    go test -bench=.

# Run go vet
vet:
    go vet

# Tidy dependencies
tidy:
    go mod tidy

# Run standard checks
check: fmt vet test

# Clean build caches
clean:
    go clean -cache -testcache
 
# Create and push an annotated release tag
release version:
    @test -n "{{version}}" || (echo "usage: just release v0.1.0" && exit 1)
    @echo "{{version}}" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$$' || (echo "invalid semver tag: {{version}}" && exit 1)
    go test ./...
    git diff --quiet || (echo "working tree is dirty" && exit 1)
    git tag -a "{{version}}" -m "{{version}}"
    git push origin "{{version}}"
