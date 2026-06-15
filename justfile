_default:
    @just --list

mod examples './examples/justfile'

# Format code
fmt:
    go fmt ./...

# Run tests
test:
    go test ./... -v -race

# Get test coverage
coverage:
    go test ./... -race -coverprofile=coverage.out && \
    go tool cover -html=coverage.out && \
    rm coverage.out

# Run benchmarks and output profiles
bench:
    go test -bench=. -run=^$ -benchmem -cpuprofile cpu.prof -memprofile mem.prof -trace trace.out

# View CPU profile in browser
pprof-cpu:
    go tool pprof -http=:8080 cpu.prof

# View memory profile in browser
pprof-mem:
    go tool pprof -http=:8080 mem.prof

# View execution trace in browser
trace:
    go tool trace trace.out

# Run go vet
vet:
    go vet ./...

# Tidy dependencies
tidy:
    go mod tidy

# Run standard checks
check: fmt vet test

# Clean build caches
clean:
    go clean -cache -testcache && \
    rm cpu.prof mem.prof trace.out
