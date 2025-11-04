.PHONY: build bench vet lint test clean run

build:
	@echo "Building data generator..."
	@go build -o bin/gendata ./cmd/gendata
	@if [ -d "./cmd/bench" ] && [ -n "$$(ls -A ./cmd/bench/*.go 2>/dev/null)" ]; then \
		go build -o bin/bench ./cmd/bench; \
	fi

bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

vet:
	@echo "Running go vet..."
	@go vet ./...

lint:
	@echo "Running golangci-lint (if installed)..."
	@which golangci-lint > /dev/null && golangci-lint run ./... || echo "golangci-lint not installed, skipping"

test:
	@echo "Running tests..."
	@go test -v ./...

clean:
	@echo "Cleaning..."
	@rm -rf bin/

run:
	@go run ./cmd/gendata

