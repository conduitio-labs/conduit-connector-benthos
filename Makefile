.PHONY: build test

build:
	go build -o conduit-connector-benthos cmd/benthos/main.go

test:
	go test $(GOTEST_FLAGS) -v -race ./...
