.PHONY: build test

build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o conduit-connector-benthos cmd/benthos/main.go
	cp conduit-connector-benthos /home/haris/projects/conduitio/conduit/connectors/

test:
	go test $(GOTEST_FLAGS) -v -race ./...
