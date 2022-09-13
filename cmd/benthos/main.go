package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	benthos "github.com/conduitio-labs/conduit-connector-benthos"
)

func main() {
	sdk.Serve(benthos.Connector)
}
