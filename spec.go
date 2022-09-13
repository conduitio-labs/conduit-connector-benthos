package connector

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "benthos",
		Summary:     "Conduit wrapper for Benthos.",
		Description: "Conduit wrapper for Benthos inputs, processors and outputs.",
		Version:     "v0.1.0",
		Author:      "Meroxa, Inc.",
	}
}
