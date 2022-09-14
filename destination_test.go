package benthos

import (
	"context"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"testing"
	"time"
)

func TestWrite(t *testing.T) {
	is := is.New(t)

	d := NewDestination()
	ctx := context.Background()
	cfg := map[string]string{
		"benthos.yaml": `
output:
  label: "benthos_output_file"
  file:
    path: "/home/haris/projects/other/conduit-utils/benthos-out.txt"
    codec: lines
`}

	d.Configure(ctx, cfg)
	d.Open(ctx)

	go func() {
		for {
			records := []sdk.Record{
				sdk.SourceUtil{}.NewRecordCreate(
					sdk.Position(uuid.NewString()),
					make(sdk.Metadata),
					sdk.RawData(uuid.NewString()),
					sdk.RawData(uuid.NewString()),
				),
			}
			n, err := d.Write(ctx, records)
			is.NoErr(err)
			is.Equal(n, len(records))
			time.Sleep(time.Second)
		}
	}()

	time.Sleep(5 * time.Second)
	d.Teardown(ctx)
	time.Sleep(2 * time.Second)

}
