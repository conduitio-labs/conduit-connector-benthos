package connector

import (
	"context"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"sync"
)

type Destination struct {
	sdk.UnimplementedDestination

	config  DestinationConfig
	records chan sdk.Record

	benthosStream *service.Stream
	cancelBenthos context.CancelFunc
	errC          chan batchError

	mu       sync.Mutex
	inFlight int
	done     chan struct{}
}

type batchError struct {
	err     error
	written int
}

func (d *Destination) Connect(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (d *Destination) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	rec := <-d.records
	return d.toMessage(rec),
		func(ctx context.Context, err error) error {
			d.mu.Lock()

			if err != nil {
				d.errC <- batchError{
					err:     err,
					written: d.inFlight,
				}
			}

			d.inFlight--
			if d.inFlight == 0 {
				d.done <- struct{}{}
			}

			d.mu.Unlock()

			return nil
		},
		nil
}

func (d *Destination) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func NewDestination() sdk.Destination {
	return &Destination{}
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		BenthosYaml: {
			Default:     "localhost:10000",
			Required:    true,
			Description: "The URL of the server.",
		},
	}
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring a Destination connector...")
	config, err := ParseDestinationConfig(cfg)
	if err != nil {
		return err
	}
	d.config = config
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	builder := service.NewStreamBuilder()
	builder.DisableLinting()
	// Register our new output, which doesn't require a config schema.
	err := service.RegisterInput(
		"conduit_destination_input",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return service.AutoRetryNacks(d), nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed registering Benthos output: %w", err)
	}

	err = builder.SetYAML(d.config.benthosYaml)
	if err != nil {
		return fmt.Errorf("failed parsing Benthos YAML config: %w", err)
	}
	builder.AddOutputYAML(`
label: "label_conduit_destination_input"
conduit_destination_input: {}
`)

	// Build a stream with our configured components.
	stream, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed building Benthos stream: %w", err)
	}
	d.benthosStream = stream

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message, and it has flushed through the stream.
	benthosCtx, cancelBenthos := context.WithCancel(context.Background())
	d.cancelBenthos = cancelBenthos

	go func() {
		err = stream.Run(benthosCtx)
		sdk.Logger(ctx).Err(err).Msg("benthos: stream done running")
		d.errC <- batchError{err: err}
	}()

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	d.inFlight = len(records)
	for _, r := range records {
		d.records <- r
	}

	select {
	case <-d.done:
		return len(records), nil
	case err := <-d.errC:
		return len(records) - d.inFlight, err.err
	}
}

func (d *Destination) Teardown(ctx context.Context) error {
	return nil
}

func (d *Destination) toMessage(rec sdk.Record) *service.Message {
	return service.NewMessage(rec.Bytes())
}
