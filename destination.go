package benthos

import (
	"context"
	"errors"
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

func NewDestination() sdk.Destination {
	return &Destination{
		records: make(chan sdk.Record),
		done:    make(chan struct{}),
	}
}

func (d *Destination) Connect(ctx context.Context) error {
	return nil
}

func (d *Destination) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	sdk.Logger(ctx).Info().Msgf("reading record...")
	rec := <-d.records
	sdk.Logger(ctx).Info().Msgf("got a record to read")

	return d.toMessage(rec),
		func(ctx context.Context, err error) error {
			sdk.Logger(ctx).Info().Msgf("service.AckFunc called")
			d.mu.Lock()

			if err != nil {
				if d.errC == nil {
					sdk.Logger(ctx).Info().Msg("Read: initializing error channel")
					d.errC = make(chan batchError)
				}

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
	return nil
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
	sdk.Logger(ctx).Info().Msgf("opening destination...")
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
	builder.AddInputYAML(`
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
		sdk.Logger(ctx).Info().Msgf("running stream...")
		err = stream.Run(benthosCtx)
		sdk.Logger(ctx).Err(err).Msg("benthos: stream done running")
		d.errC <- batchError{err: err}
	}()

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	sdk.Logger(ctx).Info().Msgf("writing %v records...", len(records))
	d.inFlight = len(records)
	for _, r := range records {
		sdk.Logger(ctx).Info().Msg("Write: putting record into channel")
		d.records <- r
	}

	if d.errC == nil {
		sdk.Logger(ctx).Info().Msg("Write: initializing error channel")
		d.errC = make(chan batchError)
	}

	sdk.Logger(ctx).Info().Msg("Write: select")
	select {
	case <-d.done:
		sdk.Logger(ctx).Info().Msgf("done writing records")
		return len(records), nil
	case err := <-d.errC:
		sdk.Logger(ctx).Err(err.err).
			Int("inFlight", d.inFlight).
			Msgf("error while writing records")
		return len(records) - d.inFlight, err.err
	}
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("teardown started...")

	if d.errC != nil {
		d.errC <- batchError{
			err: errors.New("error: teardown called"),
		}
	}

	sdk.Logger(ctx).Info().Msg("teardown done!")

	return nil
}

func (d *Destination) toMessage(rec sdk.Record) *service.Message {
	return service.NewMessage(rec.Bytes())
}
