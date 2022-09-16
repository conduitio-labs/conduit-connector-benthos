package benthos

import (
	"context"
	"fmt"
	_ "github.com/benthosdev/benthos/v4/public/components/all"
	"github.com/benthosdev/benthos/v4/public/service"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"time"
)

type Source struct {
	sdk.UnimplementedSource

	config           SourceConfig
	lastPositionRead sdk.Position

	cancelBenthos context.CancelFunc
	errC          chan error
	messages      chan *service.Message
	benthosStream *service.Stream
}

func NewSource() sdk.Source {
	return &Source{
		errC:     make(chan error),
		messages: make(chan *service.Message),
	}
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		BenthosYaml: {
			Default:     "",
			Required:    true,
			Description: "Benthos YAML configuration",
		},
	}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Debug().Msg("Configuring a Source Connector...")
	config, err := ParseSourceConfig(cfg)
	if err != nil {
		return err
	}

	s.config = config
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	builder := service.NewStreamBuilder()
	builder.DisableLinting()
	// Register our new output, which doesn't require a config schema.
	err := service.RegisterOutput(
		"conduit_source_output",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return s, 1, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed registering Benthos output: %w", err)
	}

	err = builder.SetYAML(s.config.benthosYaml)
	if err != nil {
		return fmt.Errorf("failed parsing Benthos YAML config: %w", err)
	}
	builder.AddOutputYAML(`
label: "testoutput"
conduit_source_output: {}
`)

	// Build a stream with our configured components.
	stream, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed building Benthos stream: %w", err)
	}
	s.benthosStream = stream

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message, and it has flushed through the stream.
	benthosCtx, cancelBenthos := context.WithCancel(context.Background())
	s.cancelBenthos = cancelBenthos

	go func() {
		err = stream.Run(benthosCtx)
		sdk.Logger(ctx).Err(err).Msg("benthos: stream done running")
		s.errC <- err
	}()

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	sdk.Logger(ctx).Debug().Msg("benthos: read")
	select {
	case m := <-s.messages:
		return s.toRecord(m)
	case <-time.After(10 * time.Second):
		return sdk.Record{}, sdk.ErrBackoffRetry
	case err := <-s.errC:
		sdk.Logger(ctx).Err(err).Msg("benthos read: got error")
		return sdk.Record{}, fmt.Errorf("got error from benthos stream: %w", err)
	}
}

func (s *Source) toRecord(m *service.Message) (sdk.Record, error) {
	payload, err := m.AsBytes()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed converting Benthos message to bytes: %w", err)
	}
	meta := make(sdk.Metadata)
	m.MetaWalk(func(k string, v string) error {
		meta[k] = v
		return nil
	})
	return sdk.SourceUtil{}.NewRecordCreate(
		sdk.Position(uuid.NewString()),
		meta,
		sdk.RawData(uuid.NewString()),
		sdk.RawData(payload),
	), nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Msg("benthos: ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msg("benthos teardown")
	if s.cancelBenthos != nil {
		s.cancelBenthos()
	}
	return nil
}

// --------------------------------------------------
// Implement service.Output from Benthos

func (s *Source) Connect(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msg("benthos close")
	return nil
}

func (s *Source) Write(ctx context.Context, message *service.Message) error {
	sdk.Logger(ctx).Debug().Msg("benthos write")
	s.messages <- message
	return nil
}

func (s *Source) Close(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msg("benthos close")
	return nil
}
