package connector

import (
	"context"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
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
	sdk.Logger(ctx).Info().Msg("Configuring a Source Connector...")
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
		s.errC <- err
	}()

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	select {
	case m := <-s.messages:
		return toRecord(m)
	case err := <-s.errC:
		return sdk.Record{}, err
	}
}

func toRecord(m *service.Message) (sdk.Record, error) {
	payload, err := m.AsBytes()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed converting Benthos message to bytes: %w", err)
	}
	return sdk.SourceUtil{}.NewRecordCreate(
		sdk.Position(uuid.NewString()),
		make(sdk.Metadata),
		sdk.RawData(uuid.NewString()),
		sdk.RawData(payload),
	), nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	s.cancelBenthos()
	return nil
}

// --------------------------------------------------
// Implement service.Output from Benthos

func (s *Source) Connect(ctx context.Context) error {
	return nil
}

func (s *Source) Write(ctx context.Context, message *service.Message) error {
	s.messages <- message
	return nil
}

func (s *Source) Close(ctx context.Context) error {
	return nil
}
