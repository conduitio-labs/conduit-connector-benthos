package connector

import "errors"

const (
	BenthosYaml = "benthos.yaml"
)

var Required = []string{BenthosYaml}

var (
	ErrEmptyConfig = errors.New("missing or empty config")
)

type Config struct {
	benthosYaml string
}

type SourceConfig struct {
	Config
	sourceConfigParam string
}

type DestinationConfig struct {
	Config
	destinationConfigParam string
}

func ParseSourceConfig(cfg map[string]string) (SourceConfig, error) {
	err := checkEmpty(cfg)
	if err != nil {
		return SourceConfig{}, err
	}
	return SourceConfig{
		Config: Config{benthosYaml: cfg[BenthosYaml]},
	}, nil
}

func ParseDestinationConfig(cfg map[string]string) (DestinationConfig, error) {
	err := checkEmpty(cfg)
	if err != nil {
		return DestinationConfig{}, err
	}
	return DestinationConfig{
		Config: Config{benthosYaml: cfg[BenthosYaml]},
	}, nil
}

func checkEmpty(cfg map[string]string) error {
	if cfg == nil || len(cfg) == 0 {
		return ErrEmptyConfig
	}
	return nil
}
