package connector_test

import (
	"context"
	"strings"
	"testing"

	connector "github.com/conduitio-labs/conduit-connector-benthos"
)

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := connector.Source{}
	err := con.Configure(context.Background(), make(map[string]string))
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}

	if strings.HasPrefix(err.Error(), "config is invalid:") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

func TestConfigureSource_FailsWhenConfigInvalid(t *testing.T) {
	con := connector.Source{}
	err := con.Configure(context.Background(), map[string]string{"foobar": "foobar"})
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}

	if strings.HasPrefix(err.Error(), "config is missing:") {
		t.Errorf("expected error to be about invalid config, got %v", err)
	}
}

func TestTeardownSource_NoOpen(t *testing.T) {
	con := connector.NewSource()
	err := con.Teardown(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}
}
