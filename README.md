### Conduit Connector Template
This is a template project for building [Conduit](https://conduit.io) connectors in Go.

### How to use
Clone this repo and implement the various relevant methods.

### Specification
The `spec.go` file provides a programmatic representation of the configuration options. This is used by the Conduit
server to validate configuration and dynamically display configuration options to end users.

### How to build?
Run `make build` to build the connector.

### Testing
Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to quickly start a Kafka instance. 

### Configuration
There are 2 types of "configs", general configs (that apply to both Sources and Destinations) and Source/Destination 
specific configs.

General configs should be added to `config/config.go` whereas any source or destination specific configs should be added
to `source/config.go` and `destination/config.go` respectively
