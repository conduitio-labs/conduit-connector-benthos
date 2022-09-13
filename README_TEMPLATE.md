### Conduit Connector for <resource>
[Conduit](https://conduit.io) for <resource>.

#### Source
A source connector pulls data from an external resource and pushes it to downstream resources via Conduit.

#### Destination
A destination connector pushes data from upstream resources to an external resource via Conduit.

### How to build?
Run `make build` to build the connector.

### Testing
Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

### Configuration

| name | part of | description | required | default value |
|------|---------|-------------|----------|---------------|
|`servers`|destination, source|A list of bootstrap servers to which the plugin will connect.|true| |
|`topic`|destination, source|The topic to which records will be written to.|true| |
|`acks`|destination|The number of acknowledgments required before considering a record written to Kafka. Valid values: 0, 1, all|false|`all`|
|`deliveryTimeout`|destination|Message delivery timeout.|false|`10s`|
|`readFromBeginning`|destination|Whether or not to read a topic from beginning (i.e. existing messages or only new messages).|false|`false`|

### Known Issues & Limitations
* Some known issue
* Some limitation

### Planned work
- [ ] Some future work
- [ ] Some other piece of future work
