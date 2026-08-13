# hyperfleet-applier

The HyperFleet desire contract and applier.

## Description

This repository defines the HyperFleet **desire contract** - the declarative
intent for a fleet - and the **applier** that reconciles that desire against a
backend store. It is part of the HyperFleet initiative tracked by epic
[HYPERFLEET-1418](https://issues.redhat.com/browse/HYPERFLEET-1418).

The concrete desire types, store interfaces, and backends are delivered in
follow-up tickets; this repository currently provides the module foundation
(`pkg/desire`).

## Security posture

Credentials are stored scoped to the applier's own partition by convention.
Cryptographic enforcement of that scoping arrives with the production backends
(see HYPERFLEET-1423).

## Development

Run `make help` for the full list of targets. Common ones:

```sh
make build   # go build ./...
make test    # go test ./...
make lint    # golangci-lint run
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
