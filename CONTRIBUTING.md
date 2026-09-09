# Contributing

## Cloning this repository

This repository contains submodules. Be sure to clone it with the option to include submodules. Otherwise, you will not be able to generate the protobuf code.

```bash
git clone --recurse-submodules https://github.com/mafilus/durabletask-go
```

If you already cloned the repository without `--recurse-submodules`, you can initialize and update the submodules with:

```bash
git submodule update --init --recursive
```

The protobuf definitions are pinned to the commit recorded by this repository.
To reset a local submodule checkout to that pinned revision, run:
```bash
git submodule sync --recursive
git submodule update --init --recursive
```

Do not use `git submodule update --remote`: protobuf updates are made only by
intentionally changing the pinned submodule commit.

## Building the project

Version v1.2.0 requires Go 1.26.6 or newer. The authoritative minimum is declared
in `go.mod`, which CI also uses to select Go. You can build a standalone
executable by running `go build` at the project root.

### Generating protobuf

Use the following command to regenerate the protobuf from the submodule. Use this whenever updating the submodule reference.

```bash
# Run from the repo root and specify the output directory
# This will place the generated files directly in api/protos/, matching the go_package and your repo structure.
protoc --go_out=. --go-grpc_out=. \
  -I submodules/durabletask-protobuf/protos \
  submodules/durabletask-protobuf/protos/backend_service.proto   \
  submodules/durabletask-protobuf/protos/history_events.proto \
  submodules/durabletask-protobuf/protos/runtime_state.proto  \
  submodules/durabletask-protobuf/protos/orchestration.proto \
  submodules/durabletask-protobuf/protos/orchestrator_actions.proto \
  submodules/durabletask-protobuf/protos/orchestrator_service.proto
```

For local development with protobuf changes, use a neighboring checkout of
`mafilus/durabletask-protobuf`:
```bash
# Regenerate protobuf files using your local proto definitions
protoc --go_out=. --go-grpc_out=. \
  -I ../durabletask-protobuf/protos \
  ../durabletask-protobuf/protos/backend_service.proto   \
  ../durabletask-protobuf/protos/history_events.proto \
  ../durabletask-protobuf/protos/runtime_state.proto  \
  ../durabletask-protobuf/protos/orchestration.proto \
  ../durabletask-protobuf/protos/orchestrator_actions.proto \
  ../durabletask-protobuf/protos/orchestrator_service.proto
```

This uses the local proto files instead of the pinned submodule, which is useful
when testing a deliberate protobuf update before pinning its commit here.

### Generating mocks for testing

Test mocks were generated using [mockery](https://github.com/vektra/mockery). Use the following command at the project root to regenerate the mocks.

```bash
mockery --dir ./backend --name="^Backend|^Executor|^TaskWorker" --output ./tests/mocks --with-expecter
```

## Running tests

Tests live both alongside their packages and under `./tests`, including the
SQLite/gRPC recovery tests in `./tests/transport`. Run the complete default suite
from the repository root:

```bash
go test -count=1 -timeout=3m ./...
go vet ./...
```

PostgreSQL tests that require a database are skipped unless enabled; see the
[backend test setup](backend/postgres/README.md). `PR Validation` provisions
PostgreSQL and runs the default suite with `POSTGRES_ENABLED=true`.

Build-tagged integration tests are separate from the default suite:

- `OpenTelemetry Collector integration` runs the collector integration test.
- `PostgreSQL chaos integration` is manual (`workflow_dispatch`). Select the
  branch or tag to validate in GitHub Actions. It provisions an isolated Docker
  PostgreSQL instance, runs the five restart-boundary scenarios with
  `-tags 'integration postgreschaos'`, collects diagnostics, and removes the
  instance. Never point these restart tests at a production database.
- `PostgreSQL durability lab` is a separate manual workflow with selectable
  durability and stress suites.

A green default CI run does not imply that either manual workflow ran. Record
the exact commit and run link when reporting chaos or durability results.

## Publishing a release

Validate the intended runtime commit and prepare `docs/releases/vX.Y.Z.md` with
a matching `# vX.Y.Z:` title, without an `unreleased` marker. Use absolute links
in this note so they also work on the GitHub release page. Release preparation
must not introduce unvalidated runtime changes.

Push an annotated, stable-version tag only after verifying its exact commit.
The `Publish release` workflow checks out that tag and publishes its committed
notes through GitHub's built-in token. It requires no personal access token.
It verifies that the tag exists and does not overwrite an existing release.
When publishing multiple versions, publish and verify them in ascending order:
each new publication becomes the latest release.

Verify the workflow result, the published release, and the remote peeled tag
OID. Tags and release publication do not substitute for runtime test results.
