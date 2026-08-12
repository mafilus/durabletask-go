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

This project requires go v1.19.x or greater. You can build a standalone executable by simply running `go build` at the project root.

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

All automated tests are under `./tests`. A separate test package hierarchy was chosen intentionally to prioritize [black box testing](https://en.wikipedia.org/wiki/Black-box_testing). This strategy also makes it easier to catch accidental breaking API changes.

Run tests with the following command.

```bash
go test ./tests/... -coverpkg ./api,./task,./client,./backend/...,./api/helpers
```
