# ethindexer

[![CI](https://github.com/LeTamanoir/ethindexer/actions/workflows/ci.yml/badge.svg)](https://github.com/LeTamanoir/ethindexer/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/LeTamanoir/ethindexer.svg)](https://pkg.go.dev/github.com/LeTamanoir/ethindexer)

`ethindexer` is a lightweight Go library for indexing Ethereum logs.

It handles backfilling, live indexing, checkpointing, reorg recovery, and resumable restarts so handlers only need to implement application-specific indexing logic.

## Install

```bash
go get github.com/LeTamanoir/ethindexer
```

## Usage

See [`examples/weth`](examples/weth) for a complete example.

## How it works

`Sync` restores the latest finalized checkpoint, backfills to the node's current finalized block, and saves a new finalized checkpoint.

`Process` ingests new heads after `Sync` returns. Each header is checked against the current head. If a gap is detected, the indexer fills it. If a parent hash mismatch is detected, the indexer restores the finalized checkpoint and replays the canonical chain.

```text
Start block               Finalized block           Staged      Latest
     |                          |                     |           |
     S --------[...]----------- F ------------------- S --------- L
                                  <- FinalityDepth ->
```

The indexer keeps two checkpoints:

* **Finalized (`F`)**: durable restart point.
* **Staged (`S`)**: pending checkpoint promoted once it is old enough.

This lets the indexer resume quickly while avoiding committing state that may still be affected by reorgs.

## Development

```bash
just check
go test ./...
```

## License

[MIT](LICENSE)
