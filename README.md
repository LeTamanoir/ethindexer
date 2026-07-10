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

### Handler

Your handler must implement the `Handler` interface. The key methods are:

* **`Filter() Filter`** - specifies which logs to index, including the starting block (`FromBlock`).
* **`Process(ctx, logs) error`** - called with matching logs in block order.
* **`Snapshot() ([]byte, error)`** / **`Restore(ctx, []byte) error`** - serialize and deserialize your handler state for checkpointing.

### `Initer` (optional)

If your handler needs to perform setup before syncing begins, you can also implement the optional `Initer` interface:

```go
type Initer interface {
    Init(ctx context.Context, client ethindexer.ChainReader) error
}
```

`Init` is called once by `Sync` when the indexer has no finalized checkpoint to restore. It runs before any logs are processed and before the first checkpoint is saved.

This is useful when you want to start indexing from a very late block (for example, after a contract upgrade) but still need to reconstruct some pre-upgrade state. Instead of setting `FromBlock` to the contract's original deployment and processing years of logs, set `FromBlock` to the upgrade block and use `Init` to perform heavy one-time setup (RPC calls, database migrations, etc.). Once `Init` succeeds, the indexer saves a checkpoint as usual, so the setup work is not repeated on restart.

## Development

```bash
just check
go test ./...
```

## License

[MIT](LICENSE)
