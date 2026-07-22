# ethindexer

[![CI](https://github.com/LeTamanoir/ethindexer/actions/workflows/ci.yml/badge.svg)](https://github.com/LeTamanoir/ethindexer/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/LeTamanoir/ethindexer.svg)](https://pkg.go.dev/github.com/LeTamanoir/ethindexer)

`ethindexer` is a lightweight Go library for indexing Ethereum logs.

It handles backfilling, live indexing, checkpointing, reorg recovery, and resumable restarts so applications only need to provide their indexing callbacks.

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

### Indexing callbacks

Configure application-specific indexing logic directly on `Indexer`:

* **`FromBlock`** specifies the first block to index.
* **`Filter`** specifies which logs to index.
* **`ProcessFunc`** receives matching logs in block order.
* **`SnapshotFunc`** and **`RestoreFunc`** serialize and deserialize application state for checkpointing.
* **`InitFunc`** optionally initializes application state on a fresh start and receives the configured `ChainReader` plus a cached `LogsRangeFunc`.

Stateful methods can be passed as callbacks without implementing an interface:

```go
state := NewWETH()

idx := &ethindexer.Indexer{
    Client:    client,
    DataDir:   ".ethindexer",
    FromBlock: deploymentBlock,
    Filter: ethindexer.Filter{
        Addresses: []common.Address{contractAddress},
    },
    ProcessFunc:  state.Process,
    SnapshotFunc: state.Snapshot,
    RestoreFunc:  state.Restore,
}
if err := idx.Sync(ctx); err != nil {
    return err
}
```

`InitFunc`, when set, is called once by `Sync` when the indexer has no finalized checkpoint to restore. It receives the configured `ChainReader` plus a `LogsRangeFunc` that caches block-range queries in `DataDir`. Initialization runs before any logs are processed and before the first checkpoint is saved.

This is useful when you want to start indexing from a very late block (for example, after a contract upgrade) but still need to reconstruct some pre-upgrade state. Instead of setting `FromBlock` to the contract's original deployment and processing years of logs, set `FromBlock` to the upgrade block and use `InitFunc` to perform heavy one-time setup (RPC calls, database migrations, etc.). Once initialization succeeds, the indexer saves a checkpoint as usual, so the setup work is not repeated on restart.

Indexer tunables such as `FinalityDepth`, `MaxBlockRange`, `CheckpointInterval`, and `MaxConcurrency` are set directly on `Indexer`.

## Development

```bash
just check
go test ./...
```

## License

[MIT](LICENSE)
