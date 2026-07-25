# ethindexer

[![CI](https://github.com/LeTamanoir/ethindexer/actions/workflows/ci.yml/badge.svg)](https://github.com/LeTamanoir/ethindexer/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/LeTamanoir/ethindexer.svg)](https://pkg.go.dev/github.com/LeTamanoir/ethindexer)

`ethindexer` is a lightweight Go library for indexing Ethereum logs.

It handles backfilling, live indexing, checkpointing, reorg recovery, and
resumable restarts. Applications only need to provide state with a `Process`
method.

## Install

```bash
go get github.com/LeTamanoir/ethindexer
```

## Usage

See [`examples/weth`](examples/weth) for a complete example.

## How it works

The first call to `Process` restores the latest finalized checkpoint when one
exists, then backfills through the supplied target. Pass a nil target to use
the node's latest head.

Each subsequent target is checked against the indexed head. Gaps use block-range
queries through the finalized head and reorg-safe block-hash queries afterward.
On a parent hash mismatch, the indexer restores the finalized checkpoint and
replays the canonical chain.

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

### Indexing state

State must provide a `Process` method:

```go
func (s *State) Process(ctx context.Context, logs []types.Log) error
```

```go
state := &WETH{
    Balances:   make(map[common.Address]uint256.Int),
    Allowances: make(map[common.Address]map[common.Address]uint256.Int),
}

idx := &ethindexer.Indexer[*WETH]{
    Client:    client,
    DataDir:   ".ethindexer",
    FromBlock: deploymentBlock,
    Filter: ethindexer.Filter{
        Addresses: []common.Address{contractAddress},
    },
    State: state,
}
if err := idx.Process(ctx, nil); err != nil {
    return err
}
```

`State` is automatically encoded into checkpoints with `encoding/gob`. It must
be a pointer so checkpoints can restore it in place. Its persisted fields must
be gob-compatible; state with custom serialization requirements can implement
`gob.GobEncoder` and `gob.GobDecoder`.

Applications that need custom initialization can use `HasCheckpoint` before
calling `Process`:

```go
hasCheckpoint, err := idx.HasCheckpoint()
if err != nil {
    return err
}
if !hasCheckpoint {
    if err := state.Init(ctx); err != nil {
        return err
    }
}
```

`CachedFilterLogs` is available for explicit historical block-range queries and
caches results in `DataDir`. `ClearCheckpoint` removes finalized and staged
checkpoints while preserving those cached ranges.

`FromBlock` and `Filter` define the indexed log stream. Tunables such as
`FinalityDepth`, `MaxBlockRange`, `CheckpointInterval`, and `MaxConcurrency`
are configured directly on `Indexer`.

## Development

```bash
just check
go test ./...
```

## License

[MIT](LICENSE)
