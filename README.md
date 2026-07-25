# ethindexer

[![CI](https://github.com/LeTamanoir/ethindexer/actions/workflows/ci.yml/badge.svg)](https://github.com/LeTamanoir/ethindexer/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/LeTamanoir/ethindexer.svg)](https://pkg.go.dev/github.com/LeTamanoir/ethindexer)

> [!WARNING]
> This package is experimental and will likely have many breaking changes
> before v1.

`ethindexer` is a lightweight Go library for indexing Ethereum logs into
checkpointed application state.

## Install

```bash
go get github.com/LeTamanoir/ethindexer
```

## How it works

The first `Process` call restores any finalized checkpoint and backfills through
the target. A nil target selects the latest head. Gaps use block-range queries
through the finalized head and reorg-safe block-hash queries afterward. On a
parent hash mismatch, the indexer restores the finalized checkpoint and replays
the canonical chain.

## Usage

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

State is persisted with `encoding/gob`, so its data must be gob-compatible.

See [`examples/weth`](examples/weth) for a complete example.

## License

[MIT](LICENSE)
