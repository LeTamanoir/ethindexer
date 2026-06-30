# ethindex

[![CI](https://github.com/letamanoir/ethindex/actions/workflows/ci.yml/badge.svg)](https://github.com/letamanoir/ethindex/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/letamanoir/ethindex.svg)](https://pkg.go.dev/github.com/letamanoir/ethindex)

`ethindex` is a lightweight Go library for indexing Ethereum logs.

It handles backfilling, live indexing, checkpointing, reorg recovery, and resumable restarts so handlers only need to implement application-specific indexing logic.

## Install

```bash
go get github.com/letamanoir/ethindex
```

## Usage

```go
ctx := context.Background()

client, err := ethclient.DialContext(ctx, "http://127.0.0.1:8545")
if err != nil {
	log.Fatal(err)
}

store, err := ethindex.NewFileStore("./indexer_data")
if err != nil {
	log.Fatal(err)
}

idx := ethindex.NewIndexer(client, myHandler, store, nil, ethindex.Config{})

if err := idx.Sync(ctx); err != nil {
	log.Fatal(err)
}

heads := make(chan *types.Header, 128)

sub, err := client.SubscribeNewHead(ctx, heads)
if err != nil {
	log.Fatal(err)
}
defer sub.Unsubscribe()

for {
	select {
	case <-ctx.Done():
		return

	case err := <-sub.Err():
		log.Fatal(err)

	case h := <-heads:
		if err := idx.Process(ctx, h); err != nil {
			log.Fatal(err)
		}

		// Read handler state here.
	}
}
```

Implement `Handler` with your indexing logic:

```go
type Handler interface {
	Process(context.Context, []types.Log) error
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
}
```

See [`examples/weth`](examples/weth) for a complete example.

## How it works

`Sync` restores the latest finalized checkpoint, backfills to the node's current finalized block, and saves a new finalized checkpoint.

`Process` ingests new heads after `Sync` returns. Each header is checked against the current head. If a gap is detected, the indexer fills it. If a parent hash mismatch is detected, the indexer restores the finalized checkpoint and replays the canonical chain.

```text
Start block               Finalized block          Dangling     Latest
     |                          |                     |           |
     S --------[...]----------- F ------------------- D --------- L
                                  <- FinalityDepth ->
```

The indexer keeps two checkpoints:

* **Finalized (`F`)**: durable restart point.
* **Dangling (`D`)**: pending checkpoint promoted once it is old enough.

This lets the indexer resume quickly while avoiding committing state that may still be affected by reorgs.

## Configuration

```go
type Config struct {
	MaxBlockRange  uint64
	FinalityDepth uint64
	MaxConcurrency int
}
```

| Field            |  Default | Description                                      |
| ---------------- | -------: | ------------------------------------------------ |
| `MaxBlockRange`  | `10,000` | Maximum block span per backfill request          |
| `FinalityDepth`  |     `64` | Blocks before a dangling checkpoint is finalized |
| `MaxConcurrency` |     `16` | Maximum concurrent header fetches                |

## Development

```bash
just check
go test ./...
```

## License

[MIT](LICENSE)
