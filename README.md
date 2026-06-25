# ethindex

[![CI](https://github.com/letamanoir/ethindex/actions/workflows/ci.yml/badge.svg)](https://github.com/letamanoir/ethindex/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/letamanoir/ethindex.svg)](https://pkg.go.dev/github.com/letamanoir/ethindex)

A lightweight Ethereum log indexer library in Go. Handles backfilling, live
following, checkpointing, reorg detection and resumable restarts so you can
focus on your indexing logic.

## Install

```bash
go get github.com/letamanoir/ethindex
```

Requires Go 1.26+ and an RPC node supporting `eth_getLogs` and the
`finalized` block tag.

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
	case h := <-heads:
		if err := idx.Process(ctx, h); err != nil {
			log.Fatal(err)
		}
	}
}
```

Implement `Handler` with your indexing logic. The handler owns its `Filter`,
which tells the indexer which logs to fetch:

```go
type Handler interface {
	Filter() Filter
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
	Process(context.Context, []types.Log) error
}
```

```go
type myHandler struct{}

func (h *myHandler) Filter() ethindex.Filter {
	return ethindex.Filter{
		FromBlock: 18_000_000,
		Addresses: []common.Address{contractAddr},
		Topics:    [][]common.Hash{{eventTopic}},
	}
}
```

See [`examples/weth`](examples/weth) for a full example.

## Config

`NewIndexer(client, handler, store, logger, cfg)` takes the dependencies as
positional arguments and tunables via `Config`:

| Field             | Default | Description                                  |
| ----------------- | ------- | -------------------------------------------- |
| `MaxBlockRange`   | 10,000  | Max blocks per `eth_getLogs` request         |
| `FinalityDepth`   | 64      | Blocks before a dangling checkpoint is finalized |
| `MaxConcurrency`  | 16      | Max concurrent RPC calls when filling header gaps |

`Logger` is required; pass `nil` to disable logging. `Filter.FromBlock` (set on
the handler) is the start block on a fresh run; ignored once a checkpoint
exists.

## How it works

Two checkpoints are kept:

- **Finalized (`*`)** — durable; restart always resumes here.
- **Dangling (`o`)** — taken every `FinalityDepth` blocks, then promoted to
  finalized once it ages past `FinalityDepth`.

```text
S ------------------------ F --------- o --------- H
                            *
```

`NewIndexer` constructs the indexer. `Sync` restores the finalized checkpoint,
backfills to the node's current finalized block (caching log batches on disk),
then returns. `Process` checks each header's parent hash; on mismatch it rolls
back to the last finalized checkpoint and re-indexes the divergent range, so
reorgs are handled transparently.

## Logging

The indexer logs lifecycle events (start, restore, backfill, reorgs, checkpoint
promotions) and per-chunk/per-head diagnostics through the `Logger` passed to
`NewIndexer`. Pass `nil` to disable logging, or pass an `slog.Logger` to route
output; lower the level to `slog.LevelDebug` to see per-block and per-chunk
detail:

```go
logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelDebug,
}))

idx := ethindex.NewIndexer(client, myHandler, store, logger, ethindex.Config{})
if err := idx.Sync(ctx); err != nil {
	log.Fatal(err)
}
```

## Development

```bash
just check   # fmt + vet + test
go test ./...
```

## License

[MIT](LICENSE)
