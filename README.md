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

idx, err := ethindex.NewIndexer(ctx, ethindex.Config{
	Client:  client,
	Handler: myHandler,
	Filter: ethindex.Filter{
		FromBlock: 18_000_000,
		Addresses: []common.Address{contractAddr},
		Topics:    [][]common.Hash{{eventTopic}},
	},
	Store: store,
})
if err != nil {
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

Implement `Handler` with your indexing logic:

```go
type Handler interface {
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
	Process(context.Context, []types.Log) error
}
```

See [`examples/weth`](examples/weth) for a full example.

## Config

| Field             | Required | Default        | Description                                  |
| ----------------- | -------- | -------------- | -------------------------------------------- |
| `Client`          | yes      |                | Ethereum RPC client                          |
| `Handler`         | yes      |                | Indexing logic                               |
| `Filter`          | yes      |                | Logs to fetch                                |
| `Store`           | yes      |                | Checkpoint/state persistence                 |
| `Logger`          | no       | `slog.Default` | Operational logger                           |
| `MaxBlockRange`   | no       | 10,000         | Max blocks per `eth_getLogs` request         |
| `FinalityDepth`   | no       | 64             | Blocks before a dangling checkpoint is finalized |
| `MaxConcurrency`  | no       | 16             | Max concurrent RPC calls when filling header gaps |

`Filter.FromBlock` is the start block on a fresh run; ignored once a
checkpoint exists.

## How it works

Two checkpoints are kept:

- **Finalized (`*`)** — durable; restart always resumes here.
- **Dangling (`o`)** — taken every `FinalityDepth` blocks, then promoted to
  finalized once it ages past `FinalityDepth`.

```text
S ------------------------ F --------- o --------- H
                            *
```

`NewIndexer` restores the finalized checkpoint, backfills to the node's current
finalized block (caching log batches on disk), then returns. `Process` checks
each header's parent hash; on mismatch it rolls back to the last finalized
checkpoint and re-indexes the divergent range, so reorgs are handled
transparently.

## Logging

The indexer logs lifecycle events (start, restore, backfill, reorgs, checkpoint
promotions) and per-chunk/per-head diagnostics through the `slog.Logger` in
`Config.Logger`. The default is `slog.Default`; pass a configured logger to
route output elsewhere, or lower the level to `slog.LevelDebug` to see
per-block and per-chunk detail:

```go
logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelDebug,
}))

idx, err := ethindex.NewIndexer(ctx, ethindex.Config{
	// ...
	Logger: logger,
})
```

## Development

```bash
just check   # fmt + vet + test
go test ./...
```

## License

[MIT](LICENSE)
