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

filter := ethindex.Filter{
	FromBlock: 18_000_000,
	Addresses: []common.Address{contractAddr},
	Topics:    [][]common.Hash{{eventTopic}},
}

idx, err := ethindex.NewIndexer(ctx, client, myHandler, filter, store, nil)
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
	Process(context.Context, []types.Log) error
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
}
```

See [`examples/weth`](examples/weth) for a full example.

## Config

| Field           | Default | Description                                  |
| --------------- | ------- | -------------------------------------------- |
| `MaxBlockRange` | 10,000  | Max blocks per `eth_getLogs` request         |
| `FinalityDepth` | 64      | Blocks before a dangling checkpoint is finalized |
| `ProgressCh`    | `nil`   | Channel to receive backfill progress updates |

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

## Observing progress

`NewIndexer` blocks during backfill, which can take a long time on a fresh run.
Pass a `ProgressCh` in `Config` to receive a best-effort snapshot on each chunk:

```go
progress := make(chan ethindex.Progress)
go func() {
	for p := range progress {
		log.Printf("backfill %d/%d blocks (%.2f%%)", p.CurrentBlock, p.EndBlock, p.Percent())
	}
}()

idx, err := ethindex.NewIndexer(ctx, client, myHandler, filter, store, &ethindex.Config{
	ProgressCh: progress,
})
if err != nil {
	log.Fatal(err)
}
close(progress)
```

`Progress` is safe to read concurrently with `NewIndexer`.

## Development

```bash
just check   # fmt + vet + test
go test ./...
```

## License

[MIT](LICENSE)
