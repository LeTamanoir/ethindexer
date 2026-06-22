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

idx := ethindex.NewIndexer(client, myHandler, store, nil)
if err := idx.Init(ctx); err != nil {
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
	Filter() Filter
	Process(context.Context, []types.Log) error
	Snapshot() ([]byte, error)
	Restore([]byte) error
}
```

See [`examples/weth`](examples/weth) for a full example.

## Config

| Field           | Default | Description                                  |
| --------------- | ------- | -------------------------------------------- |
| `MaxBlockRange` | 10,000  | Max blocks per `eth_getLogs` request         |
| `FinalityDepth` | 64      | Blocks before a dangling checkpoint is finalized |

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

`Init` restores the finalized checkpoint, backfills to the node's current
finalized block (caching log batches on disk), then returns. `Process` checks
each header's parent hash; on mismatch it rolls back to the last finalized
checkpoint and re-indexes the divergent range, so reorgs are handled
transparently.

## Observing progress

`Init` blocks during backfill, which can take a long time on a fresh run.
Call `Progress` from another goroutine to get a best-effort snapshot:

```go
idx := ethindex.NewIndexer(client, myHandler, store, nil)

go func() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p := idx.Progress()
			if p.ToBlock == 0 {
				continue
			}
			log.Printf("backfill %d/%d blocks", p.CurrentBlock, p.ToBlock)
		}
	}
}()

if err := idx.Init(ctx); err != nil {
	log.Fatal(err)
}
```

`Progress` is safe to call concurrently with `Init`.

## Development

```bash
just check   # fmt + vet + test
go test ./...
```

## License

[MIT](LICENSE)
