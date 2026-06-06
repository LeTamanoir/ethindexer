package ethindex

import (
	"log/slog"

	"github.com/ethereum/go-ethereum/ethclient"
)

func defaultIndexer() *Indexer {
	return &Indexer{
		newHeadsBuffer:     128,
		maxBlockRange:      10_000,
		maxConcurrentCalls: 100,
		checkpointInterval: 64,
		logger:             slog.Default(),
		stopCh:             make(chan struct{}),
	}
}

type configHandler struct{ i *Indexer }
type configClients struct{ i *Indexer }
type configCache struct{ i *Indexer }
type configOptional struct{ i *Indexer }

func Configure() configHandler {
	return configHandler{defaultIndexer()}
}

func (c configHandler) WithHandler(h Handler) configClients {
	c.i.handler = h
	return configClients(c)
}

func (c configClients) WithClients(http *ethclient.Client, ws *ethclient.Client) configCache {
	c.i.http = http
	c.i.ws = ws
	return configCache(c)
}

func (cc configCache) WithCache(c Cache) configOptional {
	cc.i.cache = c
	return configOptional(cc)
}

func (c configOptional) Build() *Indexer {
	return c.i
}

// WithRetryFunc determines whether an error encountered during block processing
// or RPC calls should trigger a reconnect/retry. It receives the underlying
// error and the current consecutive attempt count. If it returns true, the
// indexer enters StateReconnect. If false, the indexer halts and returns the error.
func (c configOptional) WithRetryFunc(f func(err error, attempt int) bool) configOptional {
	c.i.retryFunc = f
	return c
}

// WithNewHeadsBuffer defines the channel buffer size for the live block header
// subscription. A larger buffer prevents the RPC subscription from dropping
// due to slow consumers if the indexer takes a few seconds to process a block.
//
// Default is 128.
func (c configOptional) WithNewHeadsBuffer(n int) configOptional {
	c.i.newHeadsBuffer = n
	return c
}

// WithMaxBlockRange sets the maximum number of blocks to request in a single
// eth_getLogs call during the backfill phase. Most RPC providers impose
// strict limits on range queries.
//
// Default is 10,000.
func (c configOptional) WithMaxBlockRange(r uint64) configOptional {
	c.i.maxBlockRange = r
	return c
}

// WithCheckpointInterval defines how frequently (in blocks) the indexer writes
// its state to the database.
//
// Slots and blocks are produced at the same 12s interval.
// 2 epochs = 64 slots, which is the Ethereum finality window (~12m48s).
// Default is 64.
func (c configOptional) WithCheckpointInterval(interval uint64) configOptional {
	c.i.checkpointInterval = interval
	return c
}

// WithLogger sets a custom structured logger for the indexer.
//
// Default is slog.Default().
func (c configOptional) WithLogger(l *slog.Logger) configOptional {
	c.i.logger = l
	return c
}
