package ethindex

import (
	"log/slog"
)

type ConfigureHandler struct{ i *Indexer }
type ConfigureClients struct{ i *Indexer }
type ConfigureCache struct{ i *Indexer }
type ConfigureOptions struct{ i *Indexer }

// New begins the configuration chain for an Indexer.
func New() ConfigureHandler {
	return ConfigureHandler{&Indexer{
		newHeadsBuffer:     128,
		maxBlockRange:      10_000,
		maxConcurrentCalls: 100,
		checkpointInterval: 64,
		logger:             slog.Default(),
		stopCh:             make(chan struct{}),
	}}
}

// WithHandler sets the event handler for the indexer.
func (c ConfigureHandler) WithHandler(h Handler) ConfigureClients {
	c.i.handler = h
	return ConfigureClients(c)
}

// WithClients sets the Ethereum RPC clients.
func (c ConfigureClients) WithClients(http Caller, ws Subscriber) ConfigureCache {
	c.i.http = http
	c.i.ws = ws
	return ConfigureCache(c)
}

// WithCache sets the caching layer.
func (cc ConfigureCache) WithCache(c Cache) ConfigureOptions {
	cc.i.cache = c
	return ConfigureOptions(cc)
}

// Build finalizes the configuration and returns the fully constructed Indexer.
func (c ConfigureOptions) Build() *Indexer {
	return c.i
}

// WithRetryFunc sets the retry policy for RPC calls and block processing.
// Returning true triggers a reconnect; returning false halts the indexer.
func (c ConfigureOptions) WithRetryFunc(f func(err error, attempt int) bool) ConfigureOptions {
	c.i.retryFunc = f
	return c
}

// WithNewHeadsBuffer sets the capacity of the live block subscription channel.
// Default is 128.
func (c ConfigureOptions) WithNewHeadsBuffer(n int) ConfigureOptions {
	c.i.newHeadsBuffer = n
	return c
}

// WithMaxBlockRange sets the maximum block span per backfill RPC call.
// Default is 10,000.
func (c ConfigureOptions) WithMaxBlockRange(r uint64) ConfigureOptions {
	c.i.maxBlockRange = r
	return c
}

// WithCheckpointInterval sets how often (in blocks) the indexer saves state.
// Default is 64 (~2 epochs).
func (c ConfigureOptions) WithCheckpointInterval(interval uint64) ConfigureOptions {
	c.i.checkpointInterval = interval
	return c
}

// WithLogger sets the structured logger.
// Default is slog.Default().
func (c ConfigureOptions) WithLogger(l *slog.Logger) ConfigureOptions {
	c.i.logger = l
	return c
}
