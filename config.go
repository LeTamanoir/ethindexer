package ethindex

// Config holds the indexer's configuration.
type Config struct {
	// MaxBlockRange is the maximum block span per backfill request.
	// Defaults to 10,000.
	MaxBlockRange uint64 `json:"max_block_range"`

	// FinalityDepth is the block depth considered finalized.
	// Defaults to 64.
	FinalityDepth uint64 `json:"finality_depth"`

	// MaxConcurrency bounds concurrent header fetches.
	// Defaults to 16.
	MaxConcurrency int `json:"max_concurrency"`
}

func (c *Config) applyDefaults() {
	if c.FinalityDepth == 0 {
		c.FinalityDepth = 64
	}
	if c.MaxBlockRange == 0 {
		c.MaxBlockRange = 10_000
	}
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 16
	}
}
