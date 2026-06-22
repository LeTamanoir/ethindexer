package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/joho/godotenv"
	"github.com/letamanoir/ethindex"
	"github.com/letamanoir/ethindex/examples/contracts"
)

var (
	erc20ABI, _ = contracts.ERC20MetaData.ParseABI()

	transferEventID = erc20ABI.Events["Transfer"].ID
	approvalEventID = erc20ABI.Events["Approval"].ID

	wethFilter = ethindex.Filter{
		FromBlock: 4719568,
		Addresses: []common.Address{common.HexToAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")},
		Topics:    [][]common.Hash{{transferEventID, approvalEventID}},
	}
)

type WETH struct {
	Balances   map[common.Address]big.Int
	Allowances map[common.Address]map[common.Address]big.Int
}

func NewWETH() *WETH {
	return &WETH{
		Balances:   make(map[common.Address]big.Int),
		Allowances: make(map[common.Address]map[common.Address]big.Int),
	}
}

func (e *WETH) Restore(_ context.Context, data []byte) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(e)
}

func (e *WETH) Snapshot(_ context.Context) ([]byte, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(e); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (e *WETH) Process(_ context.Context, logs []types.Log) error {
	for _, log := range logs {
		switch log.Topics[0] {
		case transferEventID:
			if len(log.Topics) < 3 {
				return fmt.Errorf("invalid Transfer event topics")
			}
			from := common.BytesToAddress(log.Topics[1].Bytes())
			to := common.BytesToAddress(log.Topics[2].Bytes())
			value := new(big.Int).SetBytes(log.Data)

			if from != (common.Address{}) {
				fromBalance := e.Balances[from]
				e.Balances[from] = *new(big.Int).Sub(&fromBalance, value)
			}
			if to != (common.Address{}) {
				toBalance := e.Balances[to]
				e.Balances[to] = *new(big.Int).Add(&toBalance, value)
			}
		case approvalEventID:
			if len(log.Topics) < 3 {
				return fmt.Errorf("invalid Approval event topics")
			}
			owner := common.BytesToAddress(log.Topics[1].Bytes())
			spender := common.BytesToAddress(log.Topics[2].Bytes())
			value := new(big.Int).SetBytes(log.Data)

			al, ok := e.Allowances[owner]
			if !ok {
				al = make(map[common.Address]big.Int)
			}
			al[spender] = *value
		}
	}
	return nil
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	_ = godotenv.Load()

	var options []rpc.ClientOption

	if v := os.Getenv("ETH_JWT_SECRET"); v != "" {
		var secret [32]byte
		if _, err := hex.Decode(secret[:], []byte(v)); err != nil {
			return fmt.Errorf("failed to decode secret: %w", err)
		}
		options = append(options, rpc.WithHTTPAuth(node.NewJWTAuth(secret)))
	}

	httpURL := os.Getenv("ETH_HTTP_URL")
	if httpURL == "" {
		return fmt.Errorf("missing ETH_HTTP_URL")
	}

	wsURL := os.Getenv("ETH_WS_URL")
	if wsURL == "" {
		return fmt.Errorf("missing ETH_WS_URL")
	}

	// HTTP client drives backfilling (eth_getLogs + finalized block headers).
	httpRPC, err := rpc.DialOptions(ctx, httpURL, options...)
	if err != nil {
		return fmt.Errorf("dial http: %w", err)
	}
	httpClient := ethclient.NewClient(httpRPC)

	// WebSocket client drives live following via new-head subscriptions.
	wsRPC, err := rpc.DialOptions(ctx, wsURL, options...)
	if err != nil {
		return fmt.Errorf("dial ws: %w", err)
	}
	wsClient := ethclient.NewClient(wsRPC)

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	store, err := ethindex.NewFileStore("./indexer_data")
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}

	progress := make(chan ethindex.Progress)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-progress:
				slog.Info("backfill progress",
					"block", fmt.Sprintf("%d/%d", p.CurrentBlock, p.EndBlock),
					"percent", fmt.Sprintf("%%%.2f", p.Percent()))
			}
		}
	}()

	idx, err := ethindex.NewIndexer(ctx, httpClient, NewWETH(), wethFilter, store, &ethindex.Config{ProgressCh: progress})
	if err != nil {
		return fmt.Errorf("new indexer: %w", err)
	}
	close(progress)

	heads := make(chan *types.Header, 128)
	sub := event.Resubscribe(2*time.Second, func(ctx context.Context) (event.Subscription, error) {
		return wsClient.SubscribeNewHead(ctx, heads)
	})
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return nil
		case h := <-heads:
			if err := idx.Process(ctx, h); err != nil {
				return fmt.Errorf("process head %d: %w", h.Number, err)
			}
			slog.Info("processed head", "number", h.Number.Uint64(), "hash", h.Hash())
		case err := <-sub.Err():
			return fmt.Errorf("head subscription: %w", err)
		}
	}
}

func main() {
	if err := run(); err != nil {
		slog.Error("Indexer error", "error", err)
		os.Exit(1)
	}
}
