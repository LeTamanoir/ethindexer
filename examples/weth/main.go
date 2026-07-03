package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log/slog"
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
	"github.com/holiman/uint256"
	"github.com/joho/godotenv"
	"github.com/letamanoir/ethindexer"
)

var (
	// cast sig-event "Transfer(address indexed,address indexed,uint256)"
	transferEventID = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	// cast sig-event "Approval(address indexed,address indexed,uint256)"
	approvalEventID = common.HexToHash("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")
)

type WETH struct {
	Balances   map[common.Address]uint256.Int
	Allowances map[common.Address]map[common.Address]uint256.Int
}

func NewWETH() *WETH {
	return &WETH{
		Balances:   make(map[common.Address]uint256.Int),
		Allowances: make(map[common.Address]map[common.Address]uint256.Int),
	}
}

func (e *WETH) Filter() ethindexer.Filter {
	return ethindexer.Filter{
		FromBlock: 4719568,
		Addresses: []common.Address{common.HexToAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")},
		Topics:    [][]common.Hash{{transferEventID, approvalEventID}},
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
			value := new(uint256.Int).SetBytes(log.Data)

			if from != (common.Address{}) {
				fromBalance := e.Balances[from]
				e.Balances[from] = *new(uint256.Int).Sub(&fromBalance, value)
			}
			if to != (common.Address{}) {
				toBalance := e.Balances[to]
				e.Balances[to] = *new(uint256.Int).Add(&toBalance, value)
			}
		case approvalEventID:
			if len(log.Topics) < 3 {
				return fmt.Errorf("invalid Approval event topics")
			}
			owner := common.BytesToAddress(log.Topics[1].Bytes())
			spender := common.BytesToAddress(log.Topics[2].Bytes())
			value := new(uint256.Int).SetBytes(log.Data)

			al, ok := e.Allowances[owner]
			if !ok {
				al = make(map[common.Address]uint256.Int)
				e.Allowances[owner] = al
			}
			al[spender] = *value
		}
	}
	return nil
}

func initClients(ctx context.Context) (*ethclient.Client, *ethclient.Client, error) {
	var options []rpc.ClientOption

	if v := os.Getenv("ETH_JWT_SECRET"); v != "" {
		var secret [32]byte
		if _, err := hex.Decode(secret[:], []byte(v)); err != nil {
			return nil, nil, fmt.Errorf("failed to decode secret: %w", err)
		}
		options = append(options, rpc.WithHTTPAuth(node.NewJWTAuth(secret)))
	}

	httpURL := os.Getenv("ETH_HTTP_URL")
	if httpURL == "" {
		return nil, nil, fmt.Errorf("missing ETH_HTTP_URL")
	}

	wsURL := os.Getenv("ETH_WS_URL")
	if wsURL == "" {
		return nil, nil, fmt.Errorf("missing ETH_WS_URL")
	}

	httpR, err := rpc.DialOptions(ctx, httpURL, options...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial http: %w", err)
	}

	wsR, err := rpc.DialOptions(ctx, wsURL, options...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial ws: %w", err)
	}

	return ethclient.NewClient(httpR), ethclient.NewClient(wsR), nil
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	_ = godotenv.Load()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	httpC, wsC, err := initClients(ctx)
	if err != nil {
		return err
	}

	store, err := ethindexer.NewFileStore(".weth_indexer")
	if err != nil {
		return fmt.Errorf("new store: %w", err)
	}

	handler := NewWETH()

	idx, err := ethindexer.OpenContext(ctx, ethindexer.Options{
		Client:  httpC,
		Handler: handler,
		Store:   store,
		LogFunc: slog.Info,
	})
	if err != nil {
		return fmt.Errorf("open indexer: %w", err)
	}

	heads := make(chan *types.Header, 128)
	sub := event.Resubscribe(2*time.Second, func(ctx context.Context) (event.Subscription, error) {
		return wsC.SubscribeNewHead(ctx, heads)
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

			slog.Info("Processed new head", "head", h.Number.Uint64(), "hash", h.Hash())

			// Do whatever you want with handler state
		}
	}
}

func main() {
	if err := run(); err != nil {
		slog.Error("Indexer error", "error", err)
		os.Exit(1)
	}
}
