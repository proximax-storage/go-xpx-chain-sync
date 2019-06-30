package catapult_sync

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk"
	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk/websocket"
)

// Change those values if needed depending on the catapult version and used consensus
var (
	TransactionResultsTimeout   = time.Minute * 2
	TransactionCosigningTimeout = time.Second * 15
)

var (
	ErrCatapultTimeout     = errors.New("catapult is not responding for too long")
	ErrCoSignTimeout       = errors.New("no aggregate transaction is requested to cosign")
	ErrTxnDeadlineExceeded = errors.New("transaction deadline exceeded")
)

type TransactionSyncer interface {
	io.Closer

	transactionAnnouncer
	transactionCache

	Context() context.Context
}

type transactionAnnouncer interface {
	Sync(time.Time, *sdk.Hash) <-chan Result

	Announce(ctx context.Context, tx sdk.Transaction) (*sdk.SignedTransaction, error)

	AnnounceSync(ctx context.Context, tx sdk.Transaction, opts ...AnnounceOption) <-chan Result

	CoSign(ctx context.Context, hash *sdk.Hash, force bool) error
}

type transactionCache interface {
	Unconfirmed() []*sdk.Hash

	UnCosignedTransaction(hash *sdk.Hash) *sdk.AggregateTransaction

	UnCosignedTransactions() []*sdk.AggregateTransaction
}

// Result interface is a result of transaction manipulation.
// If Err equals to nil then any kind of manipulation on transaction is successful.
// TODO Change name to appropriate one
type Result interface {
	Hash() *sdk.Hash
	Err() error
}

// Option types
type SyncerOption func(*syncerConfig)
type AnnounceOption func(*announceConfig)

// Syncer configuration
type syncerConfig struct {
	wsClient          websocket.CatapultClient
	client            *sdk.Client
	getConfigFromRest bool
	connectionTimeout time.Duration
	gcTimeout         time.Duration
}

func WithWsClient(client websocket.CatapultClient) SyncerOption {
	return func(config *syncerConfig) {
		config.wsClient = client
	}
}

// WithHttpClient option configures Catapult SDK client to work with passed one
func WithClient(client *sdk.Client) SyncerOption {
	return func(config *syncerConfig) {
		config.client = client
	}
}

// WSTimeout option specifies websocket connection timeout on start-up
func WSTimeout(timeout time.Duration) SyncerOption {
	return func(config *syncerConfig) {
		config.connectionTimeout = timeout
	}
}

// WSTimeout option specifies
func GCTimeout(timeout time.Duration) SyncerOption {
	return func(config *syncerConfig) {
		config.gcTimeout = timeout
	}
}

// Transaction Announcing configuration
type announceConfig struct {
	lockDuration int64
	lockDeadline time.Duration
	lockAmount   uint64
}

func LockDuration(duration int64) AnnounceOption {
	return func(config *announceConfig) {
		config.lockDuration = duration
	}
}

func LockDeadline(deadline time.Duration) AnnounceOption {
	return func(config *announceConfig) {
		config.lockDeadline = deadline
	}
}

func LockAmount(amount uint64) AnnounceOption {
	return func(config *announceConfig) {
		config.lockAmount = amount
	}
}
