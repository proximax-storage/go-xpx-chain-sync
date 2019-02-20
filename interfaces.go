package catapult_sync

import (
	"context"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/proximax-storage/proximax-nem2-sdk-go/sdk"
)

// Change those values if needed depending on the catapult version and used consensus
// TODO Validate for ProximaX
const (
	TransactionDeadline          = time.Minute
	AggregateTransactionDeadline = time.Minute * 5
)

var (
	ErrCatapultTimeout = errors.New("catapult is not responding for too long")
)

type TransactionSyncer interface {
	Sync(time.Time, sdk.Hash) <-chan Result
	Announce(ctx context.Context, tx sdk.Transaction, opts ...AnnounceOption) <-chan Result
	AnnounceFull(ctx context.Context, tx sdk.Transaction) (sdk.Hash, error)
	AnnounceUnSync(ctx context.Context, tx sdk.Transaction) (*sdk.SignedTransaction, error)
}

// Result interface is a result of transaction manipulation.
// If Err equals to nil then any kind of manipulation on transaction is successful.
type Result interface {
	Hash() sdk.Hash
	Err() error
}

// Option types
type SyncerOption func(*syncerConfig)
type AnnounceOption func(*announceConfig)

// Syncer configuration
type syncerConfig struct {
	wsClient          *sdk.ClientWebsocket
	httpClient        *http.Client
	connectionTimeout time.Duration
	gcTimeout         time.Duration
}

func WithWsClient(client *sdk.ClientWebsocket) SyncerOption {
	return func(config *syncerConfig) {
		config.wsClient = client
	}
}

// WithHttpClient option configures Catapult SDK client to work with passed one
func WithHttpClient(client *http.Client) SyncerOption {
	return func(config *syncerConfig) {
		config.httpClient = client
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

type announceConfig struct {
	lockDuration int64
	lockDeadline time.Duration
	lockAmount   int64
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

func LockAmount(amount int64) AnnounceOption {
	return func(config *announceConfig) {
		config.lockAmount = amount
	}
}
