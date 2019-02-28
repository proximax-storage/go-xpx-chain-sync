package catapult_sync

import (
	"context"
	"math/big"
	"time"

	"github.com/pkg/errors"

	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk"
)

// TODO Consider adding cache strategies with persisting for cases with unexpected Syncer terminating
type transactionSyncer struct {
	ctx    context.Context
	cancel context.CancelFunc

	// Optional Syncer account
	Account *sdk.Account

	// Catapult SDK related
	Client   *sdk.Client
	WSClient *sdk.ClientWebsocket
	Network  sdk.NetworkType

	// WebSocket Subscriptions
	confirmed   *sdk.SubscribeTransaction
	unconfirmed *sdk.SubscribeTransaction
	status      *sdk.SubscribeStatus
	bonded      *sdk.SubscribeBonded
	cosigners   *sdk.SubscribeSigner

	// Unsigned cache
	unsignedCache map[sdk.Hash]*sdk.AggregateTransaction // contains all the aggregate transactions Syncer's account taking part in TODO Handle possible memory leak when transactions are not confirmed
	getUnsigned   chan *unsignedRequest

	// Unconfirmed transactions cache and request channels
	unconfirmedCache map[sdk.Hash]*transactionMeta
	newUnconfirmed   chan *transactionMeta
	getUnconfirmed   chan *unconfirmedRequest

	// GC ticker
	gc  *time.Ticker
	cfg *syncerConfig
}

// NewTransactionSyncer creates new instance of TransactionSyncer
func NewTransactionSyncer(ctx context.Context, config *sdk.Config, acc *sdk.Account, opts ...SyncerOption) (TransactionSyncer, error) {
	if acc == nil {
		return nil, errors.New("account can't be nil")
	}

	var err error
	ctx, cancel := context.WithCancel(ctx)

	cfg := &syncerConfig{
		connectionTimeout: defConnTimeout,
		gcTimeout:         defGCTimeout,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	syncer := &transactionSyncer{
		ctx:              ctx,
		cancel:           cancel,
		Network:          config.NetworkType,
		Account:          acc,
		unsignedCache:    make(map[sdk.Hash]*sdk.AggregateTransaction),
		getUnsigned:      make(chan *unsignedRequest),
		unconfirmedCache: make(map[sdk.Hash]*transactionMeta),
		newUnconfirmed:   make(chan *transactionMeta),
		gc:               time.NewTicker(cfg.gcTimeout),
		cfg:              cfg,
	}

	if cfg.wsClient == nil {
		syncer.WSClient, err = sdk.NewConnectWs(config.BaseURL.String(), cfg.connectionTimeout)
		if err != nil {
			return nil, errors.Wrapf(err, "error while connecting to %s Catapult REST via WebSocket", config.BaseURL.String())
		}
	} else {
		syncer.WSClient = cfg.wsClient
	}

	if err = syncer.subscribe(); err != nil {
		return nil, err
	}

	go syncer.dispatcherLoop()

	return syncer, nil
}

// subscribe initialize listening to websocket
func (syncer *transactionSyncer) subscribe() (err error) {
	syncer.status, err = syncer.WSClient.Subscribe.Status(syncer.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to status ")
		return
	}

	syncer.confirmed, err = syncer.WSClient.Subscribe.ConfirmedAdded(syncer.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to confirmed")
		return
	}

	syncer.bonded, err = syncer.WSClient.Subscribe.PartialAdded(syncer.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to partial")
		return
	}

	syncer.cosigners, err = syncer.WSClient.Subscribe.Cosignature(syncer.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to cosigners")
		return
	}

	syncer.unconfirmed, err = syncer.WSClient.Subscribe.UnconfirmedAdded(syncer.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to unconfirmed transactions ")
		return
	}

	return
}

// dispatcherLoop method does most of Syncer logic.
// It handles all the incoming data from subscriptions and syncs it with underlying cache.
// Also process requests on what transaction to sync and data return requests
func (syncer *transactionSyncer) dispatcherLoop() {
	pushResult := func(ch chan<- Result, res Result) {
		ch <- res
	}

	for {
		select {

		// New transactions to handle
		case meta := <-syncer.newUnconfirmed:
			syncer.unconfirmedCache[meta.hash] = meta

		// Listening to websocket
		case status := <-syncer.status.Ch: // TODO Parse statuses to return right result
			if meta, ok := syncer.unconfirmedCache[status.Hash]; ok {
				go pushResult(meta.resultCh, &ConfirmationResult{
					hash: meta.hash,
					err:  errors.New(status.Status), // TODO Introduce new error type with all possible Catapult errors
				})

				delete(syncer.unconfirmedCache, status.Hash)
			}
		case confirmed := <-syncer.confirmed.Ch:
			if meta, ok := syncer.unconfirmedCache[confirmed.GetAbstractTransaction().Hash]; ok {
				go pushResult(meta.resultCh, &ConfirmationResult{
					hash: meta.hash,
					tx:   confirmed,
				})

				delete(syncer.unconfirmedCache, confirmed.GetAbstractTransaction().Hash)
			}

			delete(syncer.unsignedCache, confirmed.GetAbstractTransaction().Hash)
		case bonded := <-syncer.bonded.Ch:
			if meta, ok := syncer.unconfirmedCache[bonded.GetAbstractTransaction().Hash]; ok {
				go pushResult(meta.resultCh, &AggregatedAddedResult{
					tx: bonded,
				})
			} else {
				// Unhandled transaction received, saving to cache...
				syncer.unsignedCache[bonded.GetAbstractTransaction().Hash] = bonded
			}
		case cosignature := <-syncer.cosigners.Ch:
			if meta, ok := syncer.unconfirmedCache[cosignature.ParentHash]; ok {
				go pushResult(meta.resultCh, &CoSignatureResult{
					txHash:    cosignature.ParentHash,
					signer:    cosignature.Signer,
					signature: cosignature.Signature,
				})
			}
		case unconfirmed := <-syncer.unconfirmed.Ch:
			if meta, ok := syncer.unconfirmedCache[unconfirmed.GetAbstractTransaction().Hash]; ok {
				meta.unconfirmed = true
				go pushResult(meta.resultCh, &UnconfirmedResult{
					tx: unconfirmed,
				})
			}

		// Value requests
		case req := <-syncer.getUnsigned:
			var out []*sdk.AggregateTransaction

			if req.hash != "" {
				if hash, ok := syncer.unsignedCache[req.hash]; ok {
					out = append(out, hash)
				} else {
					out = append(out, nil)
				}
			} else {
				for _, tx := range syncer.unsignedCache {
					out = append(out, tx)
				}
			}

			req.resp <- out
		case req := <-syncer.getUnconfirmed:
			var hashes []sdk.Hash

			for _, meta := range syncer.unconfirmedCache {
				if meta.unconfirmed {
					hashes = append(hashes, meta.hash)
				}
			}

			req.resp <- hashes

		// Util
		case <-syncer.gc.C:
			syncer.collectGarbage()
		case <-syncer.ctx.Done():
			syncer.collectGarbage()
			syncer.gc.Stop()

			syncer.unconfirmedCache = nil
			syncer.unsignedCache = nil

			return
		}
	}
}

// Announce simply signs transaction with Syncer account and sends it to Catapult via SDK
func (syncer *transactionSyncer) Announce(ctx context.Context, tx sdk.Transaction) (*sdk.SignedTransaction, error) {
	if tx == nil {
		return nil, errors.New("nil transaction passed")
	}

	signedTx, err := syncer.Account.Sign(tx)
	if err != nil {
		return nil, err
	}

	if tx.GetAbstractTransaction().Type == sdk.AggregateBonded {
		_, err = syncer.Client.Transaction.AnnounceAggregateBonded(ctx, signedTx)
		if err != nil {
			return signedTx, err
		}
	} else {
		_, err = syncer.Client.Transaction.Announce(ctx, signedTx)
		if err != nil {
			return signedTx, err
		}
	}

	return signedTx, nil
}

// AnnounceSync wraps Announce and Sync methods to synchronize and validate transaction announcing.
// Can return multiple results depending on what happening with transaction on catapult side.
func (syncer *transactionSyncer) AnnounceSync(ctx context.Context, tx sdk.Transaction, opts ...AnnounceOption) <-chan Result {
	var result *AnnounceResult
	var err error

	resultCh := make(chan Result, 3)
	defer func() {
		result.err = err
		resultCh <- result
	}()

	if tx == nil {
		err = errors.New("nil transaction passed")
		return resultCh
	}

	if tx.GetAbstractTransaction().Type == sdk.AggregateBonded {
		return syncer.announceAggregateSync(ctx, tx.(*sdk.AggregateTransaction), opts...)
	}

	signedTxn, err := syncer.Announce(ctx, tx)
	if err != nil {
		return resultCh
	}

	result.signedTxn = signedTxn

	syncer.handleTxn(tx.GetAbstractTransaction().Deadline.Time, signedTxn.Hash, resultCh)

	return resultCh
}

// Sync handles announced and signed transaction and returns multiple results through channel
// Pass only hash of announced transactions related to Syncer,
// otherwise transaction won't be handled and would be cleaned after specified deadline
// TODO Possible delete of this method? Cause of no ability to check if txn was announced
func (syncer *transactionSyncer) Sync(deadline time.Time, hash sdk.Hash) <-chan Result {
	resultCh := make(chan Result)
	syncer.handleTxn(deadline, hash, resultCh)
	return resultCh
}

// CoSign cosigns any transaction by given hash with Syncer's account.
// If force is false, validates if that transaction exists through some time.
func (syncer *transactionSyncer) CoSign(ctx context.Context, hash sdk.Hash, force bool) error {
	if force {
		return syncer.coSign(ctx, hash)
	}

	timeout := time.After(TransactionCosigningTimeout)
	ticker := time.Tick(defReadTimeout)

	for {
		select {
		case <-timeout:
			return ErrCoSignTimeout
		case <-ticker:
			tx := syncer.UnCosignedTransaction(hash)
			if tx != nil {
				return syncer.coSign(ctx, hash)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Unconfirmed returns hashes of all unconfirmed transactions from cache.
func (syncer *transactionSyncer) Unconfirmed() []sdk.Hash { // TODO Return more information than just a hash in separate struct
	out := make(chan []sdk.Hash, 1)
	syncer.getUnconfirmed <- &unconfirmedRequest{resp: out}

	return <-out
}

// UnCosignedTransaction returns aggregate bonded transaction in which Syncer's account signature is requested.
func (syncer *transactionSyncer) UnCosignedTransaction(hash sdk.Hash) *sdk.AggregateTransaction {
	out := make(chan []*sdk.AggregateTransaction, 1)
	syncer.getUnsigned <- &unsignedRequest{resp: out, hash: hash}

	return (<-out)[0]
}

// UnCosignedTransactions returns all aggregate bonded transactions in which Syncer's account is taking part
// and where Syncer's co signature is needed to confirm transaction
// NOTICE: Handles only those transactions which are requested when Syncer is active
func (syncer *transactionSyncer) UnCosignedTransactions() []*sdk.AggregateTransaction {
	out := make(chan []*sdk.AggregateTransaction, 1)
	syncer.getUnsigned <- &unsignedRequest{resp: out}

	return <-out
}

// Close gracefully terminates Syncer, returns error if occurs
func (syncer *transactionSyncer) Close() (err error) {
	syncer.cancel()

	err = syncer.confirmed.Unsubscribe()
	err = syncer.status.Unsubscribe()
	err = syncer.bonded.Unsubscribe()

	return
}

func (syncer *transactionSyncer) announceAggregateSync(ctx context.Context, tx *sdk.AggregateTransaction, opts ...AnnounceOption) <-chan Result {
	var result *AnnounceResult
	var err error

	resultCh := make(chan Result, 32) // 32 cause possible amount of CoSignatureResults is big
	defer func() {
		result.err = err
		resultCh <- result
	}()

	signedTx, err := syncer.Account.Sign(tx)
	if err != nil {
		return resultCh
	}

	// TODO Possible move to lockFundsSync
	cfg := &announceConfig{
		lockDuration: defLockDuration,
		lockDeadline: defLockDeadline,
		lockAmount:   defLockAmount,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	err = syncer.lockFundsSync(ctx, cfg.lockAmount, cfg.lockDuration, cfg.lockDeadline, signedTx)
	if err != nil {
		err = errors.Wrap(err, "can't lock funds")
		return resultCh
	}

	_, err = syncer.Client.Transaction.AnnounceAggregateBonded(ctx, signedTx)
	if err != nil {
		return resultCh
	}

	syncer.handleTxn(tx.Deadline.Time, signedTx.Hash, resultCh)

	return resultCh
}

func (syncer *transactionSyncer) lockFundsSync(ctx context.Context, amount, duration int64, deadline time.Duration, signedTx *sdk.SignedTransaction) error {
	if amount < 10 {
		return errors.New("lock amount have to be bigger than 10")
	}

	lockTx, err := sdk.NewLockFundsTransaction(
		sdk.NewDeadline(deadline),
		sdk.XpxRelative(amount),
		big.NewInt(duration),
		signedTx,
		syncer.Network,
	)
	if err != nil {
		return err
	}

	signedLockTx, err := syncer.Announce(ctx, lockTx)
	if err != nil {
		return err
	}

	return (<-syncer.Sync(lockTx.Deadline.Time, signedLockTx.Hash)).Err()
}

func (syncer *transactionSyncer) coSign(ctx context.Context, hash sdk.Hash) error {
	tx := &sdk.AggregateTransaction{
		AbstractTransaction: sdk.AbstractTransaction{
			TransactionInfo: &sdk.TransactionInfo{
				Hash: hash,
			},
		},
	}

	cTx, err := sdk.NewCosignatureTransaction(tx)
	if err != nil {
		return err
	}

	sTx, err := syncer.Account.SignCosignatureTransaction(cTx)
	if err != nil {
		return err
	}

	_, err = syncer.Client.Transaction.AnnounceAggregateBondedCosignature(ctx, sTx)
	if err != nil {
		return err
	}

	return nil
}

func (syncer *transactionSyncer) handleTxn(deadline time.Time, hash sdk.Hash, res chan<- Result) {
	syncer.newUnconfirmed <- &transactionMeta{
		hash:        hash,
		resultCh:    res,
		deadline:    deadline,
		unconfirmed: false,
	}
}

func (syncer *transactionSyncer) collectGarbage() {
	for hash, meta := range syncer.unconfirmedCache {
		if meta.isValid() {
			delete(syncer.unconfirmedCache, hash)
		}
	}
}

const (
	defConnTimeout  = time.Second * 10
	defGCTimeout    = time.Minute * 10
	defLockDuration = 240
	defLockDeadline = time.Hour
	defLockAmount   = 10
	defReadTimeout  = time.Millisecond * 100
)

type transactionMeta struct {
	deadline    time.Time
	hash        sdk.Hash
	resultCh    chan<- Result
	unconfirmed bool
}

func (meta *transactionMeta) isValid() bool {
	now := time.Now()
	return meta.deadline.Before(now) || meta.deadline.Equal(now)
}

type unsignedRequest struct {
	resp chan []*sdk.AggregateTransaction
	hash sdk.Hash
}

type unconfirmedRequest struct {
	resp chan []sdk.Hash
}
