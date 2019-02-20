package catapult_sync

import (
	"context"
	"math/big"
	"time"

	"github.com/pkg/errors"

	"github.com/proximax-storage/proximax-nem2-sdk-go/sdk"
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
	unsignedCache map[sdk.Hash]*sdk.AggregateTransaction // contains all the aggregate transactions Syncer's account taking part in
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
	if acc != nil {
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
		Client:           sdk.NewClient(cfg.httpClient, config),
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
func (b *transactionSyncer) subscribe() (err error) {
	b.status, err = b.WSClient.Subscribe.Status(b.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to status ")
		return
	}

	b.confirmed, err = b.WSClient.Subscribe.ConfirmedAdded(b.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to confirmed")
		return
	}

	b.bonded, err = b.WSClient.Subscribe.PartialAdded(b.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to partial")
		return
	}

	b.cosigners, err = b.WSClient.Subscribe.Cosignature(b.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to cosigners")
		return
	}

	b.unconfirmed, err = b.WSClient.Subscribe.UnconfirmedAdded(b.Account.Address)
	if err != nil {
		err = errors.Wrap(err, "error while listening to unconfirmed transactions ")
		return
	}

	return
}

// dispatcherLoop method does most of Syncer logic.
// It handles all the incoming data from subscriptions and syncs it with underlying cache.
// Also process requests on what transaction to sync and data return requests
func (b *transactionSyncer) dispatcherLoop() {
	pushResult := func(ch chan<- Result, res Result) {
		ch <- res
	}

	for {
		select {

		// New transactions to handle
		case meta := <-b.newUnconfirmed:
			b.unconfirmedCache[meta.hash] = meta

		// Listening to websocket
		case status := <-b.status.Ch: // TODO Parse statuses to return right result
			if meta, ok := b.unconfirmedCache[status.Hash]; ok {
				go pushResult(meta.resultCh, &ConfirmationResult{
					hash: meta.hash,
					err:  errors.New(status.Status), // TODO Introduce new error type with all possible Catapult errors
				})

				delete(b.unconfirmedCache, status.Hash)
			}
		case confirmed := <-b.confirmed.Ch:
			if meta, ok := b.unconfirmedCache[confirmed.GetAbstractTransaction().Hash]; ok {
				go pushResult(meta.resultCh, &ConfirmationResult{
					hash: meta.hash,
					tx:   confirmed,
				})

				delete(b.unconfirmedCache, confirmed.GetAbstractTransaction().Hash)
			}

			if _, ok := b.unsignedCache[confirmed.GetAbstractTransaction().Hash]; ok {
				delete(b.unsignedCache, confirmed.GetAbstractTransaction().Hash)
			}
		case bonded := <-b.bonded.Ch:
			if meta, ok := b.unconfirmedCache[bonded.GetAbstractTransaction().Hash]; ok {
				go pushResult(meta.resultCh, &AggregatedAddedResult{
					tx: bonded,
				})
			} else {
				// Unhandled transaction received, saving to cache...
				b.unsignedCache[bonded.GetAbstractTransaction().Hash] = bonded
			}
		case cosignature := <-b.cosigners.Ch:
			if meta, ok := b.unconfirmedCache[cosignature.ParentHash]; ok {
				go pushResult(meta.resultCh, &CoSignatureResult{
					txHash:    cosignature.ParentHash,
					signer:    cosignature.Signer,
					signature: cosignature.Signature,
				})
			}
		case unconfirmed := <-b.unconfirmed.Ch:
			if meta, ok := b.unconfirmedCache[unconfirmed.GetAbstractTransaction().Hash]; ok {
				meta.unconfirmed = true
				go pushResult(meta.resultCh, &UnconfirmedResult{
					tx: unconfirmed,
				})
			}

		// Value requests
		case req := <-b.getUnsigned:
			var out []*sdk.AggregateTransaction

			if req.hash != "" {
				if hash, ok := b.unsignedCache[req.hash]; ok {
					out = append(out, hash)
				} else {
					out = append(out, nil)
				}
			} else {
				for _, tx := range b.unsignedCache {
					out = append(out, tx)
				}
			}

			req.resp <- out
		case req := <-b.getUnconfirmed:
			var hashes []sdk.Hash

			for _, meta := range b.unconfirmedCache {
				if meta.unconfirmed {
					hashes = append(hashes, meta.hash)
				}
			}

			req.resp <- hashes

		// Util
		case <-b.gc.C:
			b.collectGarbage()
		case <-b.ctx.Done():
			b.collectGarbage()
			b.gc.Stop()

			b.unconfirmedCache = nil
			b.unsignedCache = nil

			return
		}
	}
}

// AnnounceUnSync simply signs transaction with Syncer account and sends it to Catapult via SDK
func (b *transactionSyncer) AnnounceUnSync(ctx context.Context, tx sdk.Transaction) (*sdk.SignedTransaction, error) {
	if tx == nil {
		return nil, errors.New("nil transaction passed")
	}

	signedTx, err := b.Account.Sign(tx)
	if err != nil {
		return nil, err
	}

	if tx.GetAbstractTransaction().Type == sdk.AggregateBonded {
		_, err = b.Client.Transaction.AnnounceAggregateBonded(ctx, signedTx)
		if err != nil {
			return signedTx, err
		}
	} else {
		_, err = b.Client.Transaction.Announce(ctx, signedTx)
		if err != nil {
			return signedTx, err
		}
	}

	return signedTx, nil
}

// Sync handles announced and signed transaction and returns multiple results through channel
// Pass only hash of announced transactions related to Syncer,
// otherwise transaction won't be handled and would be cleaned after specified deadline
// TODO Possible delete of this method? Cause of no ability to check if txn was announced
func (b *transactionSyncer) Sync(deadline time.Time, hash sdk.Hash) <-chan Result {
	resultCh := make(chan Result)
	b.handleTxn(deadline, hash, resultCh)
	return resultCh
}

// Announce wraps AnnounceUnSync and Sync methods to synchronize and validate transaction announcing.
// Can return multiple results depending on what happening with transaction on catapult side.
func (b *transactionSyncer) Announce(ctx context.Context, tx sdk.Transaction, opts ...AnnounceOption) <-chan Result {
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
		return b.announceAggregateSync(ctx, tx.(*sdk.AggregateTransaction), opts...)
	}

	signedTxn, err := b.AnnounceUnSync(ctx, tx)
	if err != nil {
		return resultCh
	}

	result.signedTxn = signedTxn

	b.handleTxn(tx.GetAbstractTransaction().Deadline.Time, signedTxn.Hash, resultCh)

	return resultCh
}

func (b *transactionSyncer) AnnounceFull(ctx context.Context, tx sdk.Transaction) (sdk.Hash, error) {
	var timeout time.Duration
	if tx.GetAbstractTransaction().Type == sdk.AggregateBonded {
		timeout = AggregateTransactionDeadline
	} else {
		timeout = TransactionDeadline
	}

loop:
	for {
		select {
		case res := <-b.Announce(ctx, tx):
			switch res.(type) {
			case *AnnounceResult:
				if res.Err() != nil {
					return "", res.Err()
				}
			case *ConfirmationResult:
				return res.Hash(), res.Err()
			default:
				continue loop
			}
		case <-time.After(timeout):
			return "", ErrCatapultTimeout
		}
	}
}

func (b *transactionSyncer) announceAggregateSync(ctx context.Context, tx *sdk.AggregateTransaction, opts ...AnnounceOption) <-chan Result {
	var result *AnnounceResult
	var err error

	resultCh := make(chan Result, 32) // 32 cause possible amount of CoSignatureResult is big
	defer func() {
		result.err = err
		resultCh <- result
	}()

	signedTx, err := b.Account.Sign(tx)
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

	err = b.lockFundsSync(ctx, cfg.lockAmount, cfg.lockDuration, cfg.lockDeadline, signedTx)
	if err != nil {
		err = errors.Wrap(err, "can't lock funds")
		return resultCh
	}

	_, err = b.Client.Transaction.AnnounceAggregateBonded(ctx, signedTx)
	if err != nil {
		return resultCh
	}

	b.handleTxn(tx.Deadline.Time, signedTx.Hash, resultCh)

	return resultCh
}

func (b *transactionSyncer) lockFundsSync(ctx context.Context, amount, duration int64, deadline time.Duration, signedTx *sdk.SignedTransaction) error {
	if amount < 10 {
		return errors.New("lock amount have to be bigger than 10")
	}

	lockTx, err := sdk.NewLockFundsTransaction(
		sdk.NewDeadline(deadline),
		sdk.XpxRelative(amount),
		big.NewInt(duration),
		signedTx,
		b.Network,
	)
	if err != nil {
		return err
	}

	signedLockTx, err := b.AnnounceUnSync(ctx, lockTx)
	if err != nil {
		return err
	}

	return (<-b.Sync(lockTx.Deadline.Time, signedLockTx.Hash)).Err()
}

func (b *transactionSyncer) Unconfirmed() []sdk.Hash { // TODO Return more information than just a hash in separate struct
	out := make(chan []sdk.Hash, 1)
	b.getUnconfirmed <- &unconfirmedRequest{resp: out}

	return <-out
}

// Close gracefully terminates Syncer, returns error if occurs
func (b *transactionSyncer) Close() (err error) {
	b.cancel()

	err = b.confirmed.Unsubscribe()
	err = b.status.Unsubscribe()
	err = b.bonded.Unsubscribe()

	return
}

func (b *transactionSyncer) handleTxn(deadline time.Time, hash sdk.Hash, res chan<- Result) {
	b.newUnconfirmed <- &transactionMeta{
		hash:        hash,
		resultCh:    res,
		deadline:    deadline,
		unconfirmed: false,
	}
}

func (b *transactionSyncer) collectGarbage() {
	for hash, meta := range b.unconfirmedCache {
		if meta.isValid() {
			delete(b.unconfirmedCache, hash)
		}
	}
}
