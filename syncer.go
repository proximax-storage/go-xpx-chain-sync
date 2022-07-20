package catapult_sync

import (
	"context"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/proximax-storage/go-xpx-chain-sdk/sdk"
	"github.com/proximax-storage/go-xpx-chain-sdk/sdk/websocket"
)

type transactionSyncer struct {
	ctx    context.Context
	cancel context.CancelFunc

	// Syncer's account
	Account *sdk.Account

	// Catapult SDK related
	Client   *sdk.Client
	WSClient websocket.CatapultClient

	statusChanel           chan *sdk.StatusInfo
	confirmedAddedChanel   chan sdk.Transaction
	partialAddedChanel     chan sdk.Transaction
	cosignatureChanel      chan *sdk.SignerInfo
	unconfirmedAddedChanel chan sdk.Transaction

	// Unsigned cache
	unsignedCache map[sdk.Hash]sdk.Transaction // contains all the aggregate transactions Syncer's account taking part in TODO Handle possible memory leak when transactions are not confirmed
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
		return nil, errors.New("nil account passed")
	}

	if config == nil {
		return nil, errors.New("nil config passed")
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
		Account:          acc,
		unsignedCache:    make(map[sdk.Hash]sdk.Transaction),
		getUnsigned:      make(chan *unsignedRequest),
		unconfirmedCache: make(map[sdk.Hash]*transactionMeta),
		newUnconfirmed:   make(chan *transactionMeta),

		statusChanel:           make(chan *sdk.StatusInfo, 16),
		confirmedAddedChanel:   make(chan sdk.Transaction, 16),
		partialAddedChanel:     make(chan sdk.Transaction, 16),
		cosignatureChanel:      make(chan *sdk.SignerInfo, 16),
		unconfirmedAddedChanel: make(chan sdk.Transaction, 16),

		gc:  time.NewTicker(cfg.gcTimeout),
		cfg: cfg,
	}

	if cfg.wsClient == nil {
		syncer.WSClient, err = websocket.NewClient(ctx, config)
		if err != nil {
			return nil, errors.Wrap(err, "creating websocket client")
		}
	} else {
		syncer.WSClient = cfg.wsClient
	}

	if cfg.client == nil {
		syncer.Client = sdk.NewClient(http.DefaultClient, config)
	} else {
		syncer.Client = cfg.client
	}

	syncer.Account, err = syncer.Client.AdaptAccount(acc)
	if err != nil {
		return nil, err
	}

	go syncer.WSClient.Listen()

	if err = syncer.subscribe(); err != nil {
		return nil, err
	}

	go syncer.dispatcherLoop()

	return syncer, nil
}

// subscribe initialize listening to websocket
func (sync *transactionSyncer) subscribe() (err error) {
	if err = sync.WSClient.AddStatusHandlers(sync.Account.Address, func(info *sdk.StatusInfo) bool {
		if info == nil {
			// TODO Log that nil is passed
			return false
		}

		sync.statusChanel <- info
		return false
	}); err != nil {
		return errors.Wrap(err, "adding status subscriber")
	}

	if err = sync.WSClient.AddConfirmedAddedHandlers(sync.Account.Address, func(tx sdk.Transaction) bool {
		if tx == nil {
			// TODO Log that nil is passed
			return false
		}

		sync.confirmedAddedChanel <- tx
		return false
	}); err != nil {
		return errors.Wrap(err, "adding confirmed added subscriber")
	}

	if err = sync.WSClient.AddPartialAddedHandlers(sync.Account.Address, func(tx sdk.Transaction) bool {
		if tx == nil {
			// TODO Log that nil is passed
			return false
		}

		sync.partialAddedChanel <- tx
		return false
	}); err != nil {
		return errors.Wrap(err, "adding partial added subscriber")
	}

	if err = sync.WSClient.AddCosignatureHandlers(sync.Account.Address, func(info *sdk.SignerInfo) bool {
		if info == nil {
			// TODO Log that nil is passed
			return false
		}

		sync.cosignatureChanel <- info
		return false
	}); err != nil {
		return errors.Wrap(err, "adding cosignature subscriber")
	}

	if err = sync.WSClient.AddUnconfirmedAddedHandlers(sync.Account.Address, func(tx sdk.Transaction) bool {
		if tx == nil {
			// TODO Log that nil is passed
			return false
		}

		sync.unconfirmedAddedChanel <- tx
		return false
	}); err != nil {
		return errors.Wrap(err, "adding unconfirmed added subscriber")
	}

	return err
}

// dispatcherLoop method does most of Syncer logic.
// It handles all the incoming data from subscriptions and syncs it with underlying cache.
// Also process requests on what transaction to sync and data return requests
func (sync *transactionSyncer) dispatcherLoop() {
	pushResult := func(ch chan<- Result, res Result, closing bool) {
		defer func() {
			recover()
		}()

		ch <- res

		if closing {
			close(ch) // TODO Fix race
		}
	}

	metaGc := func(meta *transactionMeta, res *ConfirmationResult) {
		res.hash = meta.hash
		go pushResult(meta.resultCh, res, true)
		delete(sync.unconfirmedCache, *meta.hash)
	}

	getAbstract := func(tx sdk.Transaction) *sdk.AbstractTransaction {
		atx := tx.GetAbstractTransaction()
		if atx != nil {
			return atx
		}

		panic("Serialization error")
	}

	for {
		select {

		// New transactions to handle
		case meta := <-sync.newUnconfirmed:
			sync.unconfirmedCache[*meta.hash] = meta

		// Listening to websocket
		case status := <-sync.statusChanel: // TODO Parse statuses to return right result
			if meta, ok := sync.unconfirmedCache[*status.Hash]; ok {
				metaGc(meta, &ConfirmationResult{err: errors.New(status.Status)}) // TODO Introduce new error type with all possible Catapult errors
			}
		case confirmed := <-sync.confirmedAddedChanel:
			tx := getAbstract(confirmed)
			if meta, ok := sync.unconfirmedCache[*tx.TransactionHash]; ok {
				metaGc(meta, &ConfirmationResult{tx: confirmed})
			}

			delete(sync.unsignedCache, *confirmed.GetAbstractTransaction().TransactionHash)
		case bonded := <-sync.partialAddedChanel:
			tx := getAbstract(bonded)
			if meta, ok := sync.unconfirmedCache[*tx.TransactionHash]; ok {
				go pushResult(meta.resultCh, &AggregatedAddedResult{
					tx: bonded,
				}, false)
			} else {
				// Unhandled transaction received, saving to cache...
				sync.unsignedCache[*tx.TransactionHash] = bonded
			}
		case cosignature := <-sync.cosignatureChanel:
			if meta, ok := sync.unconfirmedCache[*cosignature.ParentHash]; ok {
				go pushResult(meta.resultCh, &CoSignatureResult{
					txHash:    cosignature.ParentHash,
					signer:    cosignature.Signer,
					signature: cosignature.Signature,
				}, false)
			}
		case unconfirmed := <-sync.unconfirmedAddedChanel:
			tx := getAbstract(unconfirmed)
			if meta, ok := sync.unconfirmedCache[*tx.TransactionHash]; ok {
				meta.unconfirmed = true
				go pushResult(meta.resultCh, &UnconfirmedResult{
					tx: unconfirmed,
				}, false)
			}

		// Value requests
		case req := <-sync.getUnsigned:
			var out []sdk.Transaction

			if req.hash != nil {
				if hash, ok := sync.unsignedCache[*req.hash]; ok {
					out = append(out, hash)
				} else {
					out = append(out, nil)
				}
			} else {
				for _, tx := range sync.unsignedCache {
					out = append(out, tx)
				}
			}

			req.resp <- out
		case req := <-sync.getUnconfirmed:
			var hashes []*sdk.Hash

			for _, meta := range sync.unconfirmedCache {
				if meta.unconfirmed {
					hashes = append(hashes, meta.hash)
				}
			}

			req.resp <- hashes

		// Util
		case <-sync.gc.C:
			sync.collectGarbage(metaGc)
		case <-sync.ctx.Done():
			sync.collectGarbage(metaGc)
			sync.gc.Stop()

			for _, meta := range sync.unconfirmedCache {
				close(meta.resultCh)
			}

			sync.unconfirmedCache = nil
			sync.unsignedCache = nil

			return
		}
	}
}

// Announce simply signs transaction with Syncer account and sends it to Catapult via SDK
func (sync *transactionSyncer) Announce(ctx context.Context, tx sdk.Transaction) (*sdk.SignedTransaction, error) {
	if tx == nil {
		return nil, errors.New("nil transaction passed")
	}

	signedTx, err := sync.Account.Sign(tx)
	if err != nil {
		return nil, err
	}
	transactionType := tx.GetAbstractTransaction().Type
	if transactionType == sdk.AggregateBondedV1 || transactionType == sdk.AggregateBondedV2 {
		_, err = sync.Client.Transaction.AnnounceAggregateBonded(ctx, signedTx)
		if err != nil {
			return signedTx, err
		}
	} else {
		_, err = sync.Client.Transaction.Announce(ctx, signedTx)
		if err != nil {
			return signedTx, err
		}
	}

	return signedTx, nil
}

// AnnounceSync wraps Announce and Sync methods to synchronize and validate transaction announcing.
// Can return multiple results depending on what happening with transaction on catapult side.
func (sync *transactionSyncer) AnnounceSync(ctx context.Context, tx sdk.Transaction, opts ...AnnounceOption) <-chan Result {
	result := new(AnnounceResult)

	resultCh := make(chan Result, 1)
	defer func() {
		resultCh <- result
	}()

	if tx == nil {
		result.err = errors.New("nil transaction passed")
		return resultCh
	}

	transactionType := tx.GetAbstractTransaction().Type
	if transactionType == sdk.AggregateBondedV1 {
		return sync.announceAggregateSync(ctx, tx.(*sdk.AggregateTransactionV1).Deadline, tx, opts...)
	} else if transactionType == sdk.AggregateBondedV2 {
		return sync.announceAggregateSync(ctx, tx.(*sdk.AggregateTransactionV2).Deadline, tx, opts...)
	}

	result.signedTxn, result.err = sync.Announce(ctx, tx)
	if result.err != nil {
		return resultCh
	}

	sync.handleTxn(tx.GetAbstractTransaction().Deadline.Time, result.signedTxn.Hash, resultCh)

	return resultCh
}

// Sync handles announced and signed transaction and returns multiple results through channel
// Pass only hash of announced transactions related to Syncer,
// otherwise transaction won't be handled and would be cleaned after specified deadline
func (sync *transactionSyncer) Sync(deadline time.Time, hash *sdk.Hash) <-chan Result {
	if hash == nil {
		return nil
	}

	resultCh := make(chan Result, 1)
	sync.handleTxn(deadline, hash, resultCh)
	return resultCh
}

// CoSign cosigns any transaction by given hash with Syncer's account.
// If force is false, validates if that transaction exists through some time.
func (sync *transactionSyncer) CoSign(ctx context.Context, hash *sdk.Hash, force bool) error {
	if hash == nil {
		return errors.New("empty hash passed")
	}

	if force {
		return sync.coSign(ctx, hash)
	}

	tx := sync.UnCosignedTransaction(hash)
	if tx != nil {
		return sync.coSign(ctx, hash)
	}

	results := sync.Sync(time.Now().Add(TransactionCosigningTimeout), hash)
	for {
		select {
		case res := <-results:
			switch res.(type) {
			case *AggregatedAddedResult:
				return sync.coSign(ctx, hash)
			case *ConfirmationResult:
				if res.Err() == ErrTxnDeadlineExceeded {
					tx := sync.UnCosignedTransaction(hash)
					if tx != nil {
						return sync.coSign(ctx, hash)
					}

					return ErrCoSignTimeout
				}
				// hmm, very strange behavior...

				return res.Err()
			}
		case <-sync.ctx.Done():
			return sync.ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Unconfirmed returns hashes of all unconfirmed transactions from cache.
func (sync *transactionSyncer) Unconfirmed() []*sdk.Hash { // TODO Return more information than just a hash in separate struct
	out := make(chan []*sdk.Hash, 1)
	sync.getUnconfirmed <- &unconfirmedRequest{resp: out}

	return <-out
}

// UnCosignedTransaction returns aggregate bonded transaction in which Syncer's account signature is requested.
func (sync *transactionSyncer) UnCosignedTransaction(hash *sdk.Hash) sdk.Transaction {
	if hash == nil {
		return nil
	}

	out := make(chan []sdk.Transaction, 1)
	sync.getUnsigned <- &unsignedRequest{resp: out, hash: hash}

	if txs := <-out; txs != nil {
		return txs[0]
	}

	return nil
}

// UnCosignedTransactions returns all aggregate bonded transactions in which Syncer's account is taking part
// and where Syncer's co signature is needed to confirm transaction
// NOTICE: Handles only those transactions which are requested when Syncer is active
func (sync *transactionSyncer) UnCosignedTransactions() []sdk.Transaction {
	out := make(chan []sdk.Transaction, 1)
	sync.getUnsigned <- &unsignedRequest{resp: out}

	return <-out
}

// Context returns Syncer's context
func (sync *transactionSyncer) Context() context.Context {
	return sync.ctx
}

// Close gracefully terminates Syncer, returns error if occurs
func (sync *transactionSyncer) Close() (err error) {
	sync.cancel()
	return
}

func (sync *transactionSyncer) announceAggregateSync(ctx context.Context, deadline *sdk.Deadline, tx sdk.Transaction, opts ...AnnounceOption) <-chan Result {
	result := new(AnnounceResult)

	resultCh := make(chan Result, 32) // 32 cause possible amount of CoSignatureResults is big
	defer func() {
		resultCh <- result
	}()

	result.signedTxn, result.err = sync.Account.Sign(tx)
	if result.err != nil {
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

	result.err = sync.lockFundsSync(ctx, cfg.lockAmount, cfg.lockDuration, cfg.lockDeadline, result.signedTxn)
	if result.err != nil {
		result.err = errors.Wrap(result.err, "can't lock funds")
		return resultCh
	}

	_, result.err = sync.Client.Transaction.AnnounceAggregateBonded(ctx, result.signedTxn)
	if result.err != nil {
		return resultCh
	}

	sync.handleTxn(deadline.Time, result.signedTxn.Hash, resultCh)

	return resultCh
}

func (sync *transactionSyncer) lockFundsSync(ctx context.Context, amount uint64, duration int64, deadline time.Duration, signedTx *sdk.SignedTransaction) error {
	lockTx, err := sync.Client.NewLockFundsTransaction(
		sdk.NewDeadline(deadline),
		sdk.XpxRelative(amount),
		sdk.Duration(duration),
		signedTx,
	)
	if err != nil {
		return err
	}

	signedLockTx, err := sync.Announce(ctx, lockTx)
	if err != nil {
		return err
	}

	results := sync.Sync(lockTx.Deadline.Time, signedLockTx.Hash)

	for {
		select {
		case res := <-results:
			switch res.(type) {
			case *ConfirmationResult:
				return res.Err()
			}
		case <-time.After(TransactionResultsTimeout):
			return ErrCatapultTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (sync *transactionSyncer) coSign(ctx context.Context, hash *sdk.Hash) error {
	sTx, err := sync.Account.SignCosignatureTransaction(sdk.NewCosignatureTransactionFromHash(hash))
	if err != nil {
		return err
	}

	_, err = sync.Client.Transaction.AnnounceAggregateBondedCosignature(ctx, sTx)
	if err != nil {
		return err
	}

	return nil
}

func (sync *transactionSyncer) handleTxn(deadline time.Time, hash *sdk.Hash, res chan<- Result) {
	sync.newUnconfirmed <- &transactionMeta{
		hash:        hash,
		resultCh:    res,
		deadline:    deadline,
		unconfirmed: false,
	}
}

func (sync *transactionSyncer) collectGarbage(gc transactionMetaGCFunc) {
	for _, meta := range sync.unconfirmedCache {
		if meta.isValid() {
			gc(meta, &ConfirmationResult{err: ErrTxnDeadlineExceeded})
		}
	}
}

const (
	defConnTimeout  = time.Second * 10
	defGCTimeout    = time.Minute * 10
	defLockDuration = 240
	defLockDeadline = time.Hour
	defLockAmount   = 10
)

type transactionMeta struct {
	deadline    time.Time
	hash        *sdk.Hash
	resultCh    chan<- Result
	unconfirmed bool
}

func (meta *transactionMeta) isValid() bool {
	now := time.Now()
	return meta.deadline.Before(now) || meta.deadline.Equal(now)
}

type transactionMetaGCFunc func(*transactionMeta, *ConfirmationResult)

type unsignedRequest struct {
	resp chan []sdk.Transaction
	hash *sdk.Hash
}

type unconfirmedRequest struct {
	resp chan []*sdk.Hash
}
