package catapult_sync

import (
	"context"
	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk/websocket"
	"math/big"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk"
)

// TODO Consider adding cache strategies with persisting for cases with unexpected Syncer terminating
type transactionSyncer struct {
	ctx    context.Context
	cancel context.CancelFunc

	// Syncer's account
	Account *sdk.Account

	// Catapult SDK related
	Client   *sdk.Client
	WSClient websocket.CatapultClient
	Network  sdk.NetworkType

	statusChanel           chan *sdk.StatusInfo
	confirmedAddedChanel   chan sdk.Transaction
	partialAddedChanel     chan *sdk.AggregateTransaction
	cosignatureChanel      chan *sdk.SignerInfo
	unconfirmedAddedChanel chan sdk.Transaction

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
		Network:          config.NetworkType,
		Account:          acc,
		unsignedCache:    make(map[sdk.Hash]*sdk.AggregateTransaction),
		getUnsigned:      make(chan *unsignedRequest),
		unconfirmedCache: make(map[sdk.Hash]*transactionMeta),
		newUnconfirmed:   make(chan *transactionMeta),

		statusChanel:           make(chan *sdk.StatusInfo),
		confirmedAddedChanel:   make(chan sdk.Transaction),
		partialAddedChanel:     make(chan *sdk.AggregateTransaction),
		cosignatureChanel:      make(chan *sdk.SignerInfo),
		unconfirmedAddedChanel: make(chan sdk.Transaction),

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

	go syncer.WSClient.Listen()

	if err = syncer.subscribe(); err != nil {
		return nil, err
	}

	go syncer.dispatcherLoop()

	return syncer, nil
}

// subscribe initialize listening to websocket
func (syncer *transactionSyncer) subscribe() error {
	var err error

	if err := syncer.WSClient.AddStatusHandlers(syncer.Account.Address, func(info *sdk.StatusInfo) bool {
		syncer.statusChanel <- info
		return true
	}); err != nil {
		return errors.Wrap(err, "adding status subscriber")
	}

	if err = syncer.WSClient.AddConfirmedAddedHandlers(syncer.Account.Address, func(transaction sdk.Transaction) bool {
		syncer.confirmedAddedChanel <- transaction
		return false
	}); err != nil {
		return errors.Wrap(err, "adding confirmed added subscriber")
	}

	if err = syncer.WSClient.AddPartialAddedHandlers(syncer.Account.Address, func(transaction *sdk.AggregateTransaction) bool {
		syncer.partialAddedChanel <- transaction
		return false
	}); err != nil {
		return errors.Wrap(err, "adding partial added subscriber")
	}

	if err = syncer.WSClient.AddCosignatureHandlers(syncer.Account.Address, func(info *sdk.SignerInfo) bool {
		syncer.cosignatureChanel <- info
		return false
	}); err != nil {
		return errors.Wrap(err, "adding cosignature subscriber")
	}

	if err = syncer.WSClient.AddUnconfirmedAddedHandlers(syncer.Account.Address, func(transaction sdk.Transaction) bool {
		syncer.unconfirmedAddedChanel <- transaction
		return false
	}); err != nil {
		return errors.Wrap(err, "adding unconfirmed added subscriber")
	}

	return err
}

// dispatcherLoop method does most of Syncer logic.
// It handles all the incoming data from subscriptions and syncs it with underlying cache.
// Also process requests on what transaction to sync and data return requests
func (syncer *transactionSyncer) dispatcherLoop() {
	pushResult := func(ch chan<- Result, res Result, closing bool) {
		defer func() {
			recover()
		}()

		ch <- res

		if closing {
			close(ch)
		}
	}

	metaGc := func(meta *transactionMeta, res *ConfirmationResult) {
		res.hash = meta.hash
		go pushResult(meta.resultCh, res, true)
		delete(syncer.unconfirmedCache, meta.hash)
	}

	for {
		select {

		// New transactions to handle
		case meta := <-syncer.newUnconfirmed:
			syncer.unconfirmedCache[meta.hash] = meta

		// Listening to websocket
		case status := <-syncer.statusChanel: // TODO Parse statuses to return right result
			if meta, ok := syncer.unconfirmedCache[status.Hash]; ok {
				metaGc(meta, &ConfirmationResult{err: errors.New(status.Status)}) // TODO Introduce new error type with all possible Catapult errors
			}
		case confirmed := <-syncer.confirmedAddedChanel:
			if meta, ok := syncer.unconfirmedCache[confirmed.GetAbstractTransaction().Hash]; ok {
				metaGc(meta, &ConfirmationResult{tx: confirmed})
			}

			delete(syncer.unsignedCache, confirmed.GetAbstractTransaction().Hash)
		case bonded := <-syncer.partialAddedChanel:
			if meta, ok := syncer.unconfirmedCache[bonded.GetAbstractTransaction().Hash]; ok {
				go pushResult(meta.resultCh, &AggregatedAddedResult{
					tx: bonded,
				}, false)
			} else {
				// Unhandled transaction received, saving to cache...
				syncer.unsignedCache[bonded.GetAbstractTransaction().Hash] = bonded
			}
		case cosignature := <-syncer.cosignatureChanel:
			if meta, ok := syncer.unconfirmedCache[cosignature.ParentHash]; ok {
				go pushResult(meta.resultCh, &CoSignatureResult{
					txHash:    cosignature.ParentHash,
					signer:    cosignature.Signer,
					signature: cosignature.Signature,
				}, false)
			}
		case unconfirmed := <-syncer.unconfirmedAddedChanel:
			if meta, ok := syncer.unconfirmedCache[unconfirmed.GetAbstractTransaction().Hash]; ok {
				meta.unconfirmed = true
				go pushResult(meta.resultCh, &UnconfirmedResult{
					tx: unconfirmed,
				}, false)
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
			syncer.collectGarbage(metaGc)
		case <-syncer.ctx.Done():
			syncer.collectGarbage(metaGc)
			syncer.gc.Stop()

			for _, meta := range syncer.unconfirmedCache {
				close(meta.resultCh)
			}

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
	result := new(AnnounceResult)

	resultCh := make(chan Result, 1)
	defer func() {
		resultCh <- result
	}()

	if tx == nil {
		result.err = errors.New("nil transaction passed")
		return resultCh
	}

	if tx.GetAbstractTransaction().Type == sdk.AggregateBonded {
		return syncer.announceAggregateSync(ctx, tx.(*sdk.AggregateTransaction), opts...)
	}

	result.signedTxn, result.err = syncer.Announce(ctx, tx)
	if result.err != nil {
		return resultCh
	}

	syncer.handleTxn(tx.GetAbstractTransaction().Deadline.Time, result.signedTxn.Hash, resultCh)

	return resultCh
}

// Sync handles announced and signed transaction and returns multiple results through channel
// Pass only hash of announced transactions related to Syncer,
// otherwise transaction won't be handled and would be cleaned after specified deadline
func (syncer *transactionSyncer) Sync(deadline time.Time, hash sdk.Hash) <-chan Result {
	if hash == "" {
		return nil
	}

	resultCh := make(chan Result, 1)
	syncer.handleTxn(deadline, hash, resultCh)
	return resultCh
}

// CoSign cosigns any transaction by given hash with Syncer's account.
// If force is false, validates if that transaction exists through some time.
func (syncer *transactionSyncer) CoSign(ctx context.Context, hash sdk.Hash, force bool) error {
	if hash == "" {
		return errors.New("empty hash passed")
	}

	if force {
		return syncer.coSign(ctx, hash)
	}

	tx := syncer.UnCosignedTransaction(hash)
	if tx != nil {
		return syncer.coSign(ctx, hash)
	}

	results := syncer.Sync(time.Now().Add(TransactionCosigningTimeout), hash)
	for {
		select {
		case res := <-results:
			switch res.(type) {
			case *AggregatedAddedResult:
				return syncer.coSign(ctx, hash)
			case *ConfirmationResult:
				if res.Err() == ErrTxnDeadlineExceeded {
					tx := syncer.UnCosignedTransaction(hash)
					if tx != nil {
						return syncer.coSign(ctx, hash)
					}

					return ErrCoSignTimeout
				}
				// hmm, very strange behavior...

				return res.Err()
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
	if hash == "" {
		return nil
	}

	out := make(chan []*sdk.AggregateTransaction, 1)
	syncer.getUnsigned <- &unsignedRequest{resp: out, hash: hash}

	if txs := <-out; txs != nil {
		return txs[0]
	}

	return nil
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

	close(syncer.statusChanel)
	close(syncer.confirmedAddedChanel)
	close(syncer.unconfirmedAddedChanel)
	close(syncer.cosignatureChanel)
	close(syncer.partialAddedChanel)

	err = syncer.WSClient.Close()

	return
}

func (syncer *transactionSyncer) announceAggregateSync(ctx context.Context, tx *sdk.AggregateTransaction, opts ...AnnounceOption) <-chan Result {
	result := new(AnnounceResult)

	resultCh := make(chan Result, 32) // 32 cause possible amount of CoSignatureResults is big
	defer func() {
		resultCh <- result
	}()

	result.signedTxn, result.err = syncer.Account.Sign(tx)
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

	result.err = syncer.lockFundsSync(ctx, cfg.lockAmount, cfg.lockDuration, cfg.lockDeadline, result.signedTxn)
	if result.err != nil {
		result.err = errors.Wrap(result.err, "can't lock funds")
		return resultCh
	}

	_, result.err = syncer.Client.Transaction.AnnounceAggregateBonded(ctx, result.signedTxn)
	if result.err != nil {
		return resultCh
	}

	syncer.handleTxn(tx.Deadline.Time, result.signedTxn.Hash, resultCh)

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

	results := syncer.Sync(lockTx.Deadline.Time, signedLockTx.Hash)

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

func (syncer *transactionSyncer) coSign(ctx context.Context, hash sdk.Hash) error {
	sTx, err := syncer.Account.SignCosignatureTransaction(sdk.NewCosignatureTransactionFromHash(hash))
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

func (syncer *transactionSyncer) collectGarbage(gc transactionMetaGCFunc) {
	for _, meta := range syncer.unconfirmedCache {
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
	hash        sdk.Hash
	resultCh    chan<- Result
	unconfirmed bool
}

func (meta *transactionMeta) isValid() bool {
	now := time.Now()
	return meta.deadline.Before(now) || meta.deadline.Equal(now)
}

type transactionMetaGCFunc func(*transactionMeta, *ConfirmationResult)

type unsignedRequest struct {
	resp chan []*sdk.AggregateTransaction
	hash sdk.Hash
}

type unconfirmedRequest struct {
	resp chan []sdk.Hash
}
