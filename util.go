package catapult_sync

import (
	"context"
	"sync"
	"time"

	"github.com/proximax-storage/go-xpx-chain-sdk/sdk"
	"github.com/proximax-storage/go-xpx-chain-sdk/sdk/websocket"
)

// Announce announces transaction, waits till it confirmed and returns hash or error if any
func Announce(ctx context.Context, config *sdk.Config, client websocket.CatapultClient, from *sdk.Account, tx sdk.Transaction, opts ...AnnounceOption) (*ConfirmationResult, error) {
	syncer, err := NewTransactionSyncer(ctx, config, from, WithWsClient(client))
	if err != nil {
		return nil, err
	}

	defer syncer.Close()

	return AnnounceFullSync(ctx, syncer, tx, opts...)
}

// AnnounceMany announces transactions, waits till they all confirmed and returns hashes or error for every
func AnnounceMany(ctx context.Context, config *sdk.Config, client websocket.CatapultClient, from *sdk.Account, txs []sdk.Transaction, opts ...AnnounceOption) ([]*ConfirmationResult, error) {
	syncer, err := NewTransactionSyncer(ctx, config, from, WithWsClient(client))
	if err != nil {
		return nil, err
	}

	defer syncer.Close()

	return AnnounceFullSyncMany(ctx, syncer, txs, opts...)
}

// AnnounceFullSync fully synchronise work with Syncer and handles all the incoming Results
// Also it is a reference on how to handle Results for different manipulation and for any kind of business logic.
func AnnounceFullSync(ctx context.Context, syncer TransactionSyncer, tx sdk.Transaction, opts ...AnnounceOption) (*ConfirmationResult, error) {
	var timeout time.Duration

	isAggregated := tx.GetAbstractTransaction().Type == sdk.AggregateBonded
	if isAggregated {
		timeout = tx.GetAbstractTransaction().Deadline.Sub(time.Now())
	} else {
		timeout = TransactionResultsTimeout
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	results := syncer.AnnounceSync(ctx, tx, opts...)

	for {
		select {
		case res := <-results:
			switch res.(type) {
			case *AnnounceResult:
				if res.Err() != nil {
					return nil, res.Err()
				}
			case *ConfirmationResult:
				return res.(*ConfirmationResult), nil
			}
		case <-timer.C:
			return nil, ErrCatapultTimeout
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-syncer.Context().Done():
			return nil, syncer.Context().Err()
		}
	}
}

// AnnounceFullSyncMany announces and waits till success or error for every transaction
// returns slices of hashes and errors with results of announcing
func AnnounceFullSyncMany(ctx context.Context, syncer TransactionSyncer, txs []sdk.Transaction, opts ...AnnounceOption) ([]*ConfirmationResult, error) {
	var (
		errg error
		once sync.Once
		wg   sync.WaitGroup
	)

	results := make([]*ConfirmationResult, len(txs))
	for i, tx := range txs {
		wg.Add(1)
		go func(i int, tx sdk.Transaction) {
			defer wg.Done()
			var err error
			results[i], err = AnnounceFullSync(ctx, syncer, tx, opts...)
			if err != nil {
				once.Do(func() {
					errg = err
				})
			}
		}(i, tx)
	}

	wg.Wait()

	return results, errg
}
