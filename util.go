package catapult_sync

import (
	"context"
	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk/websocket"
	"sync"
	"time"

	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk"
)

// Announce announces transaction, waits till it confirmed and returns error if any
func Announce(ctx context.Context, config *sdk.Config, client websocket.CatapultClient, from *sdk.Account, tx sdk.Transaction, opts ...AnnounceOption) error {
	syncer, err := NewTransactionSyncer(ctx, config, from, WithWsClient(client))
	if err != nil {
		return err
	}

	defer syncer.Close()

	return AnnounceFullSync(ctx, syncer, tx, opts...)
}

// AnnounceMany announces transactions, waits till they all confirmed and returns error if any
func AnnounceMany(ctx context.Context, config *sdk.Config, client websocket.CatapultClient, from *sdk.Account, txs []sdk.Transaction, opts ...AnnounceOption) error {
	syncer, err := NewTransactionSyncer(ctx, config, from, WithWsClient(client))
	if err != nil {
		return err
	}

	defer syncer.Close()

	return AnnounceFullSyncMany(ctx, syncer, txs, opts...)
}

// AnnounceFullSync fully synchronise work with Syncer and handles all the incoming Results
// Also it is a reference on how to handle Results for different manipulation and for any kind of business logic.
func AnnounceFullSync(ctx context.Context, syncer TransactionSyncer, tx sdk.Transaction, opts ...AnnounceOption) error {
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
					return res.Err()
				}
			case *ConfirmationResult:
				return res.Err()
			}
		case <-timer.C:
			return ErrCatapultTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func AnnounceFullSyncMany(ctx context.Context, syncer TransactionSyncer, txs []sdk.Transaction, opts ...AnnounceOption) (err error) {
	wg := new(sync.WaitGroup)
	for _, tx := range txs {
		wg.Add(1)
		go func(tx sdk.Transaction) {
			defer wg.Done()
			err = AnnounceFullSync(ctx, syncer, tx, opts...)
		}(tx)
	}

	wg.Wait()

	return
}
