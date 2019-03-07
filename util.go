package catapult_sync

import (
	"context"
	"time"

	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk"
)

// TODO Get Config from client
func Announce(ctx context.Context, config *sdk.Config, client *sdk.ClientWebsocket, from *sdk.Account, tx sdk.Transaction, opts ...AnnounceOption) error {
	syncer, err := NewTransactionSyncer(ctx, config, from, WithWsClient(client))
	if err != nil {
		return err
	}

	defer syncer.Close()

	return AnnounceFullSync(ctx, syncer, tx, opts...)
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
