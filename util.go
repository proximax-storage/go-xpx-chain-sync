package catapult_sync

import (
	"context"
	"time"

	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk"
)

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

	for {
	sel:
		select {
		case res := <-syncer.AnnounceSync(ctx, tx, opts...):
			switch res.(type) {
			case *AnnounceResult:
				if res.Err() != nil {
					return res.Err()
				}
			case *ConfirmationResult:
				return res.Err()
			default:
				continue sel
			}
		case <-timer.C:
			return ErrCatapultTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
