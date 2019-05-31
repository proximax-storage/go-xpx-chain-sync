package catapult_sync

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/proximax-storage/go-xpx-catapult-sdk/sdk"
)

const TestUrl = "http://bcstage1.xpxsirius.io:3000"
const NetworkType = sdk.PublicTest

const MainPrivateKey = "349FB8AC38C9C4BD393B2E90E2CAB4ECBFA4E8088A6D840075BDEA1E22259956"
const DefaultBalance = 5000000000000

var PrivateKeys = []string{
	"6FD4CB80311B61083BE4AADC508EC34F0A76E8BECD414288A6E66E7C7E317FB7",
	"8E7230A591B011C9AA30CB2A429563D2B871087E050FA44061803D8FB931BB82",
	"C0ACC2412FBD5C4B47A49C538A7F9BB546CA6EF3742EA8AD2E85FDF7C3A484AE",
	"98D2CD411EA366330AC7A2A44FDE6D32808DD2277C392A3F81894B3CE3433183",
	"59AB3B2D66A2B176B45D7E1DD3549043DDAF0050B9A1B4A3D3D2F37A1ED744C9",
	"6F5E173D77BD74CD18B22DBF2E67546ED444B2E276EFAA057A9BAA1C7BE5E2E9",
	"C9ACD8BD5874D126070060087209445044C1C810029591D88C25380502AAEF04",
	"305541DA5175EF4F2E8929656CFAC9F526967F0F4C1D371BE99EE8FB570F2A9B",
	"45AEE4CEF41F43394AA4CFFC5123B5348BB9EF5BBDF15CB7F4ACF0E5F2BCE2B3",
	"BE438E3273E9E7E065AC7FC6D7ED6B13C6360461F26820E39989EFA8ED77B0B0",
}

var syncer, _ = newSyncer(context.Background(), TestUrl, NetworkType, MainPrivateKey)

func TestTransactionSyncer_AnnounceMany(t *testing.T) {
	ctx := context.Background()

	txs := make([]sdk.Transaction, len(PrivateKeys))
	for i, key := range PrivateKeys {
		acc, _ := sdk.NewAccountFromPrivateKey(key, syncer.Network)

		tx, err := sdk.NewTransferTransaction(
			sdk.NewDeadline(time.Hour),
			acc.Address,
			[]*sdk.Mosaic{sdk.XpxRelative(1)},
			sdk.NewPlainMessage(""),
			syncer.Network,
		)
		assert.Nil(t, err)

		txs[i] = tx
	}

	assert.Nil(t, AnnounceFullSyncMany(ctx, syncer, txs))
}

func TestTransactionSyncer_AnnounceFullSync_Transfer(t *testing.T) {
	ctx := context.Background()

	err := prepareAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}

	acc, err := sdk.NewAccountFromPrivateKey(PrivateKeys[2], syncer.Network)
	assert.Nil(t, err)

	err = sendMosaic(
		ctx,
		syncer,
		acc.PublicAccount,
		sdk.Xpx(100),
		sdk.NewPlainMessage("Add mosaic to test account"))
	assert.Nil(t, err)
}

func TestTransactionSyncer_AnnounceFullSync_Bonded(t *testing.T) {
	ctx := context.Background()

	err := prepareAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}

	acc, err := sdk.NewAccountFromPrivateKey(PrivateKeys[2], syncer.Network)
	assert.Nil(t, err)

	tx1, _ := sdk.NewTransferTransaction(
		sdk.NewDeadline(time.Hour),
		syncer.Account.PublicAccount.Address,
		[]*sdk.Mosaic{sdk.XpxRelative(10)},
		sdk.NewPlainMessage("tx1"),
		syncer.Network,
	)
	tx1.ToAggregate(acc.PublicAccount)

	tx2, _ := sdk.NewTransferTransaction(
		sdk.NewDeadline(time.Hour),
		acc.PublicAccount.Address,
		[]*sdk.Mosaic{sdk.XpxRelative(20)},
		sdk.NewPlainMessage("tx2"),
		syncer.Network,
	)
	tx2.ToAggregate(syncer.Account.PublicAccount)

	aTx, _ := sdk.NewBondedAggregateTransaction(
		sdk.NewDeadline(time.Hour),
		[]sdk.Transaction{
			tx1,
			tx2,
		},
		syncer.Network,
	)

	results := syncer.AnnounceSync(ctx, aTx)

loop:
	for {
		select {
		case res, ok := <-results:

			if !ok {
				assert.FailNow(t, "results chanel was closed")
			}

			switch res.(type) {
			case *AnnounceResult:
				if res.Err() != nil {
					assert.Nil(t, res.Err())
					break loop
				}

				scotx, err := acc.SignCosignatureTransaction(sdk.NewCosignatureTransactionFromHash(res.Hash()))
				if err != nil {
					assert.Nil(t, err)
					break loop
				}

				wg := &sync.WaitGroup{}
				wg.Add(1)

				err = syncer.WSClient.AddPartialAddedHandlers(acc.Address, func(transaction *sdk.AggregateTransaction) bool {
					wg.Done()
					return true
				})

				if err != nil {
					wg.Done()
					assert.FailNow(t, "error adding subscriber to PartialAdded topic")
				}

				wg.Wait()

				_, err = syncer.Client.Transaction.AnnounceAggregateBondedCosignature(ctx, scotx)
				if err != nil {
					assert.Nil(t, err)
					break loop
				}
			case *CoSignatureResult:
				assert.True(t, res.(*CoSignatureResult).Signer() == strings.ToUpper(acc.PublicAccount.PublicKey))
			case *ConfirmationResult:
				assert.Nil(t, res.Err())
				break loop
			}
		case <-time.After(TransactionResultsTimeout):
			assert.FailNow(t, "timeout exceeded")
		}
	}
}

// Prepare account for tests
func prepareAccounts(ctx context.Context) error {
	// Check amount of xpx for each account.
	// If amount lower than DefaultBalance, then we send DefaultBalance to account
	for _, privateKey := range PrivateKeys {

		acc, err := sdk.NewAccountFromPrivateKey(privateKey, syncer.Network)
		if err != nil {
			return err
		}

		// Account can not exist, so we skip the error
		accountInfo, err := syncer.Client.Account.GetAccountInfo(ctx, acc.PublicAccount.Address)

		var amount uint64 = 0

		if accountInfo != nil {
			for _, mosaic := range accountInfo.Mosaics {
				if mosaic.MosaicId.String() == sdk.XpxMosaicId.String() {
					amount = mosaic.Amount.Uint64()
				}
			}
		}

		if amount < DefaultBalance {
			err := sendMosaic(
				ctx,
				syncer,
				acc.PublicAccount,
				sdk.Xpx(DefaultBalance),
				sdk.NewPlainMessage("Add mosaic to test account"))

			if err != nil {
				return err
			}

		}
	}

	return nil
}

func newSyncer(ctx context.Context, url string, network sdk.NetworkType, key string) (*transactionSyncer, error) {
	cfg, err := sdk.NewConfig(url, network)
	if err != nil {
		return nil, err
	}

	acc, err := sdk.NewAccountFromPrivateKey(key, network)
	if err != nil {
		return nil, err
	}

	syncer, err := NewTransactionSyncer(ctx, cfg, acc)
	if err != nil {
		return nil, err
	}

	return syncer.(*transactionSyncer), err
}

func sendMosaic(ctx context.Context, syncer *transactionSyncer, acc *sdk.PublicAccount, mosaic *sdk.Mosaic, message *sdk.Message) error {
	tx, err := sdk.NewTransferTransaction(
		sdk.NewDeadline(time.Hour),
		acc.Address,
		[]*sdk.Mosaic{mosaic},
		message,
		syncer.Network,
	)
	if err != nil {
		return err
	}

	return AnnounceFullSync(ctx, syncer, tx)
}
