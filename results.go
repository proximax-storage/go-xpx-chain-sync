package chainsync

import (
	"github.com/proximax-storage/go-xpx-chain-sdk/sdk"
)

// AnnounceResult is a struct which defines result of announcing txn.
type AnnounceResult struct {
	signedTxn *sdk.SignedTransaction
	err       error
}

func (r *AnnounceResult) Hash() *sdk.Hash {
	return r.signedTxn.Hash
}

func (r *AnnounceResult) Err() error {
	return r.err
}

func (r *AnnounceResult) Tx() *sdk.SignedTransaction {
	return r.signedTxn
}

// ConfirmationResult is a struct which shows result of transaction confirmation
type ConfirmationResult struct {
	hash *sdk.Hash
	tx   sdk.Transaction
	err  error
}

func (r *ConfirmationResult) Hash() *sdk.Hash {
	return r.hash
}

func (r *ConfirmationResult) Err() error {
	return r.err
}

// Tx returns confirmed transaction or nil if error occurs
func (r *ConfirmationResult) Tx() sdk.Transaction {
	return r.tx
}

type UnconfirmedResult struct {
	tx sdk.Transaction
}

func (r *UnconfirmedResult) Hash() *sdk.Hash {
	return r.tx.GetAbstractTransaction().TransactionHash
}

func (r *UnconfirmedResult) Err() error {
	return nil
}

// Tx returns confirmed transaction or nil if error occurs
func (r *UnconfirmedResult) Tx() sdk.Transaction {
	return r.tx
}

type CoSignatureResult struct {
	txHash    *sdk.Hash
	signer    string
	signature *sdk.Signature
}

func (r *CoSignatureResult) Hash() *sdk.Hash {
	return r.txHash
}

func (r *CoSignatureResult) Err() error {
	return nil
}

// Tx returns confirmed transaction or nil if error occurs
func (r *CoSignatureResult) Signer() string {
	return r.signer
}

func (r *CoSignatureResult) Signature() *sdk.Signature {
	return r.signature
}

type AggregatedAddedResult struct {
	tx *sdk.AggregateTransaction
}

func (r *AggregatedAddedResult) Hash() *sdk.Hash {
	return r.tx.TransactionHash
}

func (r *AggregatedAddedResult) Err() error {
	return nil
}
