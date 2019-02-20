package catapult_sync

import (
	"time"

	"github.com/proximax-storage/proximax-nem2-sdk-go/sdk"
)

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

type unsignedRequest struct {
	resp chan []*sdk.AggregateTransaction
	hash sdk.Hash
}

type unconfirmedRequest struct {
	resp chan []sdk.Hash
}
