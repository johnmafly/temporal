package shard

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/internal/goro"
	"go.temporal.io/server/service/history/configs"
)

const (
	solReacquireMaxDuration       = 5 * time.Second
	solReacquireExpirationRoundUp = 100 * time.Millisecond
)

type (
	Ownership interface {
		VerifyOwnership(shardID int32) error
		ReportShardOwnershipLost(shardID int32)
	}

	acquirer interface {
		AcquireShards(ctx context.Context)
	}

	ownershipParams struct {
		config                 *configs.Config
		historyServiceResolver membership.ServiceResolver
		hostInfoProvider       membership.HostInfoProvider
		logger                 log.Logger
		timeSource             clock.TimeSource
	}

	ownership struct {
		*ownershipParams

		mu struct {
			sync.Mutex
			lastAcquire    time.Time
			solExpirations map[int32]time.Time
		}

		acquireCh          chan struct{}
		goros              goro.Group
		logger             log.Logger
		membershipUpdateCh chan *membership.ChangedEvent
		solExpiryCh        chan struct{}
	}
)

func newOwnership(params ownershipParams) *ownership {
	hostIdentity := params.hostInfoProvider.HostInfo().Identity()
	logger := log.With(params.logger, tag.ComponentShardController, tag.Address(hostIdentity))

	return &ownership{
		acquireCh:          make(chan struct{}, 1),
		logger:             logger,
		membershipUpdateCh: make(chan *membership.ChangedEvent, 1),
		ownershipParams:    &params,
		solExpiryCh:        make(chan struct{}, 1),
	}
}

func (o *ownership) start(acquirer acquirer) {
	o.goros.Go(func(ctx context.Context) error {
		o.eventLoop(ctx)
		return nil
	})

	o.goros.Go(func(ctx context.Context) error {
		o.acquireLoop(ctx, acquirer)
		return nil
	})

	o.goros.Go(func(ctx context.Context) error {
		o.solExpirationLoop(ctx)
		return nil
	})

	if err := o.historyServiceResolver.AddListener(
		shardControllerMembershipUpdateListenerName,
		o.membershipUpdateCh,
	); err != nil {
		o.logger.Fatal("Error adding listener", tag.Error(err))
	}
}

func (o *ownership) eventLoop(ctx context.Context) {
	acquireTicker := time.NewTicker(o.config.AcquireShardInterval())
	defer acquireTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-acquireTicker.C:
			o.scheduleAcquire()
		case <-o.membershipUpdateCh:
			o.scheduleAcquire()
		}
	}
}

func (o *ownership) scheduleAcquire() {
	select {
	case o.acquireCh <- struct{}{}:
	default:
	}
}

func (o *ownership) acquireLoop(ctx context.Context, acquirer acquirer) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.acquireCh:
			o.mu.Lock()
			o.mu.lastAcquire = o.timeSource.Now()
			o.mu.Unlock()

			acquirer.AcquireShards(ctx)
		}
	}
}

func (o *ownership) solExpirationLoop(ctx context.Context) {
	const maxDuration time.Duration = 1<<63 - 1
	timer := time.NewTimer(maxDuration)
	defer timer.Stop()

	checkWakeups := func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		now := o.timeSource.Now()
		doAcquire := false
		nextTimer := maxDuration

		for shardID, expiration := range o.mu.solExpirations {
			if now.After(expiration) {
				delete(o.mu.solExpirations, shardID)
				if o.mu.lastAcquire.Before(expiration) {
					doAcquire = true
				}
			} else {
				nextTimer = util.Min(expiration.Sub(now), nextTimer)
				nextTimer = roundUpDuration(nextTimer, solReacquireExpirationRoundUp)
			}
		}
		if doAcquire {
			o.scheduleAcquire()
		}
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(nextTimer)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-o.solExpiryCh:
			checkWakeups()
		case <-timer.C:
			checkWakeups()
		}
	}
}

func roundUpDuration(d, roundOn time.Duration) time.Duration {
	rem := d % roundOn
	if rem == 0 {
		return d
	}
	return d + roundOn - rem
}

func (o *ownership) stop() {
	if err := o.historyServiceResolver.RemoveListener(
		shardControllerMembershipUpdateListenerName,
	); err != nil {
		o.logger.Error("Error removing membership update listener", tag.Error(err), tag.OperationFailed)
	}

	o.goros.Cancel()
	o.goros.Wait()
}

func (o *ownership) VerifyOwnership(shardID int32) error {
	ownerInfo, err := o.historyServiceResolver.Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return err
	}

	hostInfo := o.hostInfoProvider.HostInfo()
	if ownerInfo.Identity() != hostInfo.Identity() {
		return serviceerrors.NewShardOwnershipLost(ownerInfo.Identity(), hostInfo.GetAddress())
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	// We can receive a shard ownership lost error from persistence before our
	// membership client has seen the update that caused another history instance
	// to acquire the shard.
	if expiration, ok := o.mu.solExpirations[shardID]; ok {
		if o.timeSource.Now().Before(expiration) {
			// We use a blank OwnerHost so that history clients won't spin
			// in their ownership lost retry loop; they should see this
			// error and perform backoff before retrying, which could allow
			// time for our membership view to get updated.
			return serviceerrors.NewShardOwnershipLost("", hostInfo.GetAddress())
		}
	}

	return nil
}

func (o *ownership) ReportShardOwnershipLost(shardID int32) {
	minDuration := o.config.ShardOwnershipLostReacquireMinDuration()
	if minDuration <= 0 {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	expiration := o.timeSource.Now().
		Add(util.Min(minDuration, solReacquireMaxDuration))

	o.mu.solExpirations[shardID] = expiration

	select {
	case o.solExpiryCh <- struct{}{}:
	default:
	}
}
