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
	"go.temporal.io/server/internal/goro"
	"go.temporal.io/server/service/history/configs"
)

type (
	Ownership interface {
		VerifyOwnership(shardID int32) error
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

		acquireCh          chan struct{}
		logger             log.Logger
		goros              goro.Group
		membershipUpdateCh chan *membership.ChangedEvent
		wg                 sync.WaitGroup
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

	scheduleAcquire := func() {
		select {
		case o.acquireCh <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-acquireTicker.C:
			scheduleAcquire()
		case <-o.membershipUpdateCh:
			scheduleAcquire()
		}
	}
}

func (o *ownership) acquireLoop(ctx context.Context, acquirer acquirer) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.acquireCh:
			acquirer.AcquireShards(ctx)
		}
	}
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

	return nil
}
