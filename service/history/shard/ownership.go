// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package shard

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/internal/goro"
	"go.temporal.io/server/service/history/configs"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination ownership_mock.go

const (
	shardControllerMembershipUpdateListenerName = "ShardController"

	solReacquireDefaultMaxDuration = 5 * time.Second
)

type (
	// ownership acts as intermediary between membership and the shard controller.
	// Upon receiving membership update events, it calls the controller's
	// acquireShards method, which acquires or closes shards as needed.
	// The controller calls its verifyOwnership method when asked to
	// acquire a shard, to check that membership believes this host should
	// own the shard.
	// The controller should also call reportShardOwnershipLost when a shard
	// ownership lost (SOL) error is received on the shard. Since membership
	// and persistence are different systems, a history host can receive
	// a SOL error before seeing the membership update that informs it of the
	// shards new owner. If configured to do so, ownership can then prevent
	// the old host from "stealing back" the shard before it receives the
	// membership update.
	ownership struct {
		mu struct {
			sync.Mutex
			lastAcquire    time.Time
			solExpirations map[int32]time.Time
		}

		acquireCh               chan struct{}
		config                  *configs.Config
		goros                   goro.Group
		historyServiceResolver  membership.ServiceResolver
		hostInfoProvider        membership.HostInfoProvider
		logger                  log.Logger
		membershipUpdateCh      chan *membership.ChangedEvent
		metricsHandler          metrics.Handler
		solReported             chan struct{}
		solReacquireMaxDuration time.Duration
	}

	shardAcquirer interface {
		acquireShards(context.Context)
	}
)

func newOwnership(
	config *configs.Config,
	historyServiceResolver membership.ServiceResolver,
	hostInfoProvider membership.HostInfoProvider,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ownership {
	hostIdentity := hostInfoProvider.HostInfo().Identity()
	logger = log.With(logger, tag.ComponentShardController, tag.Address(hostIdentity))
	o := &ownership{
		acquireCh:               make(chan struct{}, 1),
		config:                  config,
		historyServiceResolver:  historyServiceResolver,
		hostInfoProvider:        hostInfoProvider,
		logger:                  logger,
		membershipUpdateCh:      make(chan *membership.ChangedEvent, 1),
		metricsHandler:          metricsHandler,
		solReported:             make(chan struct{}, 1),
		solReacquireMaxDuration: solReacquireDefaultMaxDuration,
	}
	o.mu.solExpirations = make(map[int32]time.Time)
	return o
}

func (o *ownership) GetPingChecks() []common.PingCheck {
	return []common.PingCheck{{
		Name:    "shard ownership",
		Timeout: 10 * time.Second,
		Ping: func() []common.Pingable {
			o.mu.Lock()
			defer o.mu.Unlock()
			return nil
		},
	}}
}

func (o *ownership) start(acquirer shardAcquirer) {
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
		case changedEvent := <-o.membershipUpdateCh:
			o.metricsHandler.Counter(metrics.MembershipChangedCounter.GetMetricName()).Record(1)

			o.logger.Info("", tag.ValueRingMembershipChangedEvent,
				tag.NumberProcessed(len(changedEvent.HostsAdded)),
				tag.NumberDeleted(len(changedEvent.HostsRemoved)),
			)

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

func (o *ownership) acquireLoop(ctx context.Context, acquirer shardAcquirer) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.acquireCh:
			o.mu.Lock()
			o.mu.lastAcquire = time.Now()
			o.mu.Unlock()
			acquirer.acquireShards(ctx)
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

// verifyOwnership checks if the shard should be owned by this host's shard
// controller. If membership lists another host as the owner, it returns a
// ShardOwnershipLost error with the correct owner.
// If membership thinks this host should own the shard, but the config
// ShardOwnershipLostReacquireMinDuration is non-zero, and a shard
// ownership lost error was received within that duration, then this will
// also return a ShardOwnershipLost error.
func (o *ownership) verifyOwnership(shardID int32) error {
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
		if time.Now().Before(expiration) {
			// We use a blank OwnerHost so that history clients won't spin
			// in their ownership lost retry loop; they should see this
			// error and perform backoff before retrying, which could allow
			// time for our membership view to get updated.
			return serviceerrors.NewShardOwnershipLost("", hostInfo.GetAddress())
		}
		// If we're past the expiration, we leave the entry in place, so that
		// we still trigger an acquireShards to reconcile our shards.
	}

	return nil
}

// reportShardOwnershipLost should be called by the controller when a shard
// is closed due to recieving
func (o *ownership) reportShardOwnershipLost(shardID int32) {
	minDuration := o.config.ShardOwnershipLostReacquireMinDuration()
	if minDuration <= 0 {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if _, ok := o.mu.solExpirations[shardID]; ok {
		return
	}

	expiration := time.Now().Add(util.Min(minDuration, o.solReacquireMaxDuration))
	o.mu.solExpirations[shardID] = expiration

	select {
	case o.solReported <- struct{}{}:
	default:
	}
}

func (o *ownership) solExpirationLoop(ctx context.Context) {
	const maxDuration time.Duration = 1<<63 - 1
	timer := time.NewTimer(maxDuration)
	defer timer.Stop()

	checkExpirations := func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		now := time.Now()
		doAcquire := false
		nextTimer := maxDuration

		for shardID, expiration := range o.mu.solExpirations {
			if now.Before(expiration) {
				nextTimer = util.Min(expiration.Sub(now), nextTimer)
			} else {
				delete(o.mu.solExpirations, shardID)
				if o.mu.lastAcquire.Before(expiration) {
					doAcquire = true
				}
			}
		}
		if doAcquire {
			o.scheduleAcquire()
		}
		timer.Reset(nextTimer)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-o.solReported:
			if !timer.Stop() {
				<-timer.C
			}
			checkExpirations()
		case <-timer.C:
			checkExpirations()
		}
	}
}
