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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"golang.org/x/sync/semaphore"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
)

var (
	invalidShardIdLowerBound = serviceerror.NewInvalidArgument("shard Id cannot be equal or lower than zero")
	invalidShardIdUpperBound = serviceerror.NewInvalidArgument("shard Id cannot be larger than max shard count")
)

type (
	ControllerImpl struct {
		sync.RWMutex
		historyShards map[int32]ControllableContext

		config               *configs.Config
		contextFactory       ContextFactory
		contextTaggedLogger  log.Logger
		hostInfoProvider     membership.HostInfoProvider
		ownership            *ownership
		status               int32
		taggedMetricsHandler metrics.Handler
	}
)

var _ Controller = (*ControllerImpl)(nil)

func ControllerProvider(
	config *configs.Config,
	logger log.Logger,
	historyServiceResolver membership.ServiceResolver,
	metricsHandler metrics.Handler,
	hostInfoProvider membership.HostInfoProvider,
	contextFactory ContextFactory,
) Controller {
	hostIdentity := hostInfoProvider.HostInfo().Identity()
	contextTaggedLogger := log.With(logger, tag.ComponentShardController, tag.Address(hostIdentity))
	taggedMetricsHandler := metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryShardControllerScope))

	ownership := newOwnership(
		config,
		historyServiceResolver,
		hostInfoProvider,
		contextTaggedLogger,
		taggedMetricsHandler,
	)

	return &ControllerImpl{
		config:               config,
		contextFactory:       contextFactory,
		contextTaggedLogger:  contextTaggedLogger,
		historyShards:        make(map[int32]ControllableContext),
		hostInfoProvider:     hostInfoProvider,
		ownership:            ownership,
		taggedMetricsHandler: taggedMetricsHandler,
	}
}

func (c *ControllerImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	c.ownership.start(c)

	c.contextTaggedLogger.Info("", tag.LifeCycleStarted)
}

func (c *ControllerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	c.ownership.stop()

	c.doShutdown()

	c.contextTaggedLogger.Info("", tag.LifeCycleStopped)
}

func (c *ControllerImpl) GetPingChecks() []common.PingCheck {
	return []common.PingCheck{{
		Name:    "shard controller",
		Timeout: 10 * time.Second,
		Ping: func() []common.Pingable {
			// we only need to read but get write lock to make sure we can
			c.Lock()
			defer c.Unlock()
			out := make([]common.Pingable, 0, len(c.historyShards))
			for _, shard := range c.historyShards {
				out = append(out, shard)
			}
			return out
		},
		MetricsName: metrics.ShardControllerLockLatency.GetMetricName(),
	}}
}

func (c *ControllerImpl) Status() int32 {
	return atomic.LoadInt32(&c.status)
}

// GetShardByID returns a shard context for the given namespace and workflow.
// The shard context may not have acquired a rangeid lease yet.
// Callers can use GetEngine on the shard to block on rangeid lease acquisition.
func (c *ControllerImpl) GetShardByNamespaceWorkflow(
	namespaceID namespace.ID,
	workflowID string,
) (Context, error) {
	shardID := c.config.GetShardID(namespaceID, workflowID)
	return c.GetShardByID(shardID)
}

// GetShardByID returns a shard context for the given shard id.
// The shard context may not have acquired a rangeid lease yet.
// Callers can use GetEngine on the shard to block on rangeid lease acquisition.
func (c *ControllerImpl) GetShardByID(
	shardID int32,
) (Context, error) {
	startTime := time.Now().UTC()
	defer func() {
		c.taggedMetricsHandler.Timer(metrics.GetEngineForShardLatency.GetMetricName()).Record(time.Since(startTime))
	}()

	return c.getOrCreateShardContext(shardID)
}

func (c *ControllerImpl) CloseShardByID(shardID int32) {
	startTime := time.Now().UTC()
	defer func() {
		c.taggedMetricsHandler.Timer(metrics.RemoveEngineForShardLatency.GetMetricName()).Record(time.Since(startTime))
	}()

	shard := c.removeShard(shardID, nil)

	// Stop the current shard, if it exists.
	if shard != nil {
		shard.FinishStop()
	}
}

func (c *ControllerImpl) ShardIDs() []int32 {
	c.RLock()
	defer c.RUnlock()

	ids := make([]int32, 0, len(c.historyShards))
	for id := range c.historyShards {
		ids = append(ids, id)
	}
	return ids
}

func (c *ControllerImpl) shardClosedCallback(shard ControllableContext) {
	startTime := time.Now().UTC()
	defer func() {
		c.taggedMetricsHandler.Timer(metrics.RemoveEngineForShardLatency.GetMetricName()).Record(time.Since(startTime))
	}()

	c.taggedMetricsHandler.Counter(metrics.ShardContextClosedCounter.GetMetricName()).Record(1)
	_ = c.removeShard(shard.GetShardID(), shard)

	// Whether shard was in the shards map or not, in both cases we should stop it.
	shard.FinishStop()
}

// getOrCreateShardContext returns a shard context for the given shard ID, creating a new one
// if necessary. If a shard context is created, it will initialize in the background.
// This function won't block on rangeid lease acquisition.
func (c *ControllerImpl) getOrCreateShardContext(shardID int32) (Context, error) {
	if err := c.validateShardId(shardID); err != nil {
		return nil, err
	}
	c.RLock()
	if shard, ok := c.historyShards[shardID]; ok {
		if shard.IsValid() {
			c.RUnlock()
			return shard, nil
		}
		// if shard not valid then proceed to create a new one
	}
	c.RUnlock()

	c.Lock()
	defer c.Unlock()

	// Check again with exclusive lock
	if shard, ok := c.historyShards[shardID]; ok {
		if shard.IsValid() {
			return shard, nil
		}

		// If the shard was invalid and still in the historyShards map, the
		// shardClosedCallback call is in-flight, and will call finishStop.
		_ = c.removeShardLocked(shardID, shard)
	}

	if err := c.ownership.verifyOwnership(shardID); err != nil {
		return nil, err
	}

	hostInfo := c.hostInfoProvider.HostInfo()
	if atomic.LoadInt32(&c.status) == common.DaemonStatusStopped {
		return nil, fmt.Errorf("ControllerImpl for host '%v' shutting down", hostInfo.Identity())
	}

	shard, err := c.contextFactory.CreateContext(shardID, c.shardClosedCallback)
	if err != nil {
		return nil, err
	}
	c.historyShards[shardID] = shard
	c.taggedMetricsHandler.Counter(metrics.ShardContextCreatedCounter.GetMetricName()).Record(1)
	c.contextTaggedLogger.Info("", numShardsTag(len(c.historyShards)))

	return shard, nil
}

func (c *ControllerImpl) removeShard(shardID int32, expected ControllableContext) ControllableContext {
	c.Lock()
	defer c.Unlock()
	return c.removeShardLocked(shardID, expected)
}

func (c *ControllerImpl) removeShardLocked(shardID int32, expected ControllableContext) ControllableContext {
	current, ok := c.historyShards[shardID]
	if !ok {
		return nil
	}
	if expected != nil && current != expected {
		// the shard comparison is a defensive check to make sure we are deleting
		// what we intend to delete.
		return nil
	}

	delete(c.historyShards, shardID)
	c.contextTaggedLogger.Info("", numShardsTag(len(c.historyShards)))
	c.taggedMetricsHandler.Counter(metrics.ShardContextRemovedCounter.GetMetricName()).Record(1)

	return current
}

func (c *ControllerImpl) acquireShards(ctx context.Context) {
	c.taggedMetricsHandler.Counter(metrics.AcquireShardsCounter.GetMetricName()).Record(1)
	startTime := time.Now().UTC()
	defer func() {
		c.taggedMetricsHandler.Timer(metrics.AcquireShardsLatency.GetMetricName()).Record(time.Since(startTime))
	}()

	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundCallerInfo)

	tryAcquire := func(shardID int32) {
		if err := c.ownership.verifyOwnership(shardID); err != nil {
			if IsShardOwnershipLostError(err) {
				// current host is not owner of shard, unload it if it is already loaded.
				c.CloseShardByID(shardID)
			}
			return
		}

		shard, err := c.GetShardByID(shardID)
		if err != nil {
			c.taggedMetricsHandler.Counter(metrics.GetEngineForShardErrorCounter.GetMetricName()).Record(1)
			c.contextTaggedLogger.Error("Unable to create history shard context", tag.Error(err), tag.OperationFailed, tag.ShardID(shardID))
			return
		}

		// Wait up to 1s for the shard to acquire the rangeid lock.
		// After 1s we will move on but the shard will continue trying in the background.
		engineCtx, engineCancel := context.WithTimeout(ctx, 1*time.Second)
		defer engineCancel()
		_, _ = shard.GetEngine(engineCtx)

		assertCtx, assertCancel := context.WithTimeout(ctx, shardIOTimeout)
		defer assertCancel()
		// trust the AssertOwnership will handle shard ownership lost
		_ = shard.AssertOwnership(assertCtx)
	}

	concurrency := int64(util.Max(c.config.AcquireShardConcurrency(), 1))
	sem := semaphore.NewWeighted(concurrency)
	numShards := c.config.NumberOfShards
	randomStartOffset := rand.Int31n(numShards)
	for index := int32(0); index < numShards; index++ {
		shardID := (index+randomStartOffset)%numShards + 1
		if err := sem.Acquire(ctx, 1); err != nil {
			break
		}
		go func() {
			defer sem.Release(1)
			tryAcquire(shardID)
		}()
	}
	_ = sem.Acquire(ctx, concurrency)

	c.RLock()
	numOfOwnedShards := len(c.historyShards)
	c.RUnlock()

	c.taggedMetricsHandler.Gauge(metrics.NumShardsGauge.GetMetricName()).Record(float64(numOfOwnedShards))
}

func (c *ControllerImpl) doShutdown() {
	c.contextTaggedLogger.Info("", tag.LifeCycleStopping)
	c.Lock()
	defer c.Unlock()
	for _, shard := range c.historyShards {
		shard.FinishStop()
	}
	c.historyShards = nil
}

func (c *ControllerImpl) validateShardId(shardID int32) error {
	if shardID <= 0 {
		return invalidShardIdLowerBound
	}
	if shardID > c.config.NumberOfShards {
		return invalidShardIdUpperBound
	}
	return nil
}

func IsShardOwnershipLostError(err error) bool {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		return true
	case *serviceerrors.ShardOwnershipLost:
		return true
	}

	return false
}

func numShardsTag(n int) tag.ZapTag {
	return tag.NewInt("numShards", n)
}
