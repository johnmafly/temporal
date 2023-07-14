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

package history

import (
	"context"
	"sync"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	cacheEntry struct {
		shardID    int32
		address    rpcAddress
		connection clientConnection
	}

	// A cachingRedirector is a redirector that maintains a cache of shard
	// owners, and uses that cache instead of querying membership for each
	// operation. Cache entries are evicted either for shard ownership lost
	// errors, or for any error that might indicate the history instance
	// is no longer available (including timeouts).
	cachingRedirector struct {
		mu struct {
			sync.RWMutex
			cache map[int32]cacheEntry
		}

		connections            connections
		historyServiceResolver membership.ServiceResolver
	}
)

func newCachingRedirector(
	connections connections,
	historyServiceResolver membership.ServiceResolver,
) *cachingRedirector {
	r := &cachingRedirector{
		connections:            connections,
		historyServiceResolver: historyServiceResolver,
	}
	r.mu.cache = make(map[int32]cacheEntry)
	return r
}

func (r *cachingRedirector) clientForTarget(target operationTarget) (historyservice.HistoryServiceClient, error) {
	if err := target.validate(); err != nil {
		return nil, err
	}
	if target.shardID == 0 {
		connection := r.connections.getOrCreateClientConn(target.address)
		return connection.historyClient, nil
	}
	entry, err := r.getOrCreateEntry(target.shardID)
	if err != nil {
		return nil, err
	}
	return entry.connection.historyClient, nil
}

func (r *cachingRedirector) execute(ctx context.Context, target operationTarget, op clientOperation) error {
	if err := target.validate(); err != nil {
		return err
	}
	if target.shardID == 0 {
		connection := r.connections.getOrCreateClientConn(target.address)
		return op(ctx, connection.historyClient)
	}
	return r.redirectLoop(ctx, target.shardID, op)
}

func (r *cachingRedirector) redirectLoop(ctx context.Context, shardID int32, op clientOperation) error {
	opEntry, err := r.getOrCreateEntry(shardID)
	if err != nil {
		return err
	}
	for {
		if err := common.IsValidContext(ctx); err != nil {
			return err
		}
		opErr := op(ctx, opEntry.connection.historyClient)
		if opErr == nil {
			return opErr
		}
		if maybeHostDownError(opErr) {
			r.cacheDeleteByAddress(opEntry.address)
			return opErr
		}
		solErr, ok := opErr.(*serviceerrors.ShardOwnershipLost)
		if !ok {
			return opErr
		}
		var again bool
		opEntry, again = r.handleSolError(opEntry, solErr)
		if !again {
			return opErr
		}
	}
}

func (r *cachingRedirector) getOrCreateEntry(shardID int32) (cacheEntry, error) {
	r.mu.RLock()
	entry, ok := r.mu.cache[shardID]
	r.mu.RUnlock()
	if ok {
		return entry, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok = r.mu.cache[shardID]
	if ok {
		return entry, nil
	}

	address, err := shardLookup(r.historyServiceResolver, shardID)
	if err != nil {
		return cacheEntry{}, err
	}

	return r.cacheAddLocked(shardID, address), nil
}

func (r *cachingRedirector) cacheAddLocked(shardID int32, addr rpcAddress) cacheEntry {
	// New history instances might reuse the address of a previously live history
	// instance. Since we don't currently close GRPC connections when they become
	// unused or idle, we might have a GRPC connection that has gone into its
	// connection backoff state, due to the previous history instance becoming
	// unreachable. A request on the GRPC connection, intended for the new history
	// instance, would be delayed waiting for the next connection attempt, which
	// could be many seconds.
	// If we're adding a new cache entry for a shard, we take that as a hint that
	// the next request should attempt to connect immediately if required.
	connection := r.connections.getOrCreateClientConn(addr)
	r.connections.resetConnectBackoff(connection)

	entry := cacheEntry{
		shardID:    shardID,
		address:    addr,
		connection: connection,
	}
	r.mu.cache[shardID] = entry

	return entry
}

func (r *cachingRedirector) cacheDeleteByAddress(address rpcAddress) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for shardID, entry := range r.mu.cache {
		if entry.address == address {
			delete(r.mu.cache, shardID)
		}
	}
}

func (r *cachingRedirector) handleSolError(opEntry cacheEntry, solErr *serviceerrors.ShardOwnershipLost) (cacheEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the client operation receives an SOL error, and our cache still has
	// the address we used for the operation, remove it.
	if cached, ok := r.mu.cache[opEntry.shardID]; ok {
		if cached.address == opEntry.address {
			delete(r.mu.cache, cached.shardID)
		}
	}

	// We might update our cache & retry this operation, but only if:
	// - the sol error returned a new owner hint
	// - the hint doesn't match the address we used for the operation
	solErrNewOwner := rpcAddress(solErr.OwnerHost)
	if len(solErrNewOwner) != 0 && solErrNewOwner != opEntry.address {
		return r.cacheAddLocked(opEntry.shardID, solErrNewOwner), true
	}

	return cacheEntry{}, false
}

func maybeHostDownError(opErr error) bool {
	if _, ok := opErr.(*serviceerror.Unavailable); ok {
		return true
	}
	return common.IsContextDeadlineExceededErr(opErr)
}
