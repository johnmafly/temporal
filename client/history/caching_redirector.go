package history

import (
	"context"
	"sync"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	cachingRedirector struct {
		mu struct {
			sync.RWMutex
			lookupCache map[int32]ownershipLookup
		}
		connections            *clientConnections
		historyServiceResolver membership.ServiceResolver
	}
)

func newCachingRedirector(
	connections *clientConnections,
	historyServiceResolver membership.ServiceResolver,
) *cachingRedirector {
	r := &cachingRedirector{
		connections:            connections,
		historyServiceResolver: historyServiceResolver,
	}
	r.mu.lookupCache = make(map[int32]ownershipLookup)
	return r
}

func (r *cachingRedirector) lookupOwner(shardID int32) (ownershipLookup, error) {
	r.mu.RLock()
	lookup, ok := r.mu.lookupCache[shardID]
	r.mu.Unlock()
	if ok {
		return lookup, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	lookup, ok = r.mu.lookupCache[shardID]
	if ok {
		return lookup, nil
	}

	hostInfo, err := r.historyServiceResolver.Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return ownershipLookup{}, err
	}

	return r.cacheAddLocked(shardID, rpcAddress(hostInfo.GetAddress())), nil
}

func (r *cachingRedirector) execute(
	ctx context.Context,
	lookup ownershipLookup,
	op clientOp,
) error {
	var again bool
	for {
		if err := common.IsValidContext(ctx); err != nil {
			return err
		}
		clientConn := r.connections.getOrCreateClientConn(lookup.address)
		opErr := op(ctx, clientConn.historyClient)
		if lookup.shardID == 0 || opErr == nil {
			// No cache updates for successful requests or those directed at a
			// specific history address.
			return opErr
		}
		if maybeHostDownError(opErr) {
			r.cacheDelete(lookup.address)
			return opErr
		}
		solErr, ok := opErr.(*serviceerrors.ShardOwnershipLost)
		if !ok {
			return opErr
		}
		lookup, again = r.handleSolError(lookup, solErr)
		if !again {
			return opErr
		}
	}
}

func (r *cachingRedirector) cacheAddLocked(shardID int32, addr rpcAddress) ownershipLookup {
	lookup := ownershipLookup{
		shardID: shardID,
		address: addr,
	}
	r.mu.lookupCache[shardID] = lookup
	cc := r.connections.getOrCreateClientConn(addr)
	cc.grpcConn.ResetConnectBackoff()

	return lookup
}

func (r *cachingRedirector) cacheDelete(address rpcAddress) {
	// maybeHostDownError: Delete the entry for all shards that point to the same
	// host, so that we perform new shard lookups.
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cl := range r.mu.lookupCache {
		if cl.address == address {
			delete(r.mu.lookupCache, cl.shardID)
		}
	}
}

func (r *cachingRedirector) handleSolError(lookup ownershipLookup, solErr *serviceerrors.ShardOwnershipLost) (ownershipLookup, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.mu.lookupCache, lookup.shardID)

	newAddr, ok := r.validateAddressFromSolError(lookup, solErr)
	if !ok {
		return ownershipLookup{}, false
	}

	return r.cacheAddLocked(lookup.shardID, newAddr), true
}

func (r *cachingRedirector) validateAddressFromSolError(lookup ownershipLookup, solErr *serviceerrors.ShardOwnershipLost) (rpcAddress, bool) {
	// We only want to repeat this request here, with a new address,
	// if:
	// 1 we got an sol error
	// 2 the sol error returned a new owner
	// 3 the sol reported owner is not the same as the address we just used
	// 4 the sol reported owner matches what we get with lookup
	if solErr.OwnerHost != string(lookup.address) {
		if hostInfo, err := r.historyServiceResolver.Lookup(convert.Int32ToString(lookup.shardID)); err == nil {
			if solErr.OwnerHost == hostInfo.GetAddress() {
				return rpcAddress(solErr.OwnerHost), true
			}
		}
	}
	return "", false
}

func maybeHostDownError(opErr error) bool {
	if _, ok := opErr.(*serviceerror.Unavailable); ok {
		return true
	}
	return common.IsContextDeadlineExceededErr(opErr)
}
