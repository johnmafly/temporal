package history

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	clientOp func(ctx context.Context, client historyservice.HistoryServiceClient) error

	ownershipLookup struct {
		shardID int32
		address rpcAddress
	}

	redirector interface {
		lookupOwner(shardID int32) (ownershipLookup, error)
		execute(context.Context, ownershipLookup, clientOp) error
	}

	nonCachingRedirector struct {
		connections            *clientConnections
		historyServiceResolver membership.ServiceResolver
	}
)

func newNonCachingRedirector(
	connections *clientConnections,
	historyServiceResolver membership.ServiceResolver,
) *nonCachingRedirector {
	return &nonCachingRedirector{
		connections:            connections,
		historyServiceResolver: historyServiceResolver,
	}
}

func (r *nonCachingRedirector) lookupOwner(shardID int32) (ownershipLookup, error) {
	hostInfo, err := r.historyServiceResolver.Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return ownershipLookup{}, err
	}

	return ownershipLookup{
		shardID: shardID,
		address: rpcAddress(hostInfo.GetAddress()),
	}, nil
}

func (r *nonCachingRedirector) execute(
	ctx context.Context,
	lookup ownershipLookup,
	op clientOp,
) error {
	addr := lookup.address
	for {
		if err := common.IsValidContext(ctx); err != nil {
			return err
		}
		clientConn := r.connections.getOrCreateClientConn(addr)
		err := op(ctx, clientConn.historyClient)
		if s, ok := err.(*serviceerrors.ShardOwnershipLost); ok && len(s.OwnerHost) != 0 {
			// TODO: consider emitting a metric for number of redirects
			addr = rpcAddress(s.OwnerHost)
		} else {
			return err
		}
	}
}
