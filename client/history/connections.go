package history

import (
	"sync"

	"google.golang.org/grpc"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/membership"
)

type (
	clientConnection struct {
		historyClient historyservice.HistoryServiceClient
		grpcConn      *grpc.ClientConn
	}

	rpcAddress string

	clientConnections struct {
		mu struct {
			sync.RWMutex
			conns map[rpcAddress]clientConnection
		}

		historyServiceResolver membership.ServiceResolver
		rpcFactory             common.RPCFactory
	}
)

func newConnections(
	historyServiceResolver membership.ServiceResolver,
	rpcFactory common.RPCFactory,
) *clientConnections {
	c := &clientConnections{
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
	}
	c.mu.conns = make(map[rpcAddress]clientConnection)
	return c
}

func (c *clientConnections) getOrCreateClientConn(addr rpcAddress) clientConnection {
	c.mu.RLock()
	cc, ok := c.mu.conns[addr]
	c.mu.RUnlock()
	if ok {
		return cc
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok = c.mu.conns[addr]; ok {
		return cc
	}

	grpcConn := c.rpcFactory.CreateInternodeGRPCConnection(string(addr))
	cc = clientConnection{
		historyClient: historyservice.NewHistoryServiceClient(grpcConn),
		grpcConn:      grpcConn,
	}

	c.mu.conns[addr] = cc
	return cc
}

func (c *clientConnections) getAllClientConns() []clientConnection {
	hostInfos := c.historyServiceResolver.Members()

	var clientConns []clientConnection
	for _, hostInfo := range hostInfos {
		cc := c.getOrCreateClientConn(rpcAddress(hostInfo.GetAddress()))
		clientConns = append(clientConns, cc)
	}

	return clientConns
}
