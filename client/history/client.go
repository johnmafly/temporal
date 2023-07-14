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

// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/rpcwrappers -service history

package history

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
)

var _ historyservice.HistoryServiceClient = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = time.Second * 30 * debug.TimeoutMultiplier
)

type clientImpl struct {
	connections     *clientConnections
	logger          log.Logger
	numberOfShards  int32
	redirector      redirector
	timeout         time.Duration
	tokenSerializer common.TaskTokenSerializer
}

// NewClient creates a new history service gRPC client
func NewClient(
	historyServiceResolver membership.ServiceResolver,
	logger log.Logger,
	numberOfShards int32,
	rpcFactory common.RPCFactory,
	timeout time.Duration,
) historyservice.HistoryServiceClient {
	connections := newConnections(historyServiceResolver, rpcFactory)
	return &clientImpl{
		connections:     connections,
		logger:          logger,
		numberOfShards:  numberOfShards,
		redirector:      newNonCachingRedirector(connections, historyServiceResolver),
		timeout:         timeout,
		tokenSerializer: common.NewProtoTaskTokenSerializer(),
	}
}

func (c *clientImpl) DescribeHistoryHost(
	ctx context.Context,
	request *historyservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeHistoryHostResponse, error) {

	var err error
	var lookup ownershipLookup

	if request.GetShardId() != 0 {
		lookup, err = c.lookupByShardID(request.GetShardId())
	} else if request.GetWorkflowExecution() != nil {
		lookup, err = c.lookupByWorkflowID(request.GetNamespaceId(), request.GetWorkflowExecution().GetWorkflowId())
	} else {
		lookup = ownershipLookup{
			address: rpcAddress(request.GetHostAddress()),
		}
	}
	if err != nil {
		return nil, err
	}

	var response *historyservice.DescribeHistoryHostResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, lookup, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) GetReplicationMessages(
	ctx context.Context,
	request *historyservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetReplicationMessagesResponse, error) {
	requestsByClient := make(map[historyservice.HistoryServiceClient]*historyservice.GetReplicationMessagesRequest)

	for _, token := range request.Tokens {
		lookup, err := c.lookupByShardID(token.GetShardId())
		if err != nil {
			return nil, err
		}
		client := c.connections.getOrCreateClientConn(lookup.address).historyClient

		if _, ok := requestsByClient[client]; !ok {
			requestsByClient[client] = &historyservice.GetReplicationMessagesRequest{
				ClusterName: request.ClusterName,
			}
		}

		req := requestsByClient[client]
		req.Tokens = append(req.Tokens, token)
	}

	var wg sync.WaitGroup
	wg.Add(len(requestsByClient))
	respChan := make(chan *historyservice.GetReplicationMessagesResponse, len(requestsByClient))
	errChan := make(chan error, 1)
	for client, req := range requestsByClient {
		go func(client historyservice.HistoryServiceClient, request *historyservice.GetReplicationMessagesRequest) {
			defer wg.Done()

			ctx, cancel := c.createContext(ctx)
			defer cancel()
			resp, err := client.GetReplicationMessages(ctx, request, opts...)
			if err != nil {
				c.logger.Warn("Failed to get replication tasks from client", tag.Error(err))
				// Returns service busy error to notify replication
				if _, ok := err.(*serviceerror.ResourceExhausted); ok {
					select {
					case errChan <- err:
					default:
					}
				}
				return
			}
			respChan <- resp
		}(client, req)
	}

	wg.Wait()
	close(respChan)
	close(errChan)

	response := &historyservice.GetReplicationMessagesResponse{ShardMessages: make(map[int32]*replicationspb.ReplicationMessages)}
	for resp := range respChan {
		for shardID, tasks := range resp.ShardMessages {
			response.ShardMessages[shardID] = tasks
		}
	}
	var err error
	if len(errChan) > 0 {
		err = <-errChan
	}
	return response, err
}

func (c *clientImpl) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetReplicationStatusResponse, error) {
	clientConns := c.connections.getAllClientConns()
	respChan := make(chan *historyservice.GetReplicationStatusResponse, len(clientConns))
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(len(clientConns))
	for _, client := range clientConns {
		historyClient := client.historyClient
		go func(client historyservice.HistoryServiceClient) {
			defer wg.Done()
			resp, err := historyClient.GetReplicationStatus(ctx, request, opts...)
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
			} else {
				respChan <- resp
			}
		}(historyClient)
	}
	wg.Wait()
	close(respChan)
	close(errChan)

	response := &historyservice.GetReplicationStatusResponse{}
	for resp := range respChan {
		response.Shards = append(response.Shards, resp.Shards...)
	}

	if len(errChan) > 0 {
		err := <-errChan
		return response, err
	}

	return response, nil
}

func (c *clientImpl) StreamWorkflowReplicationMessages(
	ctx context.Context,
	opts ...grpc.CallOption,
) (historyservice.HistoryService_StreamWorkflowReplicationMessagesClient, error) {
	ctxMetadata, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("missing cluster & shard ID metadata")
	}
	_, targetClusterShardID, err := DecodeClusterShardMD(ctxMetadata)
	if err != nil {
		return nil, err
	}
	lookup, err := c.lookupByShardID(targetClusterShardID.ShardID)
	if err != nil {
		return nil, err
	}
	cc := c.connections.getOrCreateClientConn(lookup.address)
	return cc.historyClient.StreamWorkflowReplicationMessages(
		metadata.NewOutgoingContext(ctx, ctxMetadata),
		opts...,
	)
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) lookupByWorkflowID(namespaceID, workflowID string) (ownershipLookup, error) {
	key := common.WorkflowIDToHistoryShard(namespaceID, workflowID, c.numberOfShards)
	return c.lookupByShardID(key)
}

func (c *clientImpl) lookupByShardID(shardID int32) (ownershipLookup, error) {
	if shardID <= 0 {
		return ownershipLookup{}, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid ShardID: %d", shardID))
	}
	return c.redirector.lookupOwner(shardID)
}

func (c *clientImpl) executeWithRedirect(ctx context.Context,
	lookup ownershipLookup,
	op func(ctx context.Context, client historyservice.HistoryServiceClient) error,
) error {
	return c.redirector.execute(ctx, lookup, op)
}
