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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	historyclient "go.temporal.io/server/client/history"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"

	"google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/server/api/adminservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/testing/mocksdk"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	clientmocks "go.temporal.io/server/client"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/searchattribute"
)

type (
	adminHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockResource       *resourcetest.Test
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache *namespace.MockRegistry

		// DEPRECATED
		mockExecutionMgr           *persistence.MockExecutionManager
		mockVisibilityMgr          *manager.MockVisibilityManager
		mockClusterMetadataManager *persistence.MockClusterMetadataManager
		mockClientFactory          *clientmocks.MockFactory
		mockAdminClient            *adminservicemock.MockAdminServiceClient
		mockMetadata               *cluster.MockMetadata
		mockProducer               *persistence.MockNamespaceReplicationQueue

		namespace      namespace.Name
		namespaceID    namespace.ID
		namespaceEntry *namespace.Namespace

		handler *AdminHandler
	}
)

func TestAdminHandlerSuite(t *testing.T) {
	s := new(adminHandlerSuite)
	suite.Run(t, s)
}

func (s *adminHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.namespace = "some random namespace name"
	s.namespaceID = "deadd0d0-c001-face-d00d-000000000000"
	s.namespaceEntry = namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: s.namespace.String(),
			Id:   s.namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)

	s.controller = gomock.NewController(s.T())
	s.mockResource = resourcetest.NewTest(s.controller, primitives.FrontendService)
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockHistoryClient = s.mockResource.HistoryClient
	s.mockExecutionMgr = s.mockResource.ExecutionMgr
	s.mockClusterMetadataManager = s.mockResource.ClusterMetadataMgr
	s.mockClientFactory = s.mockResource.ClientFactory
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockMetadata = s.mockResource.ClusterMetadata
	s.mockVisibilityMgr = s.mockResource.VisibilityManager
	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)

	persistenceConfig := &config.Persistence{
		NumHistoryShards: 1,
	}

	cfg := &Config{
		NumHistoryShards:      4,
		AccessHistoryFraction: dynamicconfig.GetFloatPropertyFn(0.0),
	}
	args := NewAdminHandlerArgs{
		persistenceConfig,
		cfg,
		s.mockResource.GetNamespaceReplicationQueue(),
		s.mockProducer,
		s.mockResource.ESClient,
		s.mockResource.GetVisibilityManager(),
		s.mockResource.GetLogger(),
		s.mockResource.GetTaskManager(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetMetadataManager(),
		s.mockResource.GetClientFactory(),
		s.mockResource.GetClientBean(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetSDKClientFactory(),
		s.mockResource.GetMembershipMonitor(),
		s.mockResource.GetHostInfoProvider(),
		s.mockResource.GetArchiverProvider(),
		s.mockResource.GetMetricsHandler(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesManager(),
		s.mockMetadata,
		s.mockResource.GetArchivalMetadata(),
		health.NewServer(),
		serialization.NewSerializer(),
		clock.NewRealTimeSource(),
		s.mockResource.GetExecutionManager(),
	}
	s.mockMetadata.EXPECT().GetCurrentClusterName().Return(uuid.New()).AnyTimes()
	s.handler = NewAdminHandler(args)
	s.handler.Start()
}

func (s *adminHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
}

func (s *adminHandlerSuite) Test_AddSearchAttributes() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.AddSearchAttributesRequest
		Expected error
	}
	// request validation tests
	testCases1 := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &adminservice.AddSearchAttributesRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}
	for _, testCase := range testCases1 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Elasticsearch is not configured
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases3 := []test{
		{
			Name: "reserved key (empty index)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted (empty index)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute CustomTextField already exists."},
		},
	}
	for _, testCase := range testCases3 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved key (ES configured)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted (ES configured)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute CustomTextField already exists."},
		},
	}
	for _, testCase := range testCases2 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()

	// Start workflow failed.
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(nil, errors.New("start failed"))
	resp, err := handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.Error(err)
	s.Equal("Unable to start temporal-sys-add-search-attributes-workflow workflow: start failed.", err.Error())
	s.Nil(resp)

	// Workflow failed.
	mockRun := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun.EXPECT().Get(gomock.Any(), nil).Return(errors.New("workflow failed"))
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(mockRun, nil)
	resp, err = handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.Error(err)
	s.Equal("Workflow temporal-sys-add-search-attributes-workflow returned an error: workflow failed.", err.Error())
	s.Nil(resp)

	// Success case.
	mockRun.EXPECT().Get(gomock.Any(), nil).Return(nil)
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(mockRun, nil)

	resp, err = handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *adminHandlerSuite) Test_GetSearchAttributes_EmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	resp, err := handler.GetSearchAttributes(ctx, nil)
	s.Error(err)
	s.Equal(&serviceerror.InvalidArgument{Message: "Request is nil."}, err)
	s.Nil(resp)

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(s.namespaceEntry, nil).AnyTimes()

	// Elasticsearch is not configured
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()

	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{Namespace: s.namespace.String()})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *adminHandlerSuite) Test_GetSearchAttributes_NonEmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "random-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	resp, err := handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "another-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("another-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{IndexName: "another-index-name"})
	s.NoError(err)
	s.NotNil(resp)

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		nil, errors.New("random error"))
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "random-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{Namespace: s.namespace.String()})
	s.Error(err)
	s.Nil(resp)
}

func (s *adminHandlerSuite) Test_RemoveSearchAttributes_EmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.RemoveSearchAttributesRequest
		Expected error
	}
	// request validation tests
	testCases1 := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &adminservice.RemoveSearchAttributesRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}
	for _, testCase := range testCases1 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Elasticsearch is not configured
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved search attribute (empty index)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute doesn't exist (empty index)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute ProductId doesn't exist."},
		},
	}
	for _, testCase := range testCases2 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}
}

func (s *adminHandlerSuite) Test_RemoveSearchAttributes_NonEmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.RemoveSearchAttributesRequest
		Expected error
	}
	testCases := []test{
		{
			Name: "reserved search attribute (ES configured)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute doesn't exist (ES configured)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute ProductId doesn't exist."},
		},
	}

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	for _, testCase := range testCases {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Success case.
	s.mockResource.SearchAttributesManager.EXPECT().SaveSearchAttributes(gomock.Any(), "random-index-name", gomock.Any()).Return(nil)

	resp, err := handler.RemoveSearchAttributes(ctx, &adminservice.RemoveSearchAttributesRequest{
		SearchAttributes: []string{
			"CustomKeywordField",
		},
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *adminHandlerSuite) Test_RemoveRemoteCluster_Success() {
	var clusterName = "cluster"
	s.mockClusterMetadataManager.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(nil)

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &adminservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_RemoveRemoteCluster_Error() {
	var clusterName = "cluster"
	s.mockClusterMetadataManager.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(fmt.Errorf("test error"))

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &adminservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordFound_Success() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordNotFound_Success() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ClusterNameConflict() {
	var rpcAddress = uuid.New()
	var clusterId = uuid.New()

	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              s.mockMetadata.GetCurrentClusterName(),
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_FailoverVersionIncrementMismatch() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ShardCount_Invalid() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        5,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ShardCount_Multiple() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_GlobalNamespaceDisabled() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: false,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_InitialFailoverVersionConflict() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		uuid.New(): {InitialFailoverVersion: 0},
	})
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_DescribeCluster_Error() {
	var rpcAddress = uuid.New()

	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_GetClusterMetadata_Error() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_Error() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, fmt.Errorf("test error"))
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_NotApplied_Error() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_DescribeCluster_CurrentCluster_Success() {
	var clusterId = uuid.New()
	clusterName := s.mockMetadata.GetCurrentClusterName()
	s.mockResource.HostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("test"))
	s.mockResource.MembershipMonitor.EXPECT().GetReachableMembers().Return(nil, nil)
	s.mockResource.HistoryServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.HistoryServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.FrontendServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.FrontendServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.MatchingServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.MatchingServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.WorkerServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.WorkerServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.ExecutionMgr.EXPECT().GetName().Return("")
	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName})
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			ClusterMetadata: persistencespb.ClusterMetadata{
				ClusterName:              clusterName,
				HistoryShardCount:        0,
				ClusterId:                clusterId,
				FailoverVersionIncrement: 0,
				InitialFailoverVersion:   0,
				IsGlobalNamespaceEnabled: true,
			},
			Version: 1,
		}, nil)

	resp, err := s.handler.DescribeCluster(context.Background(), &adminservice.DescribeClusterRequest{})
	s.NoError(err)
	s.Equal(resp.GetClusterName(), clusterName)
	s.Equal(resp.GetClusterId(), clusterId)
	s.Equal(resp.GetHistoryShardCount(), int32(0))
	s.Equal(resp.GetFailoverVersionIncrement(), int64(0))
	s.Equal(resp.GetInitialFailoverVersion(), int64(0))
	s.True(resp.GetIsGlobalNamespaceEnabled())
}

func (s *adminHandlerSuite) Test_DescribeCluster_NonCurrentCluster_Success() {
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.HostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("test"))
	s.mockResource.MembershipMonitor.EXPECT().GetReachableMembers().Return(nil, nil)
	s.mockResource.HistoryServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.HistoryServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.FrontendServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.FrontendServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.MatchingServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.MatchingServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.WorkerServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.WorkerServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.ExecutionMgr.EXPECT().GetName().Return("")
	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName})
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			ClusterMetadata: persistencespb.ClusterMetadata{
				ClusterName:              clusterName,
				HistoryShardCount:        0,
				ClusterId:                clusterId,
				FailoverVersionIncrement: 0,
				InitialFailoverVersion:   0,
				IsGlobalNamespaceEnabled: true,
			},
			Version: 1,
		}, nil)

	resp, err := s.handler.DescribeCluster(context.Background(), &adminservice.DescribeClusterRequest{ClusterName: clusterName})
	s.NoError(err)
	s.Equal(resp.GetClusterName(), clusterName)
	s.Equal(resp.GetClusterId(), clusterId)
	s.Equal(resp.GetHistoryShardCount(), int32(0))
	s.Equal(resp.GetFailoverVersionIncrement(), int64(0))
	s.Equal(resp.GetInitialFailoverVersion(), int64(0))
	s.True(resp.GetIsGlobalNamespaceEnabled())
}

func (s *adminHandlerSuite) Test_ListClusters_Success() {
	var pageSize int32 = 1

	s.mockClusterMetadataManager.EXPECT().ListClusterMetadata(gomock.Any(), &persistence.ListClusterMetadataRequest{
		PageSize: int(pageSize),
	}).Return(
		&persistence.ListClusterMetadataResponse{
			ClusterMetadata: []*persistence.GetClusterMetadataResponse{
				{
					ClusterMetadata: persistencespb.ClusterMetadata{ClusterName: "test"},
				},
			}}, nil)

	resp, err := s.handler.ListClusters(context.Background(), &adminservice.ListClustersRequest{
		PageSize: pageSize,
	})
	s.NoError(err)
	s.Equal(1, len(resp.Clusters))
	s.Equal(0, len(resp.GetNextPageToken()))
}

func (s *adminHandlerSuite) TestStreamWorkflowReplicationMessages_ClientToServerBroken() {
	clientClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	serverClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	clusterShardMD := historyclient.EncodeClusterShardMD(
		clientClusterShardID,
		serverClusterShardID,
	)
	ctx := metadata.NewIncomingContext(context.Background(), clusterShardMD)
	clientCluster := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesServer(s.controller)
	clientCluster.EXPECT().Context().Return(ctx).AnyTimes()
	serverCluster := historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesClient(s.controller)
	s.mockHistoryClient.EXPECT().StreamWorkflowReplicationMessages(ctx).Return(serverCluster, nil)

	waitGroupStart := sync.WaitGroup{}
	waitGroupStart.Add(2)
	waitGroupEnd := sync.WaitGroup{}
	waitGroupEnd.Add(2)
	channel := make(chan struct{})

	clientCluster.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesRequest, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		return nil, serviceerror.NewUnavailable("random error")
	})
	serverCluster.EXPECT().Recv().DoAndReturn(func() (*historyservice.StreamWorkflowReplicationMessagesResponse, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		<-channel
		return nil, serviceerror.NewUnavailable("random error")
	})
	_ = s.handler.StreamWorkflowReplicationMessages(clientCluster)
	close(channel)
	waitGroupEnd.Wait()
}

func (s *adminHandlerSuite) TestStreamWorkflowReplicationMessages_ServerToClientBroken() {
	clientClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	serverClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	clusterShardMD := historyclient.EncodeClusterShardMD(
		clientClusterShardID,
		serverClusterShardID,
	)
	ctx := metadata.NewIncomingContext(context.Background(), clusterShardMD)
	clientCluster := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesServer(s.controller)
	clientCluster.EXPECT().Context().Return(ctx).AnyTimes()
	serverCluster := historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesClient(s.controller)
	s.mockHistoryClient.EXPECT().StreamWorkflowReplicationMessages(ctx).Return(serverCluster, nil)

	waitGroupStart := sync.WaitGroup{}
	waitGroupStart.Add(2)
	waitGroupEnd := sync.WaitGroup{}
	waitGroupEnd.Add(2)
	channel := make(chan struct{})

	clientCluster.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesRequest, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		<-channel
		return nil, serviceerror.NewUnavailable("random error")
	})
	serverCluster.EXPECT().Recv().DoAndReturn(func() (*historyservice.StreamWorkflowReplicationMessagesResponse, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		return nil, serviceerror.NewUnavailable("random error")
	})
	_ = s.handler.StreamWorkflowReplicationMessages(clientCluster)
	close(channel)
	waitGroupEnd.Wait()
}
