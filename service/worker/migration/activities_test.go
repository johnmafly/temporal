package migration

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller                    *gomock.Controller
	mockTaskManager               *persistence.MockTaskManager
	mockFrontendClient            *workflowservicemock.MockWorkflowServiceClient
	mockNamespaceReplicationQueue *persistence.MockNamespaceReplicationQueue

	logger         log.Logger
	metricsHandler *metrics.MockHandler
	historyClient  *historyservicemock.MockHistoryServiceClient
}

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockTaskManager = persistence.NewMockTaskManager(s.controller)
	s.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)
	s.mockNamespaceReplicationQueue = persistence.NewMockNamespaceReplicationQueue(s.controller)

	s.logger = log.NewNoopLogger()
	s.metricsHandler = metrics.NewMockHandler(s.controller)
	s.metricsHandler.EXPECT().Timer(metrics.ServiceLatency.GetMetricName()).Return(metrics.NoopTimerMetricFunc).MinTimes(0)
	s.historyClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
}

func (s *activitiesSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *activitiesSuite) TestGenerateAndVerifyReplicationTasks_Success() {
	a := &activities{
		namespaceReplicationQueue: s.mockNamespaceReplicationQueue,
		taskManager:               s.mockTaskManager,
		frontendClient:            s.mockFrontendClient,
		logger:                    log.NewCLILogger(),
	}

	env := s.NewTestActivityEnvironment()
	env.RegisterActivity(a)
	// env.SetWorkerOptions(worker.Options{
	// 	BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	// })

	request := genearteAndVerifyReplicationTasksRequest{}

	_, err := env.ExecuteActivity(a.GenerateAndVerifyReplicationTasks, request)
	s.NoError(err)
}
