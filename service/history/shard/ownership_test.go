package shard

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
)

type (
	ownershipSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		resource   *resourcetest.Test
		config     *configs.Config
	}
)

func TestOwnershipSuite(t *testing.T) {
	s := new(ownershipSuite)
	suite.Run(t, s)
}

func (s *ownershipSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.resource = resourcetest.NewTest(s.controller, primitives.HistoryService)
	s.config = tests.NewDynamicConfig()
}

func (s *ownershipSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ownershipSuite) newController(contextFactory ContextFactory) *ControllerImpl {
	return ControllerProvider(
		s.config,
		s.resource.GetLogger(),
		s.resource.GetHistoryServiceResolver(),
		s.resource.GetMetricsHandler(),
		s.resource.GetHostInfoProvider(),
		contextFactory,
		s.resource.GetTimeSource(),
	).(*ControllerImpl)
}

func (s *ownershipSuite) TestBasic() {
	s.config.NumberOfShards = 1
	shardID := int32(1)

	shard := NewMockControllableContext(s.controller)
	shard.EXPECT().
		GetEngine(gomock.Any()).
		Return(nil, nil)
	shard.EXPECT().
		AssertOwnership(gomock.Any()).
		Return(nil)
	shard.EXPECT().
		IsValid().
		Return(true)

	s.resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).AnyTimes()

	ready := make(chan struct{})
	cf := NewMockContextFactory(s.controller)
	cf.EXPECT().CreateContext(shardID, gomock.Any()).
		DoAndReturn(func(_ int32, _ CloseCallback) (ControllableContext, error) {
			ready <- struct{}{}
			return shard, nil
		})

	s.resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(s.resource.GetHostInfo(), nil).Times(2)

	s.resource.HostInfoProvider.EXPECT().HostInfo().
		Return(s.resource.GetHostInfo()).AnyTimes()

	shardController := s.newController(cf)
	shardController.Start()
	shardController.ownership.membershipUpdateCh <- &membership.ChangedEvent{}

	<-ready
	_, err := shardController.GetShardByID(shardID)
	s.NoError(err)
}
