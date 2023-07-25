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

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

// TODO add UT for the following
//  * DeleteHistoryBranch
//  * GetHistoryTree
//  * GetAllHistoryTreeBranches

type (
	HistoryEventsPacket struct {
		nodeID            int64
		transactionID     int64
		prevTransactionID int64
		events            []*historypb.HistoryEvent
	}

	HistoryEventsSuite struct {
		suite.Suite
		*require.Assertions

		store      p.ExecutionManager
		serializer serialization.Serializer
		logger     log.Logger

		Ctx    context.Context
		Cancel context.CancelFunc
	}
)

func NewHistoryEventsSuite(
	t *testing.T,
	store p.ExecutionStore,
	logger log.Logger,
) *HistoryEventsSuite {
	eventSerializer := serialization.NewSerializer()
	return &HistoryEventsSuite{
		Assertions: require.New(t),
		store: p.NewExecutionManager(
			store,
			eventSerializer,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		serializer: eventSerializer,
		logger:     logger,
	}
}

func (s *HistoryEventsSuite) SetupSuite() {

}

func (s *HistoryEventsSuite) TearDownSuite() {

}

func (s *HistoryEventsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)
}

func (s *HistoryEventsSuite) TearDownTest() {
	s.Cancel()
}

func (s *HistoryEventsSuite) TestAppendSelect_First() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)

	eventsPacket := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket)

	s.Equal(eventsPacket.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket.events, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *HistoryEventsSuite) TestAppendSelect_NonShadowing() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)
	var events []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events = append(events, eventsPacket0.events...)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)
	events = append(events, eventsPacket1.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket1.events, s.listHistoryEvents(shardID, branchToken, 4, 6))
	s.Equal(events, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *HistoryEventsSuite) TestAppendSelect_Shadowing() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	eventsPacket10 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket10)
	events0 = append(events0, eventsPacket10.events...)

	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))

	eventsPacket11 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket11)
	events1 = append(events1, eventsPacket11.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket11.events, s.listHistoryEvents(shardID, branchToken, 4, 6))
	s.Equal(events1, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelect_NoShadowing() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	eventsPacket10 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket10)
	events0 = append(events0, eventsPacket10.events...)

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 4)
	eventsPacket11 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket11)
	events1 = append(events1, eventsPacket11.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, newBranchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket10.events, s.listHistoryEvents(shardID, branchToken, 4, 6))
	s.Equal(eventsPacket11.events, s.listHistoryEvents(shardID, newBranchToken, 4, 6))
	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelect_Shadowing_NonLastBranch() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	))

	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)
	events0 = append(events0, eventsPacket1.events...)
	events1 = append(events1, eventsPacket1.events...)

	eventsPacket20 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket1.transactionID+1,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket20)
	events0 = append(events0, eventsPacket20.events...)

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 6)
	eventsPacket21 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket1.transactionID+2,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket21)
	events1 = append(events1, eventsPacket21.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, newBranchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket1.events, s.listHistoryEvents(shardID, branchToken, 4, 6))
	s.Equal(eventsPacket1.events, s.listHistoryEvents(shardID, newBranchToken, 4, 6))
	s.Equal(eventsPacket20.events, s.listHistoryEvents(shardID, branchToken, 6, 7))
	s.Equal(eventsPacket21.events, s.listHistoryEvents(shardID, newBranchToken, 6, 7))
	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelect_Shadowing_LastBranch() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	))

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 4)
	eventsPacket20 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket20)
	events0 = append(events0, eventsPacket20.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, newBranchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket20.events, s.listHistoryEvents(shardID, newBranchToken, 4, 6))
	s.Equal(events0, s.listAllHistoryEvents(shardID, newBranchToken))

	eventsPacket21 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+3,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket21)
	events1 = append(events1, eventsPacket21.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, newBranchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket21.events, s.listHistoryEvents(shardID, newBranchToken, 4, 6))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendSelectTrim() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)
	var events []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events = append(events, eventsPacket0.events...)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)
	events = append(events, eventsPacket1.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	))

	s.trimHistoryBranch(shardID, branchToken, eventsPacket1.nodeID, eventsPacket1.transactionID)

	s.Equal(events, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelectTrim_NonLastBranch() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)
	events0 = append(events0, eventsPacket1.events...)
	events1 = append(events1, eventsPacket1.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	))

	eventsPacket20 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket1.transactionID+2,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket20)
	events0 = append(events0, eventsPacket20.events...)

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 6)
	eventsPacket21 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket1.transactionID+3,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket21)
	events1 = append(events1, eventsPacket21.events...)

	if rand.Intn(2)%2 == 0 {
		s.trimHistoryBranch(shardID, branchToken, eventsPacket20.nodeID, eventsPacket20.transactionID)
	} else {
		s.trimHistoryBranch(shardID, newBranchToken, eventsPacket21.nodeID, eventsPacket21.transactionID)
	}

	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelectTrim_LastBranch() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)
	var events []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events = append(events, eventsPacket0.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	))

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 4)
	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket1)
	events = append(events, eventsPacket1.events...)

	s.appendHistoryEvents(shardID, newBranchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+3,
		eventsPacket0.transactionID,
	))

	s.trimHistoryBranch(shardID, newBranchToken, eventsPacket1.nodeID, eventsPacket1.transactionID)

	s.Equal(events, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendBatches() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	eventsPacket2 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket1.transactionID+100,
		eventsPacket1.transactionID,
	)
	eventsPacket3 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket2.transactionID+100,
		eventsPacket2.transactionID,
	)

	s.appendRawHistoryBatches(shardID, branchToken, eventsPacket1)
	s.appendRawHistoryBatches(shardID, branchToken, eventsPacket2)
	s.appendRawHistoryBatches(shardID, branchToken, eventsPacket3)
	s.Equal(eventsPacket1.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	expectedEvents := append(eventsPacket1.events, append(eventsPacket2.events, eventsPacket3.events...)...)
	events := s.listAllHistoryEvents(shardID, branchToken)
	s.Equal(expectedEvents, events)
}

func (s *HistoryEventsSuite) TestForkDeleteBranch_DeleteBaseBranchFirst() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	br1Token, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, br1Token, eventsPacket0)

	s.appendHistoryEvents(shardID, br1Token, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	))

	br2Token := s.forkHistoryBranch(shardID, br1Token, 4)
	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, br2Token, eventsPacket1)

	// delete branch1, should only delete branch1:[4,5], keep branch1:[1,2,3] as it is used as ancestor by branch2
	s.deleteHistoryBranch(shardID, br1Token)
	// verify branch1:[1,2,3] still remains
	s.Equal(eventsPacket0.events, s.listAllHistoryEvents(shardID, br1Token))
	// verify branch2 is not affected
	s.Equal(append(eventsPacket0.events, eventsPacket1.events...), s.listAllHistoryEvents(shardID, br2Token))

	// delete branch2, should delete branch2:[4,5], and also should delete ancestor branch1:[1,2,3] as it is no longer
	// used by anyone
	s.deleteHistoryBranch(shardID, br2Token)

	// at this point, both branch1 and branch2 are deleted.
	_, err = s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: br1Token,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.LastEventID,
		PageSize:    1,
	})
	s.Error(err, "Workflow execution history not found.")

	_, err = s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: br2Token,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.LastEventID,
		PageSize:    1,
	})
	s.Error(err, "Workflow execution history not found.")
}

func (s *HistoryEventsSuite) TestForkDeleteBranch_DeleteForkedBranchFirst() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	br1Token, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)

	transactionID := rand.Int63()
	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		transactionID,
		0,
	)
	s.appendHistoryEvents(shardID, br1Token, eventsPacket0)
	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		transactionID+1,
		transactionID,
	)
	s.appendHistoryEvents(shardID, br1Token, eventsPacket1)

	br2Token := s.forkHistoryBranch(shardID, br1Token, 4)
	s.appendHistoryEvents(shardID, br2Token, s.newHistoryEvents(
		[]int64{4, 5},
		transactionID+2,
		transactionID,
	))

	// delete branch2, should only delete branch2:[4,5], keep branch1:[1,2,3] [4,5] as it is by branch1
	s.deleteHistoryBranch(shardID, br2Token)
	// verify branch1 is not affected
	s.Equal(append(eventsPacket0.events, eventsPacket1.events...), s.listAllHistoryEvents(shardID, br1Token))

	// branch2:[4,5] should be deleted
	_, err = s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: br2Token,
		MinEventID:  4,
		MaxEventID:  common.LastEventID,
		PageSize:    1,
	})
	s.Error(err, "Workflow execution history not found.")

	// delete branch1, should delete branch1:[1,2,3] [4,5]
	s.deleteHistoryBranch(shardID, br1Token)

	// branch1 should be deleted
	_, err = s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: br1Token,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.LastEventID,
		PageSize:    1,
	})
	s.Error(err, "Workflow execution history not found.")
}

func (s *HistoryEventsSuite) TestAppendSelect_WAT() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		nil,
		nil,
		nil,
	)
	s.NoError(err)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		1,
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)

	eventsPacket2 := s.newHistoryEvents(
		[]int64{4},
		2,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket2)

	eventsPacket3 := s.newHistoryEvents(
		[]int64{5, 6, 7},
		3,
		eventsPacket2.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket3)

	eventsPacket4 := s.newHistoryEvents(
		[]int64{8, 9, 10},
		4,
		eventsPacket3.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket4)

	eventsPacket5 := s.newHistoryEvents(
		[]int64{11},
		5,
		eventsPacket4.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket5)

	eventsPacket6 := s.newHistoryEvents(
		[]int64{12, 13},
		6,
		eventsPacket5.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket6)

	eventsPacket7 := s.newHistoryEvents(
		[]int64{14, 15},
		7,
		eventsPacket6.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket7)

	eventsPacket8 := s.newHistoryEvents(
		[]int64{16},
		8,
		eventsPacket7.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket8)

	eventsPacket9 := s.newHistoryEvents(
		[]int64{17, 18},
		9,
		eventsPacket8.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket9)

	eventsPacket10 := s.newHistoryEvents(
		[]int64{19, 20, 21},
		10,
		eventsPacket9.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket10)

	eventsPacket11 := s.newHistoryEvents(
		[]int64{22},
		11,
		eventsPacket10.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket11)

	eventsPacket12 := s.newHistoryEvents(
		[]int64{23, 24, 25, 26},
		12,
		eventsPacket11.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket12)

	eventsPacket13 := s.newHistoryEvents(
		[]int64{27, 28, 29},
		13,
		eventsPacket12.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket13)

	eventsPacket14 := s.newHistoryEvents(
		[]int64{30},
		14,
		eventsPacket13.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket14)

	eventsPacket15 := s.newHistoryEvents(
		[]int64{31, 32},
		15,
		eventsPacket14.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket15)

	eventsPacket16 := s.newHistoryEvents(
		[]int64{33, 34, 35},
		16,
		eventsPacket15.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket16)

	eventsPacket17 := s.newHistoryEvents(
		[]int64{36},
		17,
		eventsPacket16.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket17)

	eventsPacket18 := s.newHistoryEvents(
		[]int64{37, 38},
		18,
		eventsPacket17.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket18)

	eventsPacket19 := s.newHistoryEvents(
		[]int64{39, 40, 41},
		19,
		eventsPacket18.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket19)

	eventsPacket20 := s.newHistoryEvents(
		[]int64{42},
		20,
		eventsPacket19.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket20)

	eventsPacket21 := s.newHistoryEvents(
		[]int64{43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70},
		21,
		eventsPacket20.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket21)

	eventsPacket22 := s.newHistoryEvents(
		[]int64{71, 72},
		22,
		eventsPacket21.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket22)

	eventsPacket23 := s.newHistoryEvents(
		[]int64{73},
		23,
		eventsPacket22.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket23)

	eventsPacket24 := s.newHistoryEvents(
		[]int64{74},
		24,
		eventsPacket23.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket24)

	eventsPacket25 := s.newHistoryEvents(
		[]int64{75, 76, 77},
		25,
		eventsPacket24.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket25)

	eventsPacket26 := s.newHistoryEvents(
		[]int64{78},
		26,
		eventsPacket25.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket26)

	eventsPacket27 := s.newHistoryEvents(
		[]int64{79, 80, 81, 82, 83},
		27,
		eventsPacket26.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket27)

	eventsPacket28 := s.newHistoryEvents(
		[]int64{84},
		28,
		eventsPacket27.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket28)

	eventsPacket29 := s.newHistoryEvents(
		[]int64{85, 86},
		29,
		eventsPacket28.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket29)

	eventsPacket30 := s.newHistoryEvents(
		[]int64{87},
		30,
		eventsPacket29.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket30)

	eventsPacket31 := s.newHistoryEvents(
		[]int64{88, 89, 90, 91, 92},
		31,
		eventsPacket30.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket31)

	eventsPacket32 := s.newHistoryEvents(
		[]int64{93, 94},
		32,
		eventsPacket31.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket32)

	eventsPacket33 := s.newHistoryEvents(
		[]int64{95},
		33,
		eventsPacket32.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket33)

	eventsPacket34 := s.newHistoryEvents(
		[]int64{96, 97, 98, 99},
		34,
		eventsPacket33.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket34)

	eventsPacket35 := s.newHistoryEvents(
		[]int64{100, 101, 102},
		35,
		eventsPacket34.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket35)

	eventsPacket36 := s.newHistoryEvents(
		[]int64{103},
		36,
		eventsPacket35.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket36)

	eventsPacket37 := s.newHistoryEvents(
		[]int64{104, 105},
		37,
		eventsPacket36.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket37)

	eventsPacket38 := s.newHistoryEvents(
		[]int64{106, 107, 108},
		38,
		eventsPacket37.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket38)

	eventsPacket39 := s.newHistoryEvents(
		[]int64{109},
		39,
		eventsPacket38.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket39)

	eventsPacket40 := s.newHistoryEvents(
		[]int64{110, 111},
		40,
		eventsPacket39.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket40)

	eventsPacket41 := s.newHistoryEvents(
		[]int64{112, 113, 114},
		41,
		eventsPacket40.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket41)

	eventsPacket42 := s.newHistoryEvents(
		[]int64{115},
		42,
		eventsPacket41.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket42)

	eventsPacket43 := s.newHistoryEvents(
		[]int64{116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143},
		43,
		eventsPacket42.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket43)

	eventsPacket44 := s.newHistoryEvents(
		[]int64{144, 145},
		44,
		eventsPacket43.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket44)

	eventsPacket45 := s.newHistoryEvents(
		[]int64{146},
		45,
		eventsPacket44.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket45)

	eventsPacket46 := s.newHistoryEvents(
		[]int64{147},
		46,
		eventsPacket45.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket46)

	eventsPacket47 := s.newHistoryEvents(
		[]int64{148, 149, 150},
		47,
		eventsPacket46.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket47)

	eventsPacket48 := s.newHistoryEvents(
		[]int64{151, 152},
		48,
		eventsPacket47.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket48)

	eventsPacket49 := s.newHistoryEvents(
		[]int64{153},
		49,
		eventsPacket48.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket49)

	eventsPacket50 := s.newHistoryEvents(
		[]int64{154},
		50,
		eventsPacket49.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket50)

	eventsPacket51 := s.newHistoryEvents(
		[]int64{155, 156},
		51,
		eventsPacket50.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket51)

	eventsPacket52 := s.newHistoryEvents(
		[]int64{157},
		52,
		eventsPacket51.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket52)

	eventsPacket53 := s.newHistoryEvents(
		[]int64{158, 159, 160},
		53,
		eventsPacket52.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket53)

	eventsPacket54 := s.newHistoryEvents(
		[]int64{161, 162},
		54,
		eventsPacket53.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket54)

	eventsPacket55 := s.newHistoryEvents(
		[]int64{163},
		55,
		eventsPacket54.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket55)

	eventsPacket56 := s.newHistoryEvents(
		[]int64{164, 165, 166},
		56,
		eventsPacket55.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket56)

	eventsPacket57 := s.newHistoryEvents(
		[]int64{167, 168},
		57,
		eventsPacket56.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket57)

	eventsPacket58 := s.newHistoryEvents(
		[]int64{169},
		58,
		eventsPacket57.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket58)

	eventsPacket59 := s.newHistoryEvents(
		[]int64{170, 171, 172, 173},
		59,
		eventsPacket58.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket59)

	eventsPacket60 := s.newHistoryEvents(
		[]int64{174, 175, 176},
		60,
		eventsPacket59.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket60)

	eventsPacket61 := s.newHistoryEvents(
		[]int64{177},
		61,
		eventsPacket60.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket61)

	eventsPacket62 := s.newHistoryEvents(
		[]int64{178, 179},
		62,
		eventsPacket61.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket62)

	eventsPacket63 := s.newHistoryEvents(
		[]int64{180, 181, 182},
		63,
		eventsPacket62.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket63)

	eventsPacket64 := s.newHistoryEvents(
		[]int64{183},
		64,
		eventsPacket63.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket64)

	eventsPacket65 := s.newHistoryEvents(
		[]int64{184, 185},
		65,
		eventsPacket64.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket65)

	eventsPacket66 := s.newHistoryEvents(
		[]int64{186, 187, 188},
		66,
		eventsPacket65.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket66)

	eventsPacket67 := s.newHistoryEvents(
		[]int64{189},
		67,
		eventsPacket66.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket67)

	eventsPacket68 := s.newHistoryEvents(
		[]int64{190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217},
		68,
		eventsPacket67.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket68)

	eventsPacket69 := s.newHistoryEvents(
		[]int64{218, 219},
		69,
		eventsPacket68.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket69)

	eventsPacket70 := s.newHistoryEvents(
		[]int64{220},
		70,
		eventsPacket69.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket70)

	eventsPacket71 := s.newHistoryEvents(
		[]int64{221, 222, 223, 224, 225},
		71,
		eventsPacket70.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket71)

	eventsPacket72 := s.newHistoryEvents(
		[]int64{226, 227, 228, 229, 230},
		72,
		eventsPacket71.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket72)

	eventsPacket73 := s.newHistoryEvents(
		[]int64{231},
		73,
		eventsPacket72.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket73)

	eventsPacket74 := s.newHistoryEvents(
		[]int64{232, 233},
		74,
		eventsPacket73.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket74)

	eventsPacket75 := s.newHistoryEvents(
		[]int64{234},
		75,
		eventsPacket74.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket75)

	eventsPacket76 := s.newHistoryEvents(
		[]int64{235, 236, 237},
		76,
		eventsPacket75.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket76)

	eventsPacket77 := s.newHistoryEvents(
		[]int64{238, 239},
		77,
		eventsPacket76.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket77)

	eventsPacket78 := s.newHistoryEvents(
		[]int64{240},
		78,
		eventsPacket77.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket78)

	eventsPacket79 := s.newHistoryEvents(
		[]int64{241, 242, 243},
		79,
		eventsPacket78.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket79)

	eventsPacket80 := s.newHistoryEvents(
		[]int64{244, 245},
		80,
		eventsPacket79.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket80)

	eventsPacket81 := s.newHistoryEvents(
		[]int64{246},
		81,
		eventsPacket80.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket81)

	eventsPacket82 := s.newHistoryEvents(
		[]int64{247, 248, 249, 250},
		82,
		eventsPacket81.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket82)

	eventsPacket83 := s.newHistoryEvents(
		[]int64{251, 252, 253},
		83,
		eventsPacket82.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket83)

	eventsPacket84 := s.newHistoryEvents(
		[]int64{254},
		84,
		eventsPacket83.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket84)

	eventsPacket85 := s.newHistoryEvents(
		[]int64{255, 256},
		85,
		eventsPacket84.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket85)

	eventsPacket86 := s.newHistoryEvents(
		[]int64{257, 258, 259},
		86,
		eventsPacket85.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket86)

	eventsPacket87 := s.newHistoryEvents(
		[]int64{260},
		87,
		eventsPacket86.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket87)

	eventsPacket88 := s.newHistoryEvents(
		[]int64{261, 262},
		88,
		eventsPacket87.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket88)

	eventsPacket89 := s.newHistoryEvents(
		[]int64{263, 264, 265},
		89,
		eventsPacket88.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket89)

	eventsPacket90 := s.newHistoryEvents(
		[]int64{266},
		90,
		eventsPacket89.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket90)

	eventsPacket91 := s.newHistoryEvents(
		[]int64{267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294},
		91,
		eventsPacket90.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket91)

	eventsPacket92 := s.newHistoryEvents(
		[]int64{295, 296},
		92,
		eventsPacket91.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket92)

	eventsPacket93 := s.newHistoryEvents(
		[]int64{297},
		93,
		eventsPacket92.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket93)

	eventsPacket94 := s.newHistoryEvents(
		[]int64{298},
		94,
		eventsPacket93.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket94)

	eventsPacket95 := s.newHistoryEvents(
		[]int64{299, 300, 301},
		95,
		eventsPacket94.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket95)

	eventsPacket96 := s.newHistoryEvents(
		[]int64{302},
		96,
		eventsPacket95.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket96)

	eventsPacket97 := s.newHistoryEvents(
		[]int64{303},
		97,
		eventsPacket96.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket97)

	eventsPacket98 := s.newHistoryEvents(
		[]int64{304, 305, 306},
		98,
		eventsPacket97.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket98)

	eventsPacket99 := s.newHistoryEvents(
		[]int64{307},
		99,
		eventsPacket98.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket99)

	eventsPacket100 := s.newHistoryEvents(
		[]int64{308},
		100,
		eventsPacket99.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket100)

	eventsPacket101 := s.newHistoryEvents(
		[]int64{308},
		101,
		eventsPacket100.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket101)

	eventsPacket102 := s.newHistoryEvents(
		[]int64{309, 310},
		102,
		eventsPacket101.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket102)

	prettyPrint := func(input interface{}) string {
		binary, _ := json.MarshalIndent(input, "", "  ")
		return string(binary)
	}

	events := s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 311)
	fmt.Println("#######")
	fmt.Printf("%v\n", prettyPrint(events))
	fmt.Println("#######")

	fmt.Println("@@@@@@@")
	fmt.Printf("%v\n", prettyPrint(eventsPacket100.events))
	fmt.Println("@@@@@@@")
	fmt.Printf("%v\n", prettyPrint(eventsPacket101.events))
	fmt.Println("@@@@@@@")
}

func (s *HistoryEventsSuite) appendHistoryEvents(
	shardID int32,
	branchToken []byte,
	packet HistoryEventsPacket,
) {
	_, err := s.store.AppendHistoryNodes(s.Ctx, &p.AppendHistoryNodesRequest{
		ShardID:           shardID,
		BranchToken:       branchToken,
		Events:            packet.events,
		TransactionID:     packet.transactionID,
		PrevTransactionID: packet.prevTransactionID,
		IsNewBranch:       packet.nodeID == common.FirstEventID,
		Info:              "",
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) appendRawHistoryBatches(
	shardID int32,
	branchToken []byte,
	packet HistoryEventsPacket,
) {
	blob, err := s.serializer.SerializeEvents(packet.events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	_, err = s.store.AppendRawHistoryNodes(s.Ctx, &p.AppendRawHistoryNodesRequest{
		ShardID:           shardID,
		BranchToken:       branchToken,
		NodeID:            packet.nodeID,
		TransactionID:     packet.transactionID,
		PrevTransactionID: packet.prevTransactionID,
		IsNewBranch:       packet.nodeID == common.FirstEventID,
		Info:              "",
		History:           blob,
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) forkHistoryBranch(
	shardID int32,
	branchToken []byte,
	newNodeID int64,
) []byte {
	resp, err := s.store.ForkHistoryBranch(s.Ctx, &p.ForkHistoryBranchRequest{
		ShardID:         shardID,
		NamespaceID:     uuid.New(),
		ForkBranchToken: branchToken,
		ForkNodeID:      newNodeID,
		Info:            "",
	})
	s.NoError(err)
	return resp.NewBranchToken
}

func (s *HistoryEventsSuite) deleteHistoryBranch(
	shardID int32,
	branchToken []byte,
) {
	err := s.store.DeleteHistoryBranch(s.Ctx, &p.DeleteHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: branchToken,
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) trimHistoryBranch(
	shardID int32,
	branchToken []byte,
	nodeID int64,
	transactionID int64,
) {
	_, err := s.store.TrimHistoryBranch(s.Ctx, &p.TrimHistoryBranchRequest{
		ShardID:       shardID,
		BranchToken:   branchToken,
		NodeID:        nodeID,
		TransactionID: transactionID,
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) listHistoryEvents(
	shardID int32,
	branchToken []byte,
	startEventID int64,
	endEventID int64,
) []*historypb.HistoryEvent {
	var token []byte
	var events []*historypb.HistoryEvent
	for doContinue := true; doContinue; doContinue = len(token) > 0 {
		resp, err := s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
			ShardID:       shardID,
			BranchToken:   branchToken,
			MinEventID:    startEventID,
			MaxEventID:    endEventID,
			PageSize:      100, // use 1 here for better testing exp
			NextPageToken: token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		events = append(events, resp.HistoryEvents...)
	}
	return events
}

func (s *HistoryEventsSuite) listAllHistoryEvents(
	shardID int32,
	branchToken []byte,
) []*historypb.HistoryEvent {
	var token []byte
	var events []*historypb.HistoryEvent
	for doContinue := true; doContinue; doContinue = len(token) > 0 {
		resp, err := s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
			ShardID:       shardID,
			BranchToken:   branchToken,
			MinEventID:    common.FirstEventID,
			MaxEventID:    common.LastEventID,
			PageSize:      1, // use 1 here for better testing exp
			NextPageToken: token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		events = append(events, resp.HistoryEvents...)
	}
	return events
}

func (s *HistoryEventsSuite) newHistoryEvents(
	eventIDs []int64,
	transactionID int64,
	prevTransactionID int64,
) HistoryEventsPacket {

	events := make([]*historypb.HistoryEvent, len(eventIDs))
	for index, eventID := range eventIDs {
		events[index] = &historypb.HistoryEvent{
			EventId:   eventID,
			EventTime: timestamp.TimePtr(time.Unix(0, rand.Int63()).UTC()),
		}
	}

	return HistoryEventsPacket{
		nodeID:            eventIDs[0],
		transactionID:     transactionID,
		prevTransactionID: prevTransactionID,
		events:            events,
	}
}
