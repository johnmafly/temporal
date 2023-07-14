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
	"sync"

	"go.temporal.io/server/service/history/tasks"
)

type (
	taskRequestCompletionFn func(error)

	taskRequestTracker interface {
		track(map[tasks.Category][]tasks.Task) taskRequestCompletionFn
		minTaskKey(tasks.Category) (tasks.Key, bool)
		drain()
		clear()
	}

	taskRequestTrackerImpl struct {
		sync.Mutex

		// TODO: use a priority queue to track the min pending task key
		outstandingTaskKeys map[tasks.Category][]tasks.Key
		outstandingRequests map[int64]struct{}
		waitChannels        []chan<- struct{}

		nextRequestID int64
	}
)

func newTaskRequestTracker() *taskRequestTrackerImpl {
	return &taskRequestTrackerImpl{
		outstandingTaskKeys: make(map[tasks.Category][]tasks.Key),
		outstandingRequests: make(map[int64]struct{}),
		waitChannels:        make([]chan<- struct{}, 0),
		nextRequestID:       0,
	}
}

func (t *taskRequestTrackerImpl) track(
	insertTasks map[tasks.Category][]tasks.Task,
) taskRequestCompletionFn {
	t.Lock()
	defer t.Unlock()

	requestID := t.nextRequestID
	t.nextRequestID++
	t.outstandingRequests[requestID] = struct{}{}

	minKeyByCategory := make(map[tasks.Category]tasks.Key)
	for category, tasksPerCategory := range insertTasks {
		if len(tasksPerCategory) == 0 {
			continue
		}
		minKey := minKeyTask(tasksPerCategory)
		t.outstandingTaskKeys[category] = append(t.outstandingTaskKeys[category], minKey)
		minKeyByCategory[category] = minKey
	}

	return func(writeErr error) {
		t.Lock()
		defer t.Unlock()

		if writeErr == nil || !OperationPossiblySucceeded(writeErr) {
			// we can only remove the task from the pending task list if we are sure it was inserted
			for category, minKey := range minKeyByCategory {
				pendingTasksForCategory := t.outstandingTaskKeys[category]
				for i := range pendingTasksForCategory {
					if pendingTasksForCategory[i].CompareTo(minKey) == 0 {
						pendingTasksForCategory = append(pendingTasksForCategory[:i], pendingTasksForCategory[i+1:]...)
						break
					}
				}
			}
		}

		// always mark the request as completed, otherwise rangeID renew will be blocked forever
		delete(t.outstandingRequests, requestID)
		if len(t.outstandingRequests) == 0 {
			t.closeWaitChannelsLocked()
		}
	}
}

func (t *taskRequestTrackerImpl) minTaskKey(
	category tasks.Category,
) (tasks.Key, bool) {
	t.Lock()
	defer t.Unlock()

	pendingTasksForCategory := t.outstandingTaskKeys[category]
	if len(pendingTasksForCategory) == 0 {
		return tasks.Key{}, false
	}

	minKey := pendingTasksForCategory[0]
	for _, key := range pendingTasksForCategory {
		if key.CompareTo(minKey) < 0 {
			minKey = key
		}
	}

	return minKey, true
}

func (t *taskRequestTrackerImpl) drain() {
	t.Lock()

	if len(t.outstandingRequests) == 0 {
		t.Unlock()
		return
	}

	waitCh := make(chan struct{})
	t.waitChannels = append(t.waitChannels, waitCh)
	t.Unlock()

	<-waitCh
}

func (t *taskRequestTrackerImpl) clear() {
	t.Lock()
	defer t.Unlock()

	t.outstandingTaskKeys = make(map[tasks.Category][]tasks.Key)
	t.outstandingRequests = make(map[int64]struct{})
	t.closeWaitChannelsLocked()
}

func (t *taskRequestTrackerImpl) closeWaitChannelsLocked() {
	for _, waitCh := range t.waitChannels {
		close(waitCh)
	}
	t.waitChannels = nil
}

// minKeyTask returns the min key of the given tasks
func minKeyTask(t []tasks.Task) tasks.Key {
	minKey := tasks.MaximumKey
	for _, task := range t {
		if task.GetKey().CompareTo(minKey) < 0 {
			minKey = task.GetKey()
		}
	}
	return minKey
}
