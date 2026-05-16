package kafka

import (
	"sync"
	"sync/atomic"
)

// messageCompletionGate completes one Kafka offset after its own async work ends.
type messageCompletionGate struct {
	offset         int64
	generationID   int32
	committer      *partitionOrderedCommitter
	tracker        *messageCompletionGateTracker
	remainingTasks int32
	completedOnce  sync.Once
}

func newMessageCompletionGate(offset int64, generationID int32, committer *partitionOrderedCommitter, tracker *messageCompletionGateTracker) *messageCompletionGate {
	gate := &messageCompletionGate{offset: offset, generationID: generationID, committer: committer, tracker: tracker}
	if tracker != nil {
		tracker.TrackGateCreated()
	}
	return gate
}

// AddTask records one async task that must finish before the message completes.
func (g *messageCompletionGate) AddTask() {
	if g == nil {
		return
	}
	atomic.AddInt32(&g.remainingTasks, 1)
	if g.tracker != nil {
		g.tracker.TrackTaskAdded()
	}
}

// TaskDone records one async task completion and completes the message at zero.
func (g *messageCompletionGate) TaskDone() {
	if g == nil {
		return
	}
	remaining := atomic.AddInt32(&g.remainingTasks, -1)
	if g.tracker != nil {
		g.tracker.TrackTaskDone()
	}
	if remaining == 0 {
		g.complete()
	}
}

// NoAsyncTaskCompleteNow completes messages that registered no async tasks.
func (g *messageCompletionGate) NoAsyncTaskCompleteNow() {
	if g == nil {
		return
	}
	if atomic.LoadInt32(&g.remainingTasks) == 0 {
		g.complete()
	}
}

func (g *messageCompletionGate) complete() {
	// Completion is idempotent because handler success, malformed-message DLQ,
	// and shutdown paths can race around the same offset.
	g.completedOnce.Do(func() {
		if g.tracker != nil {
			g.tracker.TrackCompleted()
		}
		g.committer.Complete(g.offset, g.generationID)
	})
}

// messageCompletionGateTracker reports aggregate completion-gate pressure.
type messageCompletionGateTracker struct {
	inFlightMessages  int64 // inFlightMessages is the current number of open gates
	waitingTasks      int64 // waitingTasks is the current number of unfinished async tasks
	completedMessages int64 // completedMessages is the lifetime completed gate count
}

// messageCompletionGateSnapshot reports per-message async completion pressure.
type messageCompletionGateSnapshot struct {
	InFlightMessages  int64 // InFlightMessages is the number of messages not yet completed
	WaitingTasks      int64 // WaitingTasks is the number of outstanding async tasks
	CompletedMessages int64 // CompletedMessages is the lifetime count of completed messages
}

// TrackGateCreated records a message that has entered the provider pipeline.
func (t *messageCompletionGateTracker) TrackGateCreated() {
	atomic.AddInt64(&t.inFlightMessages, 1)
}

// TrackTaskAdded records async work owned by one message.
func (t *messageCompletionGateTracker) TrackTaskAdded() {
	atomic.AddInt64(&t.waitingTasks, 1)
}

// TrackTaskDone records completion of async work owned by one message.
func (t *messageCompletionGateTracker) TrackTaskDone() {
	atomic.AddInt64(&t.waitingTasks, -1)
}

// TrackCompleted records that a message has become eligible for ordered commit.
func (t *messageCompletionGateTracker) TrackCompleted() {
	atomic.AddInt64(&t.inFlightMessages, -1)
	atomic.AddInt64(&t.completedMessages, 1)
}

// Snapshot returns a lock-free diagnostic view of in-flight message gates.
func (t *messageCompletionGateTracker) Snapshot() messageCompletionGateSnapshot {
	if t == nil {
		return messageCompletionGateSnapshot{}
	}
	return messageCompletionGateSnapshot{
		InFlightMessages:  atomic.LoadInt64(&t.inFlightMessages),
		WaitingTasks:      atomic.LoadInt64(&t.waitingTasks),
		CompletedMessages: atomic.LoadInt64(&t.completedMessages),
	}
}
