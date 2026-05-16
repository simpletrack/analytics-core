package kafka

import "testing"

func TestMessageCompletionGateCompletesAfterAllTasksDone(t *testing.T) {
	committer := newPartitionOrderedCommitter()
	calls := 0
	committer.Register(10, 1, func() { calls++ })

	gate := newMessageCompletionGate(10, 1, committer, &messageCompletionGateTracker{})
	gate.AddTask()
	gate.AddTask()
	gate.NoAsyncTaskCompleteNow()
	if calls != 0 {
		t.Fatalf("completed before tasks finished: %d", calls)
	}

	gate.TaskDone()
	if calls != 0 {
		t.Fatalf("completed after first task: %d", calls)
	}

	gate.TaskDone()
	if calls != 1 {
		t.Fatalf("completed after all tasks: %d", calls)
	}
}

func TestMessageCompletionGateCompletesImmediatelyWithoutAsyncTasks(t *testing.T) {
	committer := newPartitionOrderedCommitter()
	calls := 0
	committer.Register(20, 1, func() { calls++ })

	gate := newMessageCompletionGate(20, 1, committer, &messageCompletionGateTracker{})
	gate.NoAsyncTaskCompleteNow()
	if calls != 1 {
		t.Fatalf("completed without tasks = %d, want 1", calls)
	}
}
