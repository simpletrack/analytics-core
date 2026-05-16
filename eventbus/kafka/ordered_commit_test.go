package kafka

import "testing"

func TestPartitionOrderedCommitterSequentialSuccess(t *testing.T) {
	committer := newPartitionOrderedCommitter()
	calls := make([]int64, 0, 2)

	committer.Register(10, 1, func() { calls = append(calls, 10) })
	committer.Register(11, 1, func() { calls = append(calls, 11) })

	committer.Complete(11, 1)
	if len(calls) != 0 {
		t.Fatalf("committed later offset before earlier offset: %+v", calls)
	}

	committer.Complete(10, 1)
	if len(calls) != 2 || calls[0] != 10 || calls[1] != 11 {
		t.Fatalf("unexpected commit order: %+v", calls)
	}
}

func TestPartitionOrderedCommitterAdvancesAfterEarlierFailurePathCompletes(t *testing.T) {
	committer := newPartitionOrderedCommitter()
	calls := make([]int64, 0, 2)

	committer.Register(20, 1, func() { calls = append(calls, 20) })
	committer.Register(21, 1, func() { calls = append(calls, 21) })

	committer.Complete(20, 1)
	committer.Complete(21, 1)

	if len(calls) != 2 || calls[0] != 20 || calls[1] != 21 {
		t.Fatalf("unexpected commit order: %+v", calls)
	}
}

func TestPartitionOrderedCommitterSnapshotTracksDoneCount(t *testing.T) {
	committer := newPartitionOrderedCommitter()

	committer.Register(30, 1, func() {})
	committer.Register(31, 1, func() {})
	committer.Register(32, 1, func() {})

	committer.Complete(31, 1)
	snapshot := committer.snapshot()
	if snapshot.PendingCount != 3 {
		t.Fatalf("pending count = %d, want 3", snapshot.PendingCount)
	}
	if snapshot.DoneCount != 1 {
		t.Fatalf("done count = %d, want 1", snapshot.DoneCount)
	}

	committer.Complete(30, 1)
	snapshot = committer.snapshot()
	if snapshot.PendingCount != 1 {
		t.Fatalf("pending count after advance = %d, want 1", snapshot.PendingCount)
	}
	if snapshot.DoneCount != 0 {
		t.Fatalf("done count after advance = %d, want 0", snapshot.DoneCount)
	}
	if snapshot.NextOffset != 32 {
		t.Fatalf("next offset after advance = %d, want 32", snapshot.NextOffset)
	}
}

func TestPartitionOrderedCommitterIgnoresStaleGenerationCompletion(t *testing.T) {
	committer := newPartitionOrderedCommitter()
	calls := make([]int64, 0, 1)

	committer.Register(40, 1, func() { calls = append(calls, 40) })
	committer.Register(40, 2, func() { calls = append(calls, 400) })

	committer.Complete(40, 1)
	if len(calls) != 0 {
		t.Fatalf("committed stale generation: %+v", calls)
	}

	committer.Complete(40, 2)
	if len(calls) != 1 || calls[0] != 400 {
		t.Fatalf("unexpected generation-aware commit calls: %+v", calls)
	}
}
