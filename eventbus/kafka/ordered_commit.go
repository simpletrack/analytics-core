package kafka

import (
	"fmt"
	"sync"
)

// commitState stores completion and mark state for one fetched Kafka offset.
type commitState struct {
	done         bool   // done records handler, retry, or DLQ completion for this offset
	generationID int32  // generationID is the Sarama generation observed at registration time
	markFn       func() // markFn marks the Sarama session only after ordered completion advances
}

// partitionOrderedCommitter completes one Kafka topic-partition in offset order.
type partitionOrderedCommitter struct {
	mu                   sync.Mutex             // mu protects all mutable commit state
	initialized          bool                   // initialized reports whether nextOffset has been seeded
	generationID         int32                  // generationID is the active Sarama generation for this partition
	nextOffset           int64                  // nextOffset is the earliest offset that may still block commit progress
	states               map[int64]*commitState // states stores offsets registered but not yet marked to Sarama
	doneCount            int                    // doneCount counts completed offsets waiting behind nextOffset
	lastRegisteredOffset int64                  // lastRegisteredOffset tracks observed fetch order for diagnostics
	largestRegisterGap   int64                  // largestRegisterGap records the largest offset gap seen at registration
}

// newPartitionOrderedCommitter creates an empty ordered committer.
func newPartitionOrderedCommitter() *partitionOrderedCommitter {
	return &partitionOrderedCommitter{states: make(map[int64]*commitState)}
}

// Register tracks one fetched offset before it enters handler execution.
func (c *partitionOrderedCommitter) Register(offset int64, generationID int32, markFn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.initialized && c.generationID != generationID {
		// A Sarama rebalance invalidates old session marks. Drop any unfinished
		// offsets from the previous generation so late workers cannot advance the
		// newly assigned session with stale callbacks.
		c.resetLocked()
	}

	// The first observed offset becomes the lower bound for contiguous
	// completion. Later offsets can finish early but cannot mark Kafka progress
	// until this pointer reaches them.
	if !c.initialized {
		c.initialized = true
		c.generationID = generationID
		c.nextOffset = offset
		c.lastRegisteredOffset = offset
	}
	if _, ok := c.states[offset]; ok {
		return
	}
	if len(c.states) > 0 && offset > c.lastRegisteredOffset {
		if gap := offset - c.lastRegisteredOffset; gap > c.largestRegisterGap {
			c.largestRegisterGap = gap
		}
		c.lastRegisteredOffset = offset
	}
	c.states[offset] = &commitState{generationID: generationID, markFn: markFn}
}

// Complete marks one offset complete and flushes any now-contiguous range.
func (c *partitionOrderedCommitter) Complete(offset int64, generationID int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state, ok := c.states[offset]
	if !ok {
		return
	}
	if state.generationID != generationID || c.generationID != generationID {
		return
	}
	if !state.done {
		state.done = true
		c.doneCount++
	}

	// Advance only through the contiguous completed range. This mirrors the
	// xwl_bi partition commit guard: a later successful offset never skips an
	// earlier unfinished offset.
	for {
		current, ok := c.states[c.nextOffset]
		if !ok || !current.done {
			break
		}
		if current.markFn != nil {
			current.markFn()
		}
		delete(c.states, c.nextOffset)
		c.doneCount--
		c.nextOffset++
	}
	if len(c.states) == 0 {
		c.resetLocked()
	}
}

// resetLocked clears state after all offsets are marked or a generation changes.
func (c *partitionOrderedCommitter) resetLocked() {
	c.initialized = false
	c.generationID = 0
	c.nextOffset = 0
	c.states = make(map[int64]*commitState)
	c.doneCount = 0
	c.lastRegisteredOffset = 0
	c.largestRegisterGap = 0
}

// orderedCommitManager owns ordered committers keyed by topic and partition.
type orderedCommitManager struct {
	committers sync.Map // committers stores *partitionOrderedCommitter values keyed by topic:partition
}

// newOrderedCommitManager creates the provider-wide committer registry.
func newOrderedCommitManager() *orderedCommitManager {
	return &orderedCommitManager{}
}

// Get returns the ordered committer for one topic-partition.
func (m *orderedCommitManager) Get(topic string, partition int32) *partitionOrderedCommitter {
	key := fmt.Sprintf("%s:%d", topic, partition)
	if committer, ok := m.committers.Load(key); ok {
		return committer.(*partitionOrderedCommitter)
	}
	created := newPartitionOrderedCommitter()
	actual, _ := m.committers.LoadOrStore(key, created)
	return actual.(*partitionOrderedCommitter)
}

// Snapshots returns a diagnostic copy of every tracked topic-partition.
func (m *orderedCommitManager) Snapshots() []orderedCommitSnapshot {
	snapshots := make([]orderedCommitSnapshot, 0)
	m.committers.Range(func(key, value any) bool {
		snapshot := value.(*partitionOrderedCommitter).snapshot()
		snapshot.Key = key.(string)
		snapshots = append(snapshots, snapshot)
		return true
	})
	return snapshots
}

// orderedCommitSnapshot reports pending completion state for protector decisions.
type orderedCommitSnapshot struct {
	Key                 string // Key is topic:partition
	Initialized         bool   // Initialized reports whether this partition has seen any offset
	NextOffset          int64  // NextOffset is the earliest offset still blocking ordered completion
	PendingCount        int    // PendingCount is the number of registered unmarked offsets
	DoneCount           int    // DoneCount is the number of completed offsets waiting for earlier offsets
	OldestPendingOffset int64  // OldestPendingOffset is the same value as NextOffset when pending exists
	LargestPendingGap   int64  // LargestPendingGap is the largest observed registration gap
}

// snapshot returns a diagnostic copy without exposing mutable commit state.
func (c *partitionOrderedCommitter) snapshot() orderedCommitSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldestPendingOffset := int64(0)
	if len(c.states) > 0 {
		oldestPendingOffset = c.nextOffset
	}
	return orderedCommitSnapshot{
		Initialized:         c.initialized,
		NextOffset:          c.nextOffset,
		PendingCount:        len(c.states),
		DoneCount:           c.doneCount,
		OldestPendingOffset: oldestPendingOffset,
		LargestPendingGap:   c.largestRegisterGap,
	}
}
