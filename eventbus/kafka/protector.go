package kafka

import "sync"

const (
	// Default hard thresholds deliberately pause late. They are protection
	// rails for local overload, not steady-state flow-control targets.
	defaultHardPendingCount = 10000
	defaultHardGateCount    = 10000
	defaultHardQueueRatio   = 0.95
)

// pausableConsumerGroup is the Sarama pause/resume surface used by the protector.
type pausableConsumerGroup interface {
	// Pause stops fetching from the specified topic partitions.
	Pause(map[string][]int32)
	// Resume restarts fetching from the specified topic partitions.
	Resume(map[string][]int32)
}

// consumptionSnapshot carries the pressure signals used for pause/resume.
type consumptionSnapshot struct {
	Commits []orderedCommitSnapshot       // Commits are per-partition ordered-commit states
	Gate    messageCompletionGateSnapshot // Gate reports per-message async completion pressure
	Pool    workerPoolStats               // Pool reports bounded worker queue pressure
}

// consumptionProtector pauses Kafka partitions when local pressure is too high.
type consumptionProtector struct {
	mu             sync.Mutex         // mu protects paused partition state
	paused         map[string][]int32 // paused records partitions currently paused by this protector
	hardPending    int                // hardPending is the pending-offset threshold
	hardGate       int64              // hardGate is the gate/task threshold
	hardQueueRatio float64            // hardQueueRatio is the worker queue saturation threshold
}

// newConsumptionProtector creates the local pressure guard for Kafka consumers.
func newConsumptionProtector() *consumptionProtector {
	return &consumptionProtector{
		paused:         make(map[string][]int32),
		hardPending:    defaultHardPendingCount,
		hardGate:       defaultHardGateCount,
		hardQueueRatio: defaultHardQueueRatio,
	}
}

// observe applies pause or resume decisions from the latest pressure snapshot.
func (p *consumptionProtector) observe(group pausableConsumerGroup, snapshot consumptionSnapshot) {
	if p == nil || group == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	pressure := p.hasHardPressure(snapshot)
	if pressure && len(p.paused) == 0 {
		partitions := p.pausedPartitions(snapshot)
		if len(partitions) > 0 {
			p.paused = partitions
			group.Pause(copyPartitions(partitions))
		}
		return
	}
	if !pressure && len(p.paused) > 0 {
		group.Resume(copyPartitions(p.paused))
		p.paused = make(map[string][]int32)
	}
}

// restore reapplies pause state after a Sarama rebalance.
func (p *consumptionProtector) restore(group pausableConsumerGroup) {
	if p == nil || group == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.paused) > 0 {
		group.Pause(copyPartitions(p.paused))
	}
}

// hasHardPressure reports whether any tracked signal exceeds a hard threshold.
func (p *consumptionProtector) hasHardPressure(snapshot consumptionSnapshot) bool {
	if snapshot.Gate.InFlightMessages >= p.hardGate || snapshot.Gate.WaitingTasks >= p.hardGate {
		return true
	}
	if snapshot.Pool.QueueUsageRatio >= p.hardQueueRatio {
		return true
	}
	for _, commit := range snapshot.Commits {
		if commit.PendingCount >= p.hardPending {
			return true
		}
	}
	return false
}

// pausedPartitions returns all partitions currently represented by commit state.
func (p *consumptionProtector) pausedPartitions(snapshot consumptionSnapshot) map[string][]int32 {
	partitions := make(map[string][]int32)
	for _, commit := range snapshot.Commits {
		topic, partition, ok := splitCommitKey(commit.Key)
		if !ok {
			continue
		}
		partitions[topic] = append(partitions[topic], partition)
	}
	return partitions
}

// copyPartitions clones Sarama pause/resume maps before handing them to callers.
func copyPartitions(partitions map[string][]int32) map[string][]int32 {
	out := make(map[string][]int32, len(partitions))
	for topic, values := range partitions {
		out[topic] = append([]int32(nil), values...)
	}
	return out
}

// splitCommitKey parses the topic:partition key used by orderedCommitManager.
func splitCommitKey(key string) (string, int32, bool) {
	for idx := len(key) - 1; idx >= 0; idx-- {
		if key[idx] != ':' {
			continue
		}
		topic := key[:idx]
		var partition int32
		for _, r := range key[idx+1:] {
			if r < '0' || r > '9' {
				return "", 0, false
			}
			partition = partition*10 + int32(r-'0')
		}
		return topic, partition, topic != ""
	}
	return "", 0, false
}
