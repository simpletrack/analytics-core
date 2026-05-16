package kafka

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

var errWorkerPoolClosed = errors.New("kafka worker pool is closed")

// workerPoolConfig configures the bounded handler execution pool.
type workerPoolConfig struct {
	name      string // name identifies the worker pool in diagnostics
	workers   int    // workers is the fixed goroutine count
	queueSize int    // queueSize is the bounded pending task capacity
}

// workerPoolStats reports fixed worker-pool pressure for protector decisions.
type workerPoolStats struct {
	Name            string  // Name identifies this pool in diagnostics
	GoroutinesTotal int     // GoroutinesTotal is runtime.NumGoroutine at sampling time
	Queued          int64   // Queued is the current number of queued tasks
	QueueCapacity   int     // QueueCapacity is the channel capacity
	QueueUsageRatio float64 // QueueUsageRatio is Queued divided by QueueCapacity
	Workers         int     // Workers is the fixed worker count
	SubmittedTotal  int64   // SubmittedTotal is the lifetime accepted task count
	CompletedTotal  int64   // CompletedTotal is the lifetime completed task count
	RejectedTotal   int64   // RejectedTotal is the lifetime rejected task count
	Closed          bool    // Closed reports whether Close has started
}

type dynamicWorkerPool struct {
	name           string         // name identifies this pool in diagnostics
	tasks          chan func()    // tasks is the bounded handler work queue
	done           chan struct{}  // done closes to request worker shutdown
	wg             sync.WaitGroup // wg waits for worker goroutines to stop
	queued         int64          // queued tracks tasks accepted into or waiting for the queue
	submittedTotal int64          // submittedTotal counts tasks accepted by Submit
	completedTotal int64          // completedTotal counts tasks that ran to completion or panic recovery
	rejectedTotal  int64          // rejectedTotal counts submissions rejected by context or shutdown
	closed         uint32         // closed is set atomically before done closes
	workers        int            // workers is the fixed goroutine count
}

// newDynamicWorkerPool creates a fixed-size pool with dynamic-style diagnostics.
func newDynamicWorkerPool(cfg workerPoolConfig) (*dynamicWorkerPool, error) {
	// Keep the first version deliberately fixed-size. xwl_bi has a dynamically
	// tunable pool, but analytics-core only needs bounded concurrency and
	// pressure stats until real Kafka benchmark data justifies resizing.
	if cfg.workers <= 0 {
		cfg.workers = defaultWorkers
	}
	if cfg.queueSize <= 0 {
		cfg.queueSize = cfg.workers * 2
	}
	pool := &dynamicWorkerPool{
		name:    cfg.name,
		tasks:   make(chan func(), cfg.queueSize),
		done:    make(chan struct{}),
		workers: cfg.workers,
	}
	for idx := 0; idx < cfg.workers; idx++ {
		pool.wg.Add(1)
		go pool.run()
	}
	return pool, nil
}

// Submit enqueues one handler task or returns when ctx/shutdown wins first.
func (p *dynamicWorkerPool) Submit(ctx context.Context, task func()) error {
	if p == nil || task == nil {
		return nil
	}
	if atomic.LoadUint32(&p.closed) == 1 {
		atomic.AddInt64(&p.rejectedTotal, 1)
		return errWorkerPoolClosed
	}

	atomic.AddInt64(&p.queued, 1)
	select {
	case p.tasks <- task:
		atomic.AddInt64(&p.submittedTotal, 1)
		return nil
	case <-ctx.Done():
		atomic.AddInt64(&p.queued, -1)
		atomic.AddInt64(&p.rejectedTotal, 1)
		return ctx.Err()
	case <-p.done:
		atomic.AddInt64(&p.queued, -1)
		atomic.AddInt64(&p.rejectedTotal, 1)
		return errWorkerPoolClosed
	}
}

// Close stops workers and rejects future submissions.
func (p *dynamicWorkerPool) Close() error {
	if p == nil || !atomic.CompareAndSwapUint32(&p.closed, 0, 1) {
		return nil
	}
	// Close is a fast shutdown boundary. Already running tasks finish or return
	// from their cancelled context, while queued tasks are not guaranteed to run.
	// Kafka offsets for abandoned tasks remain uncompleted and replayable.
	close(p.done)
	p.wg.Wait()
	return nil
}

// Stats returns a diagnostic snapshot used by the consumption protector.
func (p *dynamicWorkerPool) Stats() workerPoolStats {
	if p == nil {
		return workerPoolStats{}
	}
	queued := atomic.LoadInt64(&p.queued)
	capacity := cap(p.tasks)
	queueUsageRatio := float64(0)
	if capacity > 0 {
		queueUsageRatio = float64(queued) / float64(capacity)
	}
	return workerPoolStats{
		Name:            p.name,
		GoroutinesTotal: runtime.NumGoroutine(),
		Queued:          queued,
		QueueCapacity:   capacity,
		QueueUsageRatio: queueUsageRatio,
		Workers:         p.workers,
		SubmittedTotal:  atomic.LoadInt64(&p.submittedTotal),
		CompletedTotal:  atomic.LoadInt64(&p.completedTotal),
		RejectedTotal:   atomic.LoadInt64(&p.rejectedTotal),
		Closed:          atomic.LoadUint32(&p.closed) == 1,
	}
}

// run executes queued tasks until Close requests shutdown.
func (p *dynamicWorkerPool) run() {
	defer p.wg.Done()
	for {
		select {
		case task := <-p.tasks:
			atomic.AddInt64(&p.queued, -1)
			func() {
				// Recover here only as a last-resort pool guard. Handler panics are
				// converted to retryable errors before they reach the pool; a panic
				// here means provider glue failed unexpectedly, so the offset remains
				// uncompleted and replayable while the shared worker capacity survives.
				defer func() {
					_ = recover()
					atomic.AddInt64(&p.completedTotal, 1)
				}()
				task()
			}()
		case <-p.done:
			return
		}
	}
}
