package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDynamicWorkerPoolSurvivesTaskPanic(t *testing.T) {
	pool := newTestWorkerPool(t, workerPoolConfig{name: "panic-test", workers: 1, queueSize: 2})

	if err := pool.Submit(context.Background(), func() { panic("unexpected provider panic") }); err != nil {
		t.Fatalf("submit panic task failed: %v", err)
	}
	waitForPoolStat(t, pool, func(stats workerPoolStats) bool {
		return stats.CompletedTotal == 1
	})

	ran := make(chan struct{})
	if err := pool.Submit(context.Background(), func() { close(ran) }); err != nil {
		t.Fatalf("submit follow-up task failed: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("worker pool did not run follow-up task after panic")
	}
	waitForPoolStat(t, pool, func(stats workerPoolStats) bool {
		return stats.CompletedTotal == 2
	})
}

func TestDynamicWorkerPoolRejectsSubmitAfterClose(t *testing.T) {
	pool := newTestWorkerPool(t, workerPoolConfig{name: "closed-test", workers: 1, queueSize: 1})
	if err := pool.Close(); err != nil {
		t.Fatalf("close pool failed: %v", err)
	}

	err := pool.Submit(context.Background(), func() {})
	if !errors.Is(err, errWorkerPoolClosed) {
		t.Fatalf("submit after close = %v, want %v", err, errWorkerPoolClosed)
	}
	if stats := pool.Stats(); !stats.Closed || stats.RejectedTotal != 1 {
		t.Fatalf("unexpected closed stats: %#v", stats)
	}
}

func TestDynamicWorkerPoolCloseStopsAfterRunningTaskReturns(t *testing.T) {
	pool := newTestWorkerPool(t, workerPoolConfig{name: "close-test", workers: 1, queueSize: 1})
	started := make(chan struct{})
	release := make(chan struct{})
	closed := make(chan error, 1)

	if err := pool.Submit(context.Background(), func() {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("submit blocking task failed: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking task did not start")
	}
	if err := pool.Submit(context.Background(), func() {}); err != nil {
		t.Fatalf("submit queued task failed: %v", err)
	}

	go func() {
		closed <- pool.Close()
	}()
	select {
	case err := <-closed:
		t.Fatalf("close returned before running task released: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not return after running task released")
	}
	if stats := pool.Stats(); !stats.Closed {
		t.Fatalf("pool did not report closed: %#v", stats)
	}
}

func TestDynamicWorkerPoolSubmitHonorsCanceledContextWhenQueueFull(t *testing.T) {
	pool := newTestWorkerPool(t, workerPoolConfig{name: "cancel-test", workers: 1, queueSize: 1})
	started := make(chan struct{})
	release := make(chan struct{})
	var queuedRan atomic.Bool

	if err := pool.Submit(context.Background(), func() {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("submit blocking task failed: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking task did not start")
	}
	if err := pool.Submit(context.Background(), func() { queuedRan.Store(true) }); err != nil {
		t.Fatalf("submit queued task failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pool.Submit(ctx, func() {})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("submit with canceled context = %v, want context.Canceled", err)
	}
	if stats := pool.Stats(); stats.RejectedTotal != 1 {
		t.Fatalf("rejected total = %d, want 1", stats.RejectedTotal)
	}

	close(release)
	waitForPoolStat(t, pool, func(stats workerPoolStats) bool {
		return stats.CompletedTotal == 2
	})
	if !queuedRan.Load() {
		t.Fatal("queued task did not run after blocking task released")
	}
}

func newTestWorkerPool(t *testing.T, cfg workerPoolConfig) *dynamicWorkerPool {
	t.Helper()

	pool, err := newDynamicWorkerPool(cfg)
	if err != nil {
		t.Fatalf("new worker pool failed: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})
	return pool
}

func waitForPoolStat(t *testing.T, pool *dynamicWorkerPool, match func(workerPoolStats) bool) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for worker pool stats")
		case <-ticker.C:
			if match(pool.Stats()) {
				return
			}
		}
	}
}
