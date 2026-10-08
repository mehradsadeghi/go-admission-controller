package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// Helpers
//
// None of the tests below sleep or rely on wall-clock timing. Synchronisation
// is done with channels, atomics and "started" signals only. A test that would
// hang on a regression is bounded by `go test -timeout`.
//
// Run with:  go test -race -count=1 ./...
// -----------------------------------------------------------------------------

const (
	blockerIDBase = 1_000_000
	stoppedMsg    = "admission controller already stopped"
)

func okProcess(_ context.Context, id int, p Priority) (Result, error) {
	return Result{ID: id, Data: fmt.Sprintf("done-%d-%s", id, p)}, nil
}

// newController creates a controller whose lifetime is bound to the test.
func newController(t *testing.T) (*AdmissionController, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ac := NewAdmissionController(ctx)
	t.Cleanup(func() {
		cancel()
		ac.Shutdown()
	})
	return ac, ctx
}

// saturation holds all workers inside blocking requests so that tests can
// build up queue state deterministically. Each blocker has its own gate, so a
// test can free exactly one worker (which then drains the queues alone and
// therefore in a fully deterministic order).
type saturation struct {
	gates []chan struct{}
	once  []sync.Once
}

func (s *saturation) release(i int) {
	s.once[i].Do(func() { close(s.gates[i]) })
}

func (s *saturation) releaseAll() {
	for i := range s.gates {
		s.release(i)
	}
}

// saturate occupies every worker. Must be called right after newController so
// that its cleanup (release) runs before the controller's cleanup (shutdown).
func saturate(t *testing.T, ac *AdmissionController, ctx context.Context) *saturation {
	t.Helper()

	s := &saturation{
		gates: make([]chan struct{}, WorkersCount),
		once:  make([]sync.Once, WorkersCount),
	}
	for i := range s.gates {
		s.gates[i] = make(chan struct{})
	}
	t.Cleanup(s.releaseAll)

	started := make(chan struct{}, WorkersCount)
	for i := 0; i < WorkersCount; i++ {
		gate := s.gates[i]
		err := ac.Submit(Request{
			ID:       blockerIDBase + i,
			Priority: HighPriority,
			Context:  ctx,
			Process: func(_ context.Context, id int, _ Priority) (Result, error) {
				started <- struct{}{}
				<-gate
				return Result{ID: id}, nil
			},
		})
		if err != nil {
			t.Fatalf("submitting blocker %d: %v", i, err)
		}
	}
	for i := 0; i < WorkersCount; i++ {
		<-started
	}

	// Every worker is now busy inside Process, so nobody touches the fairness
	// counters until a blocker is released. Start from a clean slate.
	ac.fairnessMu.Lock()
	ac.highQueueFairCount = 0
	ac.mediumQueueFairCount = 0
	ac.lowQueueFairCount = 0
	ac.fairnessMu.Unlock()

	return s
}

// recvResults receives n non-blocker results.
func recvResults(t *testing.T, ac *AdmissionController, n int) []Result {
	t.Helper()
	out := make([]Result, 0, n)
	for len(out) < n {
		r, ok := <-ac.Results()
		if !ok {
			t.Fatalf("results channel closed after %d of %d results", len(out), n)
		}
		if r.ID >= blockerIDBase {
			continue
		}
		out = append(out, r)
	}
	return out
}

func recvError(t *testing.T, ac *AdmissionController) error {
	t.Helper()
	err, ok := <-ac.Errors()
	if !ok {
		t.Fatal("errors channel closed before an error was delivered")
	}
	return err
}

func queueFor(a *AdmissionController, p Priority) chan Request {
	switch p {
	case HighPriority:
		return a.highPriorityQueue
	case MediumPriority:
		return a.mediumPriorityQueue
	default:
		return a.lowPriorityQueue
	}
}

func queueName(a *AdmissionController, q <-chan Request) string {
	switch q {
	case (<-chan Request)(a.highPriorityQueue):
		return "high"
	case (<-chan Request)(a.mediumPriorityQueue):
		return "medium"
	case (<-chan Request)(a.lowPriorityQueue):
		return "low"
	default:
		return "unknown"
	}
}

func isShutdownErr(err error) bool {
	return err != nil && (err.Error() == stoppedMsg || errors.Is(err, context.Canceled))
}

// startOrder records the order in which requests begin executing.
type startOrder struct {
	mu    sync.Mutex
	ids   []int
	prios []Priority
}

func (o *startOrder) process(_ context.Context, id int, p Priority) (Result, error) {
	o.mu.Lock()
	o.ids = append(o.ids, id)
	o.prios = append(o.prios, p)
	o.mu.Unlock()
	return Result{ID: id}, nil
}

func (o *startOrder) snapshot() ([]int, []Priority) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int(nil), o.ids...), append([]Priority(nil), o.prios...)
}

// waitForGoroutines spins (yielding the scheduler, no timers) until the
// goroutine count drops to the baseline, or fails with a full stack dump.
func waitForGoroutines(t *testing.T, baseline int) {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		runtime.Gosched()
	}
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	t.Fatalf("goroutine leak: %d goroutines, baseline %d\n%s", runtime.NumGoroutine(), baseline, buf[:n])
}

// -----------------------------------------------------------------------------
// Normal execution
// -----------------------------------------------------------------------------

func TestNormalExecution(t *testing.T) {
	t.Run("idle controller runs a high priority request", func(t *testing.T) {
		ac, ctx := newController(t)

		err := ac.Submit(Request{ID: 7, Priority: HighPriority, Context: ctx, Process: okProcess})
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}

		got := recvResults(t, ac, 1)[0]
		want := Result{ID: 7, Data: "done-7-High"}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	for _, p := range []Priority{HighPriority, MediumPriority, LowPriority} {
		t.Run(p.String()+" priority request is executed", func(t *testing.T) {
			ac, ctx := newController(t)
			sat := saturate(t, ac, ctx)

			if err := ac.Submit(Request{ID: 11, Priority: p, Context: ctx, Process: okProcess}); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			sat.release(0)

			got := recvResults(t, ac, 1)[0]
			want := Result{ID: 11, Data: fmt.Sprintf("done-11-%s", p)}
			if got != want {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		})
	}

	t.Run("unknown priority is rejected", func(t *testing.T) {
		ac, ctx := newController(t)
		err := ac.Submit(Request{ID: 1, Priority: Priority(99), Context: ctx, Process: okProcess})
		if err == nil || err.Error() != "unknown priority" {
			t.Fatalf("got %v, want unknown priority error", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Concurrent submissions
// -----------------------------------------------------------------------------

func TestConcurrentSubmissions(t *testing.T) {
	const (
		submitters   = 16
		perSubmitter = 50
		total        = submitters * perSubmitter // fits in the high queue buffer
	)

	ac, ctx := newController(t)

	start := make(chan struct{})
	errCh := make(chan error, total)
	var wg sync.WaitGroup

	for g := 0; g < submitters; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perSubmitter; i++ {
				id := g*perSubmitter + i
				if err := ac.Submit(Request{ID: id, Priority: HighPriority, Context: ctx, Process: okProcess}); err != nil {
					errCh <- err
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("unexpected submit error: %v", err)
	}

	results := recvResults(t, ac, total)
	seen := make(map[int]bool, total)
	for _, r := range results {
		if seen[r.ID] {
			t.Errorf("result %d delivered twice", r.ID)
		}
		seen[r.ID] = true
	}
	for id := 0; id < total; id++ {
		if !seen[id] {
			t.Errorf("result %d missing", id)
		}
	}
}

// -----------------------------------------------------------------------------
// Strict concurrency limit
// -----------------------------------------------------------------------------

func TestStrictConcurrencyLimit(t *testing.T) {
	const total = WorkersCount * 3

	ac, ctx := newController(t)

	var running, maxRunning atomic.Int32
	started := make(chan struct{}, total)
	tokens := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(done) }) }) // runs before controller cleanup

	proc := func(_ context.Context, id int, _ Priority) (Result, error) {
		n := running.Add(1)
		for {
			m := maxRunning.Load()
			if n <= m || maxRunning.CompareAndSwap(m, n) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-tokens:
		case <-done:
		}
		running.Add(-1)
		return Result{ID: id}, nil
	}

	for i := 0; i < total; i++ {
		if err := ac.Submit(Request{ID: i, Priority: HighPriority, Context: ctx, Process: proc}); err != nil {
			t.Fatalf("Submit(%d): %v", i, err)
		}
	}

	for i := 0; i < WorkersCount; i++ {
		<-started
	}
	if got := int(running.Load()); got != WorkersCount {
		t.Fatalf("running = %d, want exactly %d", got, WorkersCount)
	}
	if got, want := len(ac.highPriorityQueue), total-WorkersCount; got != want {
		t.Fatalf("queued = %d, want %d (no more than %d requests may be in flight)", got, want, WorkersCount)
	}

	// Let requests through one at a time; each completion admits exactly one more.
	for i := 0; i < total-WorkersCount; i++ {
		tokens <- struct{}{}
		<-started
	}
	for i := 0; i < WorkersCount; i++ {
		tokens <- struct{}{}
	}

	recvResults(t, ac, total)

	if got := int(maxRunning.Load()); got != WorkersCount {
		t.Fatalf("max concurrent executions = %d, want %d", got, WorkersCount)
	}
}

// -----------------------------------------------------------------------------
// Bounded queues / queue full
// -----------------------------------------------------------------------------

func TestBoundedQueuesRejectWhenFull(t *testing.T) {
	cases := []struct {
		priority Priority
		capacity int
		message  string
	}{
		{HighPriority, HighQueueBufferSize, "high priority queue is full"},
		{MediumPriority, MediumQueueBufferSize, "medium priority queue is full"},
		{LowPriority, LowQueueBufferSize, "low priority queue is full"},
	}

	for _, tc := range cases {
		t.Run(tc.priority.String(), func(t *testing.T) {
			ac, ctx := newController(t)
			sat := saturate(t, ac, ctx)

			var executed atomic.Int64
			counting := func(c context.Context, id int, p Priority) (Result, error) {
				executed.Add(1)
				return okProcess(c, id, p)
			}

			for i := 0; i < tc.capacity; i++ {
				err := ac.Submit(Request{ID: i, Priority: tc.priority, Context: ctx, Process: counting})
				if err != nil {
					t.Fatalf("Submit(%d) within capacity: %v", i, err)
				}
			}
			q := queueFor(ac, tc.priority)
			if len(q) != cap(q) || len(q) != tc.capacity {
				t.Fatalf("queue len=%d cap=%d, want both %d", len(q), cap(q), tc.capacity)
			}

			// Overflow: rejected, immediately, and the queue does not grow.
			for i := 0; i < 3; i++ {
				err := ac.Submit(Request{ID: tc.capacity + i, Priority: tc.priority, Context: ctx, Process: counting})
				if err == nil || err.Error() != tc.message {
					t.Fatalf("overflow Submit: got %v, want %q", err, tc.message)
				}
			}
			if len(q) != tc.capacity {
				t.Fatalf("queue grew past its bound: %d", len(q))
			}

			// Draining frees capacity again.
			sat.releaseAll()
			recvResults(t, ac, tc.capacity)

			if got := executed.Load(); got != int64(tc.capacity) {
				t.Fatalf("executed %d requests, want %d (rejected ones must never run)", got, tc.capacity)
			}
			if err := ac.Submit(Request{ID: -1, Priority: tc.priority, Context: ctx, Process: okProcess}); err != nil {
				t.Fatalf("Submit after draining: %v", err)
			}
		})
	}
}

func TestQueueFullIsPerPriority(t *testing.T) {
	ac, ctx := newController(t)
	saturate(t, ac, ctx)

	for i := 0; i < HighQueueBufferSize; i++ {
		if err := ac.Submit(Request{ID: i, Priority: HighPriority, Context: ctx, Process: okProcess}); err != nil {
			t.Fatalf("filling high queue: %v", err)
		}
	}
	if err := ac.Submit(Request{ID: -1, Priority: HighPriority, Context: ctx, Process: okProcess}); err == nil {
		t.Fatal("expected high queue to be full")
	}
	if err := ac.Submit(Request{ID: -2, Priority: MediumPriority, Context: ctx, Process: okProcess}); err != nil {
		t.Fatalf("medium queue should be unaffected by a full high queue: %v", err)
	}
	if err := ac.Submit(Request{ID: -3, Priority: LowPriority, Context: ctx, Process: okProcess}); err != nil {
		t.Fatalf("low queue should be unaffected by a full high queue: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Priority ordering
// -----------------------------------------------------------------------------

func TestPriorityOrdering(t *testing.T) {
	ac, ctx := newController(t)
	sat := saturate(t, ac, ctx)

	var order startOrder

	// Submit in the *reverse* of priority order.
	for i := 0; i < 3; i++ {
		must(t, ac.Submit(Request{ID: 300 + i, Priority: LowPriority, Context: ctx, Process: order.process}))
	}
	for i := 0; i < 3; i++ {
		must(t, ac.Submit(Request{ID: 200 + i, Priority: MediumPriority, Context: ctx, Process: order.process}))
	}
	for i := 0; i < 3; i++ {
		must(t, ac.Submit(Request{ID: 100 + i, Priority: HighPriority, Context: ctx, Process: order.process}))
	}

	// Free exactly one worker: it drains the queues alone, so the observed
	// start order is exactly the controller's selection order.
	sat.release(0)
	recvResults(t, ac, 9)

	got, _ := order.snapshot()
	want := []int{100, 101, 102, 200, 201, 202, 300, 301, 302}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("execution order = %v, want %v", got, want)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Fairness
// -----------------------------------------------------------------------------

const fairCycle = MaxHighQueueFairCount + MaxMediumQueueFairCount + MaxLowQueueFairCount

func expectedFairQueue(i int) string {
	switch pos := i % fairCycle; {
	case pos < MaxHighQueueFairCount:
		return "high"
	case pos < MaxHighQueueFairCount+MaxMediumQueueFairCount:
		return "medium"
	default:
		return "low"
	}
}

// With every queue permanently non-empty, queue selection must follow the
// quota cycle: 100 high, 10 medium, 5 low, repeat.
func TestFairQueueSelectionCycle(t *testing.T) {
	ac, ctx := newController(t)
	saturate(t, ac, ctx)

	must(t, ac.Submit(Request{ID: 1, Priority: HighPriority, Context: ctx, Process: okProcess}))
	must(t, ac.Submit(Request{ID: 2, Priority: MediumPriority, Context: ctx, Process: okProcess}))
	must(t, ac.Submit(Request{ID: 3, Priority: LowPriority, Context: ctx, Process: okProcess}))

	// getFairQueue only inspects len(), it never dequeues, so the queues stay
	// non-empty for the whole sequence.
	for i := 0; i < 3*fairCycle; i++ {
		got := queueName(ac, ac.getFairQueue())
		if want := expectedFairQueue(i); got != want {
			t.Fatalf("selection #%d = %s, want %s", i, got, want)
		}
	}
}

func TestFairQueueSkipsEmptyQueues(t *testing.T) {
	ac, ctx := newController(t)
	saturate(t, ac, ctx)

	// Only the low queue has work: it must be served even beyond its quota.
	must(t, ac.Submit(Request{ID: 1, Priority: LowPriority, Context: ctx, Process: okProcess}))
	for i := 0; i < 4*MaxLowQueueFairCount; i++ {
		if got := queueName(ac, ac.getFairQueue()); got != "low" {
			t.Fatalf("selection #%d = %s, want low", i, got)
		}
	}
}

// Sustained high-priority load must not starve lower priorities: the lower
// queues get their turn after every MaxHighQueueFairCount high requests.
func TestFairnessUnderSustainedHighPriorityLoad(t *testing.T) {
	const (
		highCount   = 300
		mediumCount = 20
		lowCount    = 20
		total       = highCount + mediumCount + lowCount
	)

	ac, ctx := newController(t)
	sat := saturate(t, ac, ctx)

	var order startOrder
	for i := 0; i < highCount; i++ {
		must(t, ac.Submit(Request{ID: i, Priority: HighPriority, Context: ctx, Process: order.process}))
	}
	for i := 0; i < mediumCount; i++ {
		must(t, ac.Submit(Request{ID: 1000 + i, Priority: MediumPriority, Context: ctx, Process: order.process}))
	}
	for i := 0; i < lowCount; i++ {
		must(t, ac.Submit(Request{ID: 2000 + i, Priority: LowPriority, Context: ctx, Process: order.process}))
	}

	sat.release(0) // a single worker drains everything => deterministic order
	recvResults(t, ac, total)

	_, prios := order.snapshot()
	if len(prios) != total {
		t.Fatalf("executed %d requests, want %d", len(prios), total)
	}

	names := map[Priority]string{HighPriority: "high", MediumPriority: "medium", LowPriority: "low"}
	for i := 0; i < 2*fairCycle; i++ {
		if got, want := names[prios[i]], expectedFairQueue(i); got != want {
			t.Fatalf("execution #%d was %s, want %s", i, got, want)
		}
	}

	// Lower priorities were served long before the high backlog drained.
	firstLow := -1
	lastHigh := -1
	for i, p := range prios {
		if p == LowPriority && firstLow < 0 {
			firstLow = i
		}
		if p == HighPriority {
			lastHigh = i
		}
	}
	if firstLow < 0 || firstLow >= lastHigh {
		t.Fatalf("low priority starved: first low at %d, last high at %d", firstLow, lastHigh)
	}
}

// -----------------------------------------------------------------------------
// Cancellation / deadline while queued
// -----------------------------------------------------------------------------

func ctxAwareProcess(ran *atomic.Bool) func(context.Context, int, Priority) (Result, error) {
	return func(ctx context.Context, id int, _ Priority) (Result, error) {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		ran.Store(true)
		return Result{ID: id}, nil
	}
}

func runQueuedContextCase(t *testing.T, derive func(parent context.Context) (context.Context, func()), want error) {
	t.Helper()

	ac, ctx := newController(t)
	sat := saturate(t, ac, ctx)

	reqCtx, trigger := derive(ctx)
	var ranDoomed, ranHealthy atomic.Bool

	must(t, ac.Submit(Request{ID: 1, Priority: HighPriority, Context: reqCtx, Process: ctxAwareProcess(&ranDoomed)}))
	must(t, ac.Submit(Request{ID: 2, Priority: HighPriority, Context: ctx, Process: ctxAwareProcess(&ranHealthy)}))

	trigger() // the request's context ends while it sits in the queue
	sat.release(0)

	if err := recvError(t, ac); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	res := recvResults(t, ac, 1)
	if res[0].ID != 2 {
		t.Fatalf("surviving result = %+v, want ID 2", res[0])
	}
	if ranDoomed.Load() {
		t.Error("request with a dead context performed its work")
	}
	if !ranHealthy.Load() {
		t.Error("healthy request behind it did not run")
	}
}

func TestCancellationWhileQueued(t *testing.T) {
	runQueuedContextCase(t, func(parent context.Context) (context.Context, func()) {
		c, cancel := context.WithCancel(parent)
		return c, cancel
	}, context.Canceled)
}

// The deadline is already in the past when the request is queued, so no
// wall-clock waiting is involved; it is expired for the entire time it is queued.
func TestDeadlineExpiredWhileQueued(t *testing.T) {
	runQueuedContextCase(t, func(parent context.Context) (context.Context, func()) {
		c, cancel := context.WithDeadline(parent, time.Unix(0, 0))
		t.Cleanup(cancel)
		return c, func() {}
	}, context.DeadlineExceeded)
}

// -----------------------------------------------------------------------------
// Cancellation during execution
// -----------------------------------------------------------------------------

func TestCancellationDuringExecution(t *testing.T) {
	ac, ctx := newController(t)

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	started := make(chan struct{})
	blocking := func(c context.Context, _ int, _ Priority) (Result, error) {
		close(started)
		<-c.Done()
		return Result{}, c.Err()
	}

	must(t, ac.Submit(Request{ID: 1, Priority: HighPriority, Context: reqCtx, Process: blocking}))
	<-started
	cancel()

	if err := recvError(t, ac); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	// The worker must have been returned to the pool.
	must(t, ac.Submit(Request{ID: 2, Priority: HighPriority, Context: ctx, Process: okProcess}))
	if got := recvResults(t, ac, 1)[0]; got.ID != 2 {
		t.Fatalf("follow-up result = %+v, want ID 2", got)
	}
}

// -----------------------------------------------------------------------------
// Execution errors
// -----------------------------------------------------------------------------

func TestExecutionErrors(t *testing.T) {
	const total = 20

	ac, ctx := newController(t)

	proc := func(_ context.Context, id int, _ Priority) (Result, error) {
		if id%2 == 1 {
			return Result{}, fmt.Errorf("bad id %d", id)
		}
		return Result{ID: id, Data: "ok"}, nil
	}
	for i := 0; i < total; i++ {
		must(t, ac.Submit(Request{ID: i, Priority: HighPriority, Context: ctx, Process: proc}))
	}

	gotResults := map[int]bool{}
	for _, r := range recvResults(t, ac, total/2) {
		if r.ID%2 != 0 {
			t.Errorf("failed request %d produced a result", r.ID)
		}
		gotResults[r.ID] = true
	}
	gotErrs := map[string]bool{}
	for i := 0; i < total/2; i++ {
		gotErrs[recvError(t, ac).Error()] = true
	}

	for id := 0; id < total; id++ {
		if id%2 == 0 && !gotResults[id] {
			t.Errorf("missing result for %d", id)
		}
		if id%2 == 1 && !gotErrs[fmt.Sprintf("bad id %d", id)] {
			t.Errorf("missing error for %d", id)
		}
	}
}

func TestPanickingRequestIsReportedAndWorkerSurvives(t *testing.T) {
	ac, ctx := newController(t)

	boom := func(context.Context, int, Priority) (Result, error) { panic("boom") }
	must(t, ac.Submit(Request{ID: 99, Priority: HighPriority, Context: ctx, Process: boom}))

	err := recvError(t, ac)
	if err == nil || !strings.Contains(err.Error(), "request 99 panicked: boom") {
		t.Fatalf("error = %v, want panic report for request 99", err)
	}

	// Saturate the pool with panics, then prove all workers are still alive.
	for i := 0; i < WorkersCount*2; i++ {
		must(t, ac.Submit(Request{ID: i, Priority: HighPriority, Context: ctx, Process: boom}))
	}
	for i := 0; i < WorkersCount*2; i++ {
		recvError(t, ac)
	}
	for i := 0; i < WorkersCount; i++ {
		must(t, ac.Submit(Request{ID: 500 + i, Priority: HighPriority, Context: ctx, Process: okProcess}))
	}
	recvResults(t, ac, WorkersCount)
}

// -----------------------------------------------------------------------------
// Shutdown behaviour
// -----------------------------------------------------------------------------

func TestShutdownWithQueuedWork(t *testing.T) {
	ac, ctx := newController(t)
	sat := saturate(t, ac, ctx)

	var ran atomic.Int32
	counting := func(c context.Context, id int, p Priority) (Result, error) {
		ran.Add(1)
		return okProcess(c, id, p)
	}
	for _, p := range []Priority{HighPriority, MediumPriority, LowPriority} {
		for i := 0; i < 5; i++ {
			must(t, ac.Submit(Request{ID: i, Priority: p, Context: ctx, Process: counting}))
		}
	}

	shutdownDone := make(chan struct{})
	go func() {
		ac.Shutdown()
		close(shutdownDone)
	}()
	<-ac.ctx.Done() // shutdown has begun and canceled the controller
	sat.releaseAll()
	<-shutdownDone

	if got := ran.Load(); got != 0 {
		t.Errorf("%d queued requests ran after shutdown began, want 0", got)
	}
	if !ac.stopped.Load() {
		t.Error("controller not marked stopped")
	}
	for r := range ac.Results() { // closed => terminates
		if r.ID < blockerIDBase {
			t.Errorf("queued request %d produced a result after shutdown", r.ID)
		}
	}
	for range ac.Errors() {
	}
}

func TestShutdownWithRunningWork(t *testing.T) {
	t.Run("waits for non-cooperative running work", func(t *testing.T) {
		ac, ctx := newController(t)
		sat := saturate(t, ac, ctx)

		shutdownDone := make(chan struct{})
		go func() {
			ac.Shutdown()
			close(shutdownDone)
		}()
		<-ac.ctx.Done() // shutdown is in progress, workers are still inside Process

		if ac.stopped.Load() {
			t.Fatal("controller reported stopped while work was still running")
		}
		select {
		case _, ok := <-ac.Results():
			if !ok {
				t.Fatal("results closed while work was still running")
			}
		default:
		}

		sat.releaseAll()
		<-shutdownDone

		if !ac.stopped.Load() {
			t.Fatal("controller not stopped after running work finished")
		}
		for range ac.Results() {
		}
		for range ac.Errors() {
		}
	})

	t.Run("cooperative running work is interrupted", func(t *testing.T) {
		ac, _ := newController(t)

		const n = 3
		started := make(chan struct{}, n)
		proc := func(c context.Context, _ int, _ Priority) (Result, error) {
			started <- struct{}{}
			<-c.Done()
			return Result{}, c.Err()
		}
		for i := 0; i < n; i++ {
			// Using the controller's own context so Shutdown cancels the work.
			must(t, ac.Submit(Request{ID: i, Priority: HighPriority, Context: ac.ctx, Process: proc}))
		}
		for i := 0; i < n; i++ {
			<-started
		}

		ac.Shutdown() // must return without anyone releasing the work

		if !ac.stopped.Load() {
			t.Fatal("controller not stopped")
		}
		count := 0
		for err := range ac.Errors() {
			count++
			if !errors.Is(err, context.Canceled) {
				t.Errorf("unexpected error: %v", err)
			}
		}
		if count > n {
			t.Errorf("got %d errors for %d requests", count, n)
		}
		for range ac.Results() {
		}
	})
}

func TestParentContextCancellationShutsDownController(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ac := NewAdmissionController(ctx)

	must(t, ac.Submit(Request{ID: 1, Priority: HighPriority, Context: ctx, Process: okProcess}))
	recvResults(t, ac, 1)

	cancel()

	// Both channels are closed once the shutdown triggered by the parent finishes.
	for range ac.Results() {
	}
	for range ac.Errors() {
	}
	if !ac.stopped.Load() {
		t.Fatal("controller not stopped after parent context cancellation")
	}
	if err := ac.Submit(Request{ID: 2, Priority: HighPriority, Context: context.Background(), Process: okProcess}); err == nil {
		t.Fatal("Submit succeeded after parent context cancellation")
	}
}

// -----------------------------------------------------------------------------
// Submission racing with shutdown / waiting for admission
// -----------------------------------------------------------------------------

func TestSubmissionRacingWithShutdown(t *testing.T) {
	const (
		iterations = 20
		submitters = 8
		perG       = 20
	)

	for iter := 0; iter < iterations; iter++ {
		ctx, cancel := context.WithCancel(context.Background())
		ac := NewAdmissionController(ctx)

		var accepted atomic.Int64
		start := make(chan struct{})
		var wg sync.WaitGroup

		for g := 0; g < submitters; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < perG; i++ {
					err := ac.Submit(Request{ID: g*perG + i, Priority: HighPriority, Context: ctx, Process: okProcess})
					switch {
					case err == nil:
						accepted.Add(1)
					case isShutdownErr(err):
					default:
						t.Errorf("unexpected Submit error: %v", err)
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ac.Shutdown()
		}()

		close(start)
		wg.Wait()

		if err := ac.Submit(Request{ID: -1, Priority: HighPriority, Context: ctx, Process: okProcess}); err == nil {
			t.Fatal("Submit succeeded after shutdown completed")
		}

		seen := map[int]bool{}
		for r := range ac.Results() {
			if seen[r.ID] {
				t.Errorf("result %d delivered twice", r.ID)
			}
			seen[r.ID] = true
		}
		if int64(len(seen)) > accepted.Load() {
			t.Errorf("%d results for %d accepted requests", len(seen), accepted.Load())
		}
		for range ac.Errors() {
		}
		cancel()
	}
}

// A Submit that arrives while a shutdown is in progress is held back by the
// controller's lifecycle lock; once the shutdown completes it must be rejected
// and its request must never run.
func TestSubmitWaitingDuringShutdownIsRejected(t *testing.T) {
	ac, ctx := newController(t)
	sat := saturate(t, ac, ctx)

	shutdownDone := make(chan struct{})
	go func() {
		ac.Shutdown()
		close(shutdownDone)
	}()
	<-ac.ctx.Done() // shutdown holds the lifecycle lock and is waiting for workers

	var ran atomic.Bool
	submitErr := make(chan error, 1)
	go func() {
		submitErr <- ac.Submit(Request{ID: 1, Priority: HighPriority, Context: ctx, Process: ctxAwareProcess(&ran)})
	}()

	sat.releaseAll()
	<-shutdownDone

	if err := <-submitErr; err == nil || err.Error() != stoppedMsg {
		t.Fatalf("Submit = %v, want %q", err, stoppedMsg)
	}
	if ran.Load() {
		t.Error("request submitted during shutdown was executed")
	}
}

func TestSubmitAfterControllerContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ac := NewAdmissionController(ctx)

	cancel()
	for range ac.Results() { // wait until shutdown (driven by the cancel) has finished
	}

	var ran atomic.Bool
	err := ac.Submit(Request{ID: 1, Priority: HighPriority, Context: context.Background(), Process: ctxAwareProcess(&ran)})
	if err == nil {
		t.Fatal("Submit succeeded on a cancelled controller")
	}
	if !isShutdownErr(err) {
		t.Fatalf("unexpected error: %v", err)
	}
	if ran.Load() {
		t.Error("request ran on a cancelled controller")
	}
}

// -----------------------------------------------------------------------------
// Repeated shutdown / submit after shutdown
// -----------------------------------------------------------------------------

func TestRepeatedShutdown(t *testing.T) {
	t.Run("sequential", func(t *testing.T) {
		ac, ctx := newController(t)
		must(t, ac.Submit(Request{ID: 1, Priority: HighPriority, Context: ctx, Process: okProcess}))
		recvResults(t, ac, 1)

		for i := 0; i < 5; i++ {
			ac.Shutdown() // must neither panic (double close) nor block
		}
		if !ac.stopped.Load() {
			t.Fatal("not stopped")
		}
		if _, ok := <-ac.Results(); ok {
			t.Error("results channel should be closed and drained")
		}
		if _, ok := <-ac.Errors(); ok {
			t.Error("errors channel should be closed and drained")
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		ac, _ := newController(t)

		const callers = 16
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ac.Shutdown()
			}()
		}
		close(start)
		wg.Wait()

		if !ac.stopped.Load() {
			t.Fatal("not stopped")
		}
	})

	t.Run("after parent cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ac := NewAdmissionController(ctx)
		cancel()
		ac.Shutdown()
		ac.Shutdown()
		if !ac.stopped.Load() {
			t.Fatal("not stopped")
		}
	})
}

func TestSubmitAfterShutdown(t *testing.T) {
	ac, ctx := newController(t)
	ac.Shutdown()

	for _, p := range []Priority{HighPriority, MediumPriority, LowPriority} {
		var ran atomic.Bool
		err := ac.Submit(Request{ID: 1, Priority: p, Context: ctx, Process: ctxAwareProcess(&ran)})
		if err == nil || err.Error() != stoppedMsg {
			t.Errorf("%s: Submit = %v, want %q", p, err, stoppedMsg)
		}
		if ran.Load() {
			t.Errorf("%s: request ran after shutdown", p)
		}
		if n := len(queueFor(ac, p)); n != 0 {
			t.Errorf("%s: %d requests were enqueued after shutdown", p, n)
		}
	}

	if err := ac.Start(); err == nil {
		t.Error("Start succeeded after shutdown")
	}
}

// -----------------------------------------------------------------------------
// Goroutine-leak-sensitive lifecycle behaviour
// -----------------------------------------------------------------------------

func TestNoGoroutineLeakAfterShutdown(t *testing.T) {
	baseline := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ac := NewAdmissionController(ctx)

		for j := 0; j < 50; j++ {
			must(t, ac.Submit(Request{ID: j, Priority: HighPriority, Context: ctx, Process: okProcess}))
		}
		recvResults(t, ac, 50)

		ac.Shutdown()
		cancel()
	}

	waitForGoroutines(t, baseline)
}

func TestNoGoroutineLeakWhenOnlyParentContextIsCancelled(t *testing.T) {
	baseline := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ac := NewAdmissionController(ctx)

		must(t, ac.Submit(Request{ID: 1, Priority: HighPriority, Context: ctx, Process: okProcess}))
		recvResults(t, ac, 1)

		cancel() // no explicit Shutdown: the internal watcher must do it
		for range ac.Results() {
		}
	}

	waitForGoroutines(t, baseline)
}

func TestNoGoroutineLeakWithQueuedAndRunningWork(t *testing.T) {
	baseline := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	ac := NewAdmissionController(ctx)

	started := make(chan struct{}, WorkersCount)
	blocking := func(c context.Context, _ int, _ Priority) (Result, error) {
		started <- struct{}{}
		<-c.Done()
		return Result{}, c.Err()
	}

	// Occupy all workers with cooperative work, then pile up a backlog.
	for i := 0; i < WorkersCount; i++ {
		must(t, ac.Submit(Request{ID: i, Priority: HighPriority, Context: ac.ctx, Process: blocking}))
	}
	for i := 0; i < WorkersCount; i++ {
		<-started
	}
	for i := 0; i < 100; i++ {
		must(t, ac.Submit(Request{ID: 1000 + i, Priority: HighPriority, Context: ctx, Process: okProcess}))
		must(t, ac.Submit(Request{ID: 2000 + i, Priority: MediumPriority, Context: ctx, Process: okProcess}))
		must(t, ac.Submit(Request{ID: 3000 + i, Priority: LowPriority, Context: ctx, Process: okProcess}))
	}

	ac.Shutdown()
	cancel()

	waitForGoroutines(t, baseline)
}
