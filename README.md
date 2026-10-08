# Admission Controller

A concurrent Go admission controller that accepts requests across three priority levels and processes them with a fixed-size worker pool.

The controller provides:

* **8 concurrent workers**
* **Three priority queues:** High, Medium, and Low
* **Bounded queues** to provide backpressure
* **Weighted fair scheduling** to prevent lower-priority requests from being permanently starved
* **Per-request processing deadlines**
* **Buffered result and error channels**
* **Context-aware cancellation and shutdown**
* **Non-blocking request submission**

## Architecture

Requests are placed into one of three bounded queues:

```text
                    +-------------------+
                    | AdmissionController|
                    +---------+---------+
                              |
              +---------------+---------------+
              |               |               |
              v               v               v
        High Queue       Medium Queue       Low Queue
        capacity 1000    capacity 1000      capacity 1000
              |               |               |
              +---------------+---------------+
                              |
                        Fair Scheduler
                              |
                +-------------+-------------+
                |             |             |
              Worker 1      ...           Worker 8
                |                           |
                +-------------+-------------+
                              |
                    +---------+---------+
                    |                   |
                    v                   v
                Results              Errors
                buffer 1000          buffer 1000
```

The controller starts its workers when `NewAdmissionController` is called.

## Priority Levels

Requests can have one of three priorities:

| Priority | Fairness quota |
| -------- | -------------: |
| High     |            100 |
| Medium   |             10 |
| Low      |              5 |

Higher priority requests receive a larger share of scheduling opportunities, but lower-priority requests are still given opportunities to execute.

The quotas are **scheduling weights**, not strict guarantees about latency or completion order.

## Scheduling and Fairness Policy

The scheduler uses a weighted round-based policy.

During a scheduling round, workers are allowed to select up to:

* **100 High** requests
* **10 Medium** requests
* **5 Low** requests

before the scheduler resets its counters and begins another round.

Conceptually, when all queues are continuously populated, the scheduler attempts to provide a ratio close to:

```text
High : Medium : Low
100  : 10     : 5
```

or approximately:

```text
87% : 9% : 4%
```

of scheduling opportunities.

### Why use weighted fairness?

A strict priority scheduler would always select High requests whenever High work is available:

```text
if high queue is non-empty:
    process high
else if medium queue is non-empty:
    process medium
else:
    process low
```

That approach maximizes preference for High requests, but a continuously busy High queue could prevent Medium and Low requests from ever running.

This implementation deliberately trades some High-priority latency for **bounded starvation risk** for lower priorities.

For example, when all three queues are populated, Low-priority work gets scheduling opportunities after the scheduler has selected at most 100 High and 10 Medium requests in the current round.

### Important fairness limitation

The fairness policy operates on **queue selection**, not on individual request completion.

There are eight workers, so several workers can concurrently remove requests from queues. Consequently:

* Requests do not execute in strict priority order.
* Results may be returned out of order.
* The exact observed distribution can differ from the configured 100:10:5 ratio.
* A request's processing time can affect observed throughput and latency.

The policy should therefore be understood as **weighted admission/scheduling fairness**, rather than strict ordering.

## Backpressure

Each priority queue has a fixed capacity of `1000`.

```text
High   = 1000
Medium = 1000
Low    = 1000
```

`Submit` is intentionally non-blocking.

If the appropriate queue is full, `Submit` immediately returns an error:

```text
high priority queue is full
medium priority queue is full
low priority queue is full
```

This prevents an unlimited number of requests from accumulating in memory.

The caller is responsible for deciding what to do when admission is rejected. Possible strategies include:

* retrying later,
* dropping the request,
* applying application-level backpressure,
* returning an error to an upstream caller,
* using a separate retry queue.

### Why bounded queues?

An unbounded queue would make submission easier for callers, but sustained overload could cause memory usage to grow without limit.

Bounded queues instead make overload visible to the caller.

The trade-off is that **requests can be rejected even though workers may eventually become available**. This is intentional: the controller favors predictable resource usage over guaranteed admission.

## Worker Pool

The controller creates exactly `8` workers:

```go
const WorkersCount = 8
```

Each worker repeatedly:

1. Selects a queue according to the fairness policy.
2. Receives a request.
3. Checks whether the controller context has been cancelled.
4. Executes `Request.Process`.
5. Publishes either the resulting `Result` or an error.

At most eight requests are actively being processed at any given time.

### Concurrency trade-off

A fixed worker pool limits resource consumption and prevents the controller from creating an unbounded number of goroutines.

The trade-off is that concurrency is capped at eight even if the machine has more available capacity.

This is particularly important when `Process` is I/O-bound: eight workers may leave available CPU capacity unused while workers wait on I/O.

Conversely, increasing the worker count can improve throughput for independent workloads but can also:

* increase CPU and memory usage,
* increase contention,
* put more pressure on downstream systems,
* increase the number of simultaneously executing requests.

Therefore, `WorkersCount` is a workload-dependent tuning parameter rather than an inherently optimal value.

## Request Deadlines

Each request contains a `Deadline` duration:

```go
type Request struct {
    ID       int
    Priority Priority
    Deadline time.Duration
    Process  func(context.Context, int, Priority, time.Duration) (Result, error)
}
```

The example `process` function creates a child context with a deadline:

```go
ctx, cancel := context.WithDeadline(
    ctx,
    time.Now().Add(d),
)
defer cancel()
```

This means request processing can terminate when its deadline expires.

The controller's parent context is also passed into the processing function, so cancellation of the controller propagates to active work.

### Important distinction

The deadline applies to **processing**, not time spent waiting in a priority queue.

A request may spend time waiting for a worker before `Process` creates its deadline context.

If queue wait time must also count against a request's total deadline budget, the request should carry an absolute deadline or admission timestamp and the processing function should derive its remaining time from that value.

## Results and Errors

Successful requests are written to the buffered `Results` channel.

Failed requests are written to the buffered `Errors` channel.

Both channels have a capacity of `1000`:

```go
const (
    ResultsBufferSize = 1000
    ErrsBufferSize    = 1000
)
```

Consumers can access them through:

```go
a.Results()
a.Errors()
```

For example:

```go
for result := range a.Results() {
    fmt.Println(result)
}

for err := range a.Errors() {
    fmt.Println(err)
}
```

### Result ordering

Results are **not guaranteed to be ordered** by:

* request ID,
* priority,
* submission order,
* or completion order across workers.

The first result received is simply the next result successfully published by any worker.

## Shutdown

Shutdown is coordinated through the controller's context.

Calling:

```go
a.Shutdown()
```

cancels the controller context and waits for all workers to exit.

After workers have stopped:

1. The controller is marked as stopped.
2. The results channel is closed.
3. The errors channel is closed.

Consumers can therefore use `range` safely:

```go
for result := range a.Results() {
    // ...
}
```

Shutdown is protected by `sync.Once`, so repeated calls to `Shutdown` are safe.

Likewise, `Start` is protected by `sync.Once`, preventing the worker pool from being started multiple times.

## Cancellation

The controller derives its internal context from the context supplied to:

```go
NewAdmissionController(ctx)
```

If the parent context is cancelled, the controller automatically shuts down.

This gives the controller two shutdown paths:

```text
Explicit:
    a.Shutdown()

Context cancellation:
    parent context cancelled
             |
             v
       controller ctx
             |
             v
        a.Shutdown()
```

## Concurrency Safety

The implementation uses several synchronization mechanisms:

### `sync.Once`

`startOnce` ensures that the worker pool is started only once.

`shutdownOnce` ensures that shutdown and channel closure happen only once.

### `sync.WaitGroup`

The worker pool uses a `WaitGroup` so shutdown can wait until every worker has exited.

### `atomic.Bool`

`stopped` provides a concurrency-safe indication that the controller has been stopped.

### `sync.Mutex`

`startedMu` protects the start/stop state transitions.

`fairnessMu` protects the fairness counters so multiple workers cannot update scheduling state concurrently.

## Fairness vs. Throughput Trade-off

The fairness scheduler introduces synchronization overhead.

Every worker calls `getFairQueue`, which takes `fairnessMu` before inspecting queue lengths and updating the fairness counters.

This creates a small serialization point:

```text
Worker 1 ----\
Worker 2 -----\
Worker 3 ------> fairnessMu -> queue selection
Worker 4 -----/
...
Worker 8 ----/
```

Without this lock, workers could select queues concurrently with less synchronization overhead, potentially improving throughput.

However, without synchronization, the fairness counters could be updated inconsistently and the intended weighted scheduling policy would become unreliable.

The implementation therefore trades a small amount of scheduler concurrency for **deterministic protection of the fairness accounting**.

This trade-off is most relevant when individual `Process` calls are extremely short. If processing takes milliseconds or longer, the cost of the scheduler lock is likely to be much smaller relative to the actual work.

## Queue Selection Behavior

When the scheduler's quotas have not been exhausted, it checks the queues in priority order:

1. High
2. Medium
3. Low

provided the corresponding queue has pending work and has not exhausted its quota.

Once all available quotas are exhausted, the counters are reset and a new round begins.

If no queue currently contains work, the scheduler falls back to the High queue:

```go
return a.highPriorityQueue
```

The worker's `select` can then wait for work from that channel or for controller cancellation.

## Capacity and Overload Characteristics

The controller has a maximum of:

```text
1000 High
1000 Medium
1000 Low
----------------
3000 queued requests
```

in addition to requests currently being processed by the eight workers.

This means the implementation has a bounded amount of queued work.

If producers submit substantially faster than workers can process requests, queues will eventually fill and subsequent submissions will fail.

This is preferable to allowing producer goroutines to block indefinitely or allowing memory usage to grow without bound.

## Example

The example creates the controller:

```go
a := NewAdmissionController(context.Background())
```

and submits 10,000 requests for each priority.

Because each queue is limited to 1,000 entries and submission is non-blocking, a significant number of submissions may be rejected when producers outrun the workers.

The example therefore demonstrates both:

* **bounded admission**, and
* **weighted priority scheduling**.

Applications using this controller should normally treat a queue-full error as an expected overload signal rather than as an unexpected internal failure.

## Design Summary

The implementation intentionally combines four properties:

| Property               | Mechanism                                       |
| ---------------------- | ----------------------------------------------- |
| Bounded resource usage | Fixed 8-worker pool + bounded queues            |
| Priority               | Separate High/Medium/Low queues                 |
| Fairness               | Weighted 100:10:5 scheduling                    |
| Overload protection    | Non-blocking submission + queue capacity limits |

The central design trade-off is:

> **Higher priority improves scheduling preference, but does not completely exclude lower priorities.**

This makes the controller appropriate for workloads where High-priority work should receive substantially more service while Medium- and Low-priority work must still make progress.

It is not intended to provide strict priority ordering, strict latency guarantees, or guaranteed admission under overload.
