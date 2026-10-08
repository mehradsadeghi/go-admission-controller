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

## Request Context

Each request contains a `Context`:

```go
type Request struct {
    ID       int
    Priority Priority
    Context  context.Context
    Process  func(context.Context, int, Priority, time.Duration) (Result, error)
}
```

The example `process` function creates a child context:

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

# Priority Admission Controller

A small Go admission controller that processes requests concurrently using a fixed worker pool and three priority queues:

* **High**
* **Medium**
* **Low**

Each priority has its own bounded buffered queue. Requests are rejected when their corresponding queue is full.

## Scheduling & Fairness

The controller runs **8 workers** by default. Workers select requests according to a quota-based fairness policy:

| Priority | Maximum consecutive selections per round |
| -------- | ---------------------------------------: |
| High     |                                      100 |
| Medium   |                                       10 |
| Low      |                                        5 |

Within a scheduling round, higher-priority queues are preferred while their quota remains available. Once all available quotas are exhausted, the counters are reset and a new round begins.

This provides **priority preference without allowing a continuously busy high-priority queue to permanently starve lower priorities**.

For example, when all three queues remain non-empty, the scheduler can process up to 100 high-priority requests, 10 medium-priority requests, and 5 low-priority requests before starting another fairness round.

The policy is **approximate rather than strict weighted scheduling** because workers independently select queues and multiple workers may make scheduling decisions concurrently.

## Backpressure

Each queue is bounded:

```text
High:   1000 requests
Medium: 1000 requests
Low:    1000 requests
```

`Submit` is non-blocking. It returns an error when the target queue is full rather than waiting for capacity.

This makes overload visible to callers and prevents producers from accumulating indefinitely inside the controller.

## Concurrency Trade-off

The scheduler protects its fairness counters with a mutex. This keeps updates consistent across workers, but introduces contention because every worker must acquire the same lock before selecting a queue.

The implementation deliberately favors **simple, predictable synchronization** over maximizing scheduler throughput. Since request processing happens outside this lock, the critical section remains small and the actual work can still execute concurrently across all workers.

A related trade-off is the use of buffered queues: buffering absorbs short bursts and keeps workers busy, but it also allows requests to wait in memory and means queue length can temporarily hide overload from callers until a buffer fills.

## Shutdown

`Shutdown`:

1. Cancels the controller context.
2. Waits for all workers to exit.
3. Marks the controller as stopped.
4. Closes the results and error channels.

Calling `Shutdown` multiple times is safe.

Requests and processing functions should honor their contexts so cancellation can propagate promptly.

## Example

```go
ctx := context.Background()

ac := NewAdmissionController(ctx)

err := ac.Submit(Request{
    ID:       1,
    Priority: HighPriority,
    Context:  ctx,
    Process:  process,
})
if err != nil {
    // Queue full or controller stopped.
}

ac.Shutdown()
```

## Configuration

The main scheduling parameters are defined as constants:

```go
WorkersCount        = 8
MaxHighQueueFairCount   = 100
MaxMediumQueueFairCount = 10
MaxLowQueueFairCount    = 5
```

Adjust these values to change worker concurrency, queue capacity, and the balance between priority and fairness.
