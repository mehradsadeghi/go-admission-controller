package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	WorkersCount          = 8
	HighQueueBufferSize   = 1000
	MediumQueueBufferSize = 1000
	LowQueueBufferSize    = 1000
	ResultsBufferSize     = 1000
	ErrsBufferSize        = 1000
)

const (
	MaxHighQueueFairCount   = 100
	MaxMediumQueueFairCount = 10
	MaxLowQueueFairCount    = 5
)

type Priority int

const (
	HighPriority Priority = iota
	MediumPriority
	LowPriority
)

func (p Priority) String() string {
	switch p {
	case HighPriority:
		return "High"
	case MediumPriority:
		return "Medium"
	case LowPriority:
		return "Low"
	default:
		return "Unknown"
	}
}

type Request struct {
	ID       int
	Priority Priority
	Context  context.Context
	Process  func(context.Context, int, Priority) (Result, error)
}

type Result struct {
	ID   int
	Data string
}

type AdmissionController struct {
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	results           chan Result
	errs              chan error
	done              chan struct{}
	startOnce         sync.Once
	shutdownOnce      sync.Once
	stopped           atomic.Bool
	startedMu         sync.Mutex
	highPriorityQueue chan Request

	mediumPriorityQueue chan Request
	lowPriorityQueue    chan Request

	fairnessMu           sync.Mutex
	highQueueFairCount   uint32
	mediumQueueFairCount uint32
	lowQueueFairCount    uint32
}

// NewAdmissionController instantiates a new AdmissionController and immediately starts consumers
func NewAdmissionController(ctx context.Context) *AdmissionController {
	ctx, cancel := context.WithCancel(ctx)
	ac := &AdmissionController{
		ctx:                 ctx,
		cancel:              cancel,
		wg:                  sync.WaitGroup{},
		fairnessMu:          sync.Mutex{},
		done:                make(chan struct{}),
		results:             make(chan Result, ResultsBufferSize),
		errs:                make(chan error, ErrsBufferSize),
		highPriorityQueue:   make(chan Request, HighQueueBufferSize),
		mediumPriorityQueue: make(chan Request, MediumQueueBufferSize),
		lowPriorityQueue:    make(chan Request, LowQueueBufferSize),
	}

	go func() {
		<-ctx.Done()
		ac.Shutdown()
	}()

	if err := ac.Start(); err != nil {
		log.Fatal("could not start admission controller", err)
	}

	return ac
}

func (a *AdmissionController) Submit(request Request) error {
	a.startedMu.Lock()

	if a.stopped.Load() {
		a.startedMu.Unlock()
		return errors.New("admission controller already stopped")
	}

	a.startedMu.Unlock()

	switch request.Priority {
	case HighPriority:
		select {
		case a.highPriorityQueue <- request:
		case <-a.ctx.Done():
			return a.ctx.Err()
		default:
			return errors.New("high priority queue is full")
		}
	case MediumPriority:
		select {
		case a.mediumPriorityQueue <- request:
		case <-a.ctx.Done():
			return a.ctx.Err()
		default:
			return errors.New("medium priority queue is full")
		}
	case LowPriority:
		select {
		case a.lowPriorityQueue <- request:
		case <-a.ctx.Done():
			return a.ctx.Err()
		default:
			return errors.New("low priority queue is full")
		}
	default:
		return errors.New("unknown priority")
	}

	return nil
}

// getFairQueue returns a queue. A simple high/medium/low priority based implementation.
func (a *AdmissionController) getFairQueue() <-chan Request {
	a.fairnessMu.Lock()
	defer a.fairnessMu.Unlock()

	if len(a.highPriorityQueue) > 0 &&
		a.highQueueFairCount < MaxHighQueueFairCount {
		a.highQueueFairCount++
		return a.highPriorityQueue
	}

	if len(a.mediumPriorityQueue) > 0 &&
		a.mediumQueueFairCount < MaxMediumQueueFairCount {
		a.mediumQueueFairCount++
		return a.mediumPriorityQueue
	}

	if len(a.lowPriorityQueue) > 0 &&
		a.lowQueueFairCount < MaxLowQueueFairCount {
		a.lowQueueFairCount++
		return a.lowPriorityQueue
	}

	// Quotas exhausted: start another round.
	a.highQueueFairCount = 0
	a.mediumQueueFairCount = 0
	a.lowQueueFairCount = 0

	if len(a.highPriorityQueue) > 0 {
		a.highQueueFairCount++
		return a.highPriorityQueue
	}

	if len(a.mediumPriorityQueue) > 0 {
		a.mediumQueueFairCount++
		return a.mediumPriorityQueue
	}

	if len(a.lowPriorityQueue) > 0 {
		a.lowQueueFairCount++
		return a.lowPriorityQueue
	}

	return a.highPriorityQueue
}

func (a *AdmissionController) Start() error {
	if a.stopped.Load() {
		return errors.New("admission controller already stopped")
	}

	a.startOnce.Do(func() {
		for range WorkersCount {
			a.wg.Add(1)
			go func() {
				defer a.wg.Done()
				for {
					select {
					case result, ok := <-a.getFairQueue():
						if !ok {
							return
						}
						if a.ctx.Err() != nil {
							return
						}

						if e := a.processRequest(result); e != nil {
							a.NewError(e)
							continue
						}
					case <-a.ctx.Done():
						return
					}
				}
			}()
		}
	})

	return nil
}

func (a *AdmissionController) NewError(e error) {
	select {
	case a.errs <- e:
	case <-a.ctx.Done():
		return
	default:
		log.Fatal("errs channel is full", e)
	}
}

func (a *AdmissionController) Shutdown() {
	a.startedMu.Lock()
	defer a.startedMu.Unlock()

	a.shutdownOnce.Do(func() {
		a.cancel()
		a.wg.Wait()
		a.stopped.Store(true)
		close(a.results)
		close(a.errs)
	})
}

func (a *AdmissionController) Results() <-chan Result {
	return a.results
}

func (a *AdmissionController) Errors() <-chan error {
	return a.errs
}

func process(ctx context.Context, id int, p Priority) (Result, error) {
	select {
	case <-time.After(time.Millisecond * time.Duration(rand.IntN(10))):
		// heavy computation
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}

	// simulate occasionally return error instead of nil
	if id == 1 || id == 2 {
		return Result{}, errors.New("bad id #" + strconv.Itoa(id) + " with priority: " + p.String())
	}

	return Result{
		ID:   id,
		Data: "Processed #" + strconv.Itoa(id) + " with priority: " + p.String(),
	}, nil
}

func (a *AdmissionController) processRequest(req Request) error {
	defer func() {
		if r := recover(); r != nil {
			select {
			case a.errs <- fmt.Errorf("request %d panicked: %v", req.ID, r):
			case <-a.ctx.Done():
			}
		}
	}()

	r, err := req.Process(req.Context, req.ID, req.Priority)
	if err != nil {
		return err
	}

	select {
	case a.results <- r:
	case <-a.ctx.Done():
	}

	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewAdmissionController(ctx)

	go func() {
		for v := range a.Results() {
			fmt.Println(v)
		}
	}()

	go func() {
		for v := range a.Errors() {
			fmt.Println(v)
		}
	}()

	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Submit(Request{ID: i, Priority: HighPriority, Context: ctx, Process: process}); err != nil {
				fmt.Println(err)
			}
		}()
	}

	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Submit(Request{ID: i, Priority: MediumPriority, Context: ctx, Process: process}); err != nil {
				fmt.Println(err)
			}
		}()
	}

	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Submit(Request{ID: i, Priority: LowPriority, Context: ctx, Process: process}); err != nil {
				fmt.Println(err)
			}
		}()
	}

	<-time.After(time.Second)
	wg.Wait()
	a.Shutdown()
}
