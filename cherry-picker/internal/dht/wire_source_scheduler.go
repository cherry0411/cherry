package dht

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const defaultAnnounceSharePercent = 75

// SourceSchedulerOptions configures the optional wire admission scheduler.
// AnnounceSharePercent is both the announce queue's reserved capacity and its
// dequeue weight while both sources are continuously backlogged.
type SourceSchedulerOptions struct {
	AnnounceSharePercent int
}

// SourceQueueSnapshot is an instantaneous, internally consistent-enough
// operational view. Depths may change immediately after this method returns.
type SourceQueueSnapshot struct {
	Enabled          bool
	AnnounceDepth    int
	AnnounceCapacity int
	AnnounceAdmitted int64
	AnnounceDropped  int64
	LookupDepth      int
	LookupCapacity   int
	LookupAdmitted   int64
	LookupDropped    int64
}

// ConfigureSourceScheduler replaces the legacy FIFO with isolated announce and
// lookup queues. It is startup-only and intentionally opt-in: callers that do
// not invoke it retain the exact legacy channel implementation and ordering.
func (wire *Wire) ConfigureSourceScheduler(options SourceSchedulerOptions) error {
	share := options.AnnounceSharePercent
	if share == 0 {
		share = defaultAnnounceSharePercent
	}
	if share < 1 || share > 99 {
		return fmt.Errorf("announce share percent must be in [1,99], got %d", share)
	}
	if wire.retryObserver != nil {
		return fmt.Errorf("source scheduler must be configured before retry observer")
	}
	if wire.sourceRequests != nil || len(wire.requests) != 0 {
		return fmt.Errorf("source scheduler must be configured before request admission")
	}
	queue, err := newSourceRequestQueue[Request](cap(wire.requests), share)
	if err != nil {
		return err
	}
	wire.sourceRequests = queue
	wire.requests = nil
	return nil
}

// SourceQueueSnapshot reports the per-source queue split. In legacy FIFO mode
// Lookup includes the shared queue because no source isolation is active.
func (wire *Wire) SourceQueueSnapshot() SourceQueueSnapshot {
	if wire.sourceObservedRequests != nil {
		return wire.sourceObservedRequests.snapshot()
	}
	if wire.sourceRequests != nil {
		return wire.sourceRequests.snapshot()
	}
	if wire.retryObserver != nil {
		return SourceQueueSnapshot{LookupDepth: len(wire.observedRequests), LookupCapacity: cap(wire.observedRequests)}
	}
	return SourceQueueSnapshot{LookupDepth: len(wire.requests), LookupCapacity: cap(wire.requests)}
}

// sourceRequestQueue uses fixed-capacity channels so admission stays lock-free.
// Fixed queue isolation is deliberate: a get_peers burst cannot occupy the
// announce reservation (and vice versa). At dequeue time an empty preferred
// source immediately falls back to the other source, so capacity isolation
// never forces an idle wire worker.
type sourceRequestQueue[T any] struct {
	announce       chan T
	lookup         chan T
	announceWeight uint64
	lookupWeight   uint64
	nextTicket     atomic.Uint64
	announceIn     atomic.Int64
	announceDrop   atomic.Int64
	lookupIn       atomic.Int64
	lookupDrop     atomic.Int64
	closeOnce      sync.Once
}

func newSourceRequestQueue[T any](totalCapacity, announceSharePercent int) (*sourceRequestQueue[T], error) {
	if totalCapacity < 2 {
		return nil, fmt.Errorf("source scheduler requires request queue capacity >= 2, got %d", totalCapacity)
	}
	announceCapacity := totalCapacity * announceSharePercent / 100
	if announceCapacity < 1 {
		announceCapacity = 1
	}
	if announceCapacity >= totalCapacity {
		announceCapacity = totalCapacity - 1
	}
	lookupCapacity := totalCapacity - announceCapacity
	return &sourceRequestQueue[T]{
		announce:       make(chan T, announceCapacity),
		lookup:         make(chan T, lookupCapacity),
		announceWeight: uint64(announceSharePercent),
		lookupWeight:   uint64(100 - announceSharePercent),
	}, nil
}

func (q *sourceRequestQueue[T]) tryEnqueue(value T, source PeerSource) bool {
	target := q.lookup
	admitted := &q.lookupIn
	dropped := &q.lookupDrop
	if source == PeerSourceAnnounce {
		target = q.announce
		admitted = &q.announceIn
		dropped = &q.announceDrop
	}
	select {
	case target <- value:
		admitted.Add(1)
		return true
	default:
		dropped.Add(1)
		return false
	}
}

func (q *sourceRequestQueue[T]) dequeue() (T, bool) {
	ticket := q.nextTicket.Add(1) - 1
	preferAnnounce := ticket%(q.announceWeight+q.lookupWeight) < q.announceWeight
	if preferAnnounce {
		return receivePreferred(q.announce, q.lookup)
	}
	return receivePreferred(q.lookup, q.announce)
}

// receivePreferred never waits on the fallback if the preferred source is
// already readable, but it removes a closed input and drains the other one
// before reporting shutdown. This matters when many workers observe close.
func receivePreferred[T any](preferred, fallback <-chan T) (T, bool) {
	var zero T
	for preferred != nil || fallback != nil {
		if preferred != nil {
			select {
			case value, ok := <-preferred:
				if ok {
					return value, true
				}
				preferred = nil
				continue
			default:
			}
		}
		if fallback != nil {
			select {
			case value, ok := <-fallback:
				if ok {
					return value, true
				}
				fallback = nil
				continue
			default:
			}
		}

		select {
		case value, ok := <-preferred:
			if ok {
				return value, true
			}
			preferred = nil
		case value, ok := <-fallback:
			if ok {
				return value, true
			}
			fallback = nil
		}
	}
	return zero, false
}

func (q *sourceRequestQueue[T]) depth() int {
	return len(q.announce) + len(q.lookup)
}

func (q *sourceRequestQueue[T]) capacity() int {
	return cap(q.announce) + cap(q.lookup)
}

func (q *sourceRequestQueue[T]) snapshot() SourceQueueSnapshot {
	return SourceQueueSnapshot{
		Enabled:          true,
		AnnounceDepth:    len(q.announce),
		AnnounceCapacity: cap(q.announce),
		AnnounceAdmitted: q.announceIn.Load(),
		AnnounceDropped:  q.announceDrop.Load(),
		LookupDepth:      len(q.lookup),
		LookupCapacity:   cap(q.lookup),
		LookupAdmitted:   q.lookupIn.Load(),
		LookupDropped:    q.lookupDrop.Load(),
	}
}

func (q *sourceRequestQueue[T]) close() {
	q.closeOnce.Do(func() {
		close(q.announce)
		close(q.lookup)
	})
}
