package syncer

import (
	"container/heap"
	"context"
	"errors"

	logging "github.com/ipfs/go-log"

	"github.com/filecoin-project/go-filecoin/block"
)

var log = logging.Logger("sync.dispatch")

var errEmptyPop = errors.New("pop on empty targetQueue")

// This is the size of the channel buffer used for receiving sync requests from
// producers.
const productionBufferSize = 5

// syncer is the interface of the logic syncing incoming chains
type syncer interface {
	HandleNewTipSet(context.Context, *block.ChainInfo, bool) error
}

// NewDispatcher creates a new syncing dispatcher.
func NewDispatcher(catchupSyncer syncer) *Dispatcher {
	return &Dispatcher{
		targetQ:             NewTargetQueue(),
		catchupSyncer:       catchupSyncer,
		production:          make(chan SyncRequest, productionBufferSize),
		control:             make(chan interface{}),
		onProcessedCountCbs: make([]onProcessedCountCb, 0),
	}
}

// OnProcessedCountMessage registers a user callback to be fired once the
// count of messages is processed.
type onProcessedCountCb struct {
	cb       func()
	n, start uint64
}

// Dispatcher executes syncing requests
type Dispatcher struct {
	// The following fields handle syncer request dispatch
	// The dispatcher maintains a targeting system for determining the
	// current best syncing target
	// targetQ is a priority queue of target tipsets
	targetQ *TargetQueue
	// production synchronizes adding sync requests to the dispatcher.
	// The dispatcher relies on a single reader pulling from this.  Don't add
	// another reader without care.
	production chan SyncRequest
	// catchupSyncer is used for dispatching sync requests for chain heads
	// during the CHAIN_CATCHUP mode of operation
	catchupSyncer syncer

	// The following fields allow outside processes to issue commands to
	// the dispatcher, for example to synchronize with it or inspect state
	onProcessedCountCbs []onProcessedCountCb
	control             chan interface{}

	// The following fields are diagnostics maintained by the dispatcher
	// syncReqCount tracks the total number of sync requests dispatched to
	// syncers.  We do not handle overflows.
	syncReqCount uint64
}

// ReceiveHello handles chain information from bootstrap peers.
func (d *Dispatcher) ReceiveHello(ci *block.ChainInfo) error { return d.receive(ci) }

// ReceiveOwnBlock handles chain info from a node's own mining system
func (d *Dispatcher) ReceiveOwnBlock(ci *block.ChainInfo) error { return d.receive(ci) }

// ReceiveGossipBlock handles chain info from new blocks sent on pubsub
func (d *Dispatcher) ReceiveGossipBlock(ci *block.ChainInfo) error { return d.receive(ci) }

func (d *Dispatcher) receive(ci *block.ChainInfo) error {
	d.production <- SyncRequest{ChainInfo: *ci}
	return nil
}

// Start launches the business logic for the syncing subsystem.
// It reads syncing requests from the target queue and dispatches them to the
// appropriate syncer.
func (d *Dispatcher) Start(syncingCtx context.Context) {
	go func() {
		var last *SyncRequest
		for {
			// Begin by firing off any callbacks that are ready			
			d.maybeFireCbs()
			// Handle shutdown
			select {
			case <-syncingCtx.Done():
				return				
			default:
			}

			// Handle control signals
			select {
			case ctrl := <-d.control:
				d.receiveCtrl(ctrl)
			default:
			}

			// Handle production
			var produced []SyncRequest
			if last != nil {
				produced = append(produced, *last)
				last = nil
			}
			select {
			case first := <-d.production:
				produced = append(produced, first)
				produced = append(produced, d.drainProduced()...)
			default:
			}
			// Sort new requests
			for _, syncReq := range produced {
				d.targetQ.Push(syncReq)
			}

			// Check for work to do
			syncReq, popped := d.targetQ.Pop()
			if popped {
				// Do work from work queue
				err := d.catchupSyncer.HandleNewTipSet(syncingCtx, &syncReq.ChainInfo, true)
				if err != nil {
					log.Info("sync request could not complete: %s", err)
				}
				d.syncReqCount++				
			} else {
				// No work left, block until something shows up
				select {
				case extra := <-d.production:
					last = &extra
				}
			}
		}
	}()
}

// drainProduced reads all values within the production channel buffer at time
// of calling without blocking.  It reads at most productionBufferSize.
func (d *Dispatcher) drainProduced() []SyncRequest {
	// drain channel. Note this relies on a single reader of the production
	// channel to avoid blocking.
	n := len(d.production)
	var produced []SyncRequest
	for i := 0; i < n; i++ {
		next := <-d.production
		produced = append(produced, next)
	}
	return produced
}

// RegisterOnProcessedCount registers a callback on the dispatcher that
// will fire after processing the provided number of sync requests.
func (d *Dispatcher) RegisterOnProcessedCount(count uint64, cb func()) {
	d.control <- onProcessedCountCb{n: count, cb: cb}
}

// receiveCtrl takes a control message, determines its type, and performs the
// specified action. 
func (d *Dispatcher) receiveCtrl(i interface{}) {
	// Using interfaces is overkill for now but is the way to make this
	// extensible.  (Delete this comment if we add more than one control)
	switch msg := i.(type) {
	case onProcessedCountCb:
		msg.start = d.syncReqCount
		d.onProcessedCountCbs = append(d.onProcessedCountCbs, msg)
	default:
		// We don't know this type, log and ignore
		log.Info("dispatcher control can not handle type %T", msg)
	}
}

// maybeFireCbs fires all callbacks registered on the dispatcher that should
// fire given the dispatcher's state.
func (d *Dispatcher) maybeFireCbs() {
	var removedIdxs []int
	for i, opcCb := range d.onProcessedCountCbs {
		if opcCb.start+opcCb.n == d.syncReqCount {
			removedIdxs = append(removedIdxs, i)
			opcCb.cb()
		}
	}
}

// SyncRequest tracks a logical request of the syncing subsystem to run a
// syncing job against given inputs. syncRequests are created by the
// Dispatcher by inspecting incoming hello messages from bootstrap peers
// and gossipsub block propagations.
type SyncRequest struct {
	block.ChainInfo
	// needed by internal container/heap methods for maintaining sort
	index int
}

// rawQueue orders the dispatchers syncRequests by a policy.
// The current simple policy is to order syncing requests by claimed chain
// height.
//
// rawQueue can panic so it shouldn't be used unwrapped
type rawQueue []SyncRequest

// Heavily inspired by https://golang.org/pkg/container/heap/
func (rq rawQueue) Len() int { return len(rq) }

func (rq rawQueue) Less(i, j int) bool {
	// We want Pop to give us the highest priority so we use greater than
	return rq[i].Height > rq[j].Height
}

func (rq rawQueue) Swap(i, j int) {
	rq[i], rq[j] = rq[j], rq[i]
	rq[i].index = j
	rq[j].index = i
}

func (rq *rawQueue) Push(x interface{}) {
	n := len(*rq)
	syncReq := x.(SyncRequest)
	syncReq.index = n
	*rq = append(*rq, syncReq)
}

func (rq *rawQueue) Pop() interface{} {
	old := *rq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*rq = old[0 : n-1]
	return item
}

// TargetQueue orders dispatcher syncRequests by the underlying rawQueue's
// policy.
//
// It is not threadsafe.
type TargetQueue struct {
	q         rawQueue
	targetSet map[string]struct{}
}

// NewTargetQueue returns a new target queue with an initialized rawQueue
func NewTargetQueue() *TargetQueue {
	rq := make(rawQueue, 0)
	heap.Init(&rq)
	return &TargetQueue{
		q:         rq,
		targetSet: make(map[string]struct{}),
	}
}

// Push adds a sync request to the target queue.
func (tq *TargetQueue) Push(req SyncRequest) {
	// If already in queue drop quickly
	if _, inQ := tq.targetSet[req.ChainInfo.Head.String()]; inQ {
		return
	}
	heap.Push(&tq.q, req)
	tq.targetSet[req.ChainInfo.Head.String()] = struct{}{}

	return
}

// Pop removes and returns the highest priority syncing target. If there is 
// nothing in the queue the second argument returns false
func (tq *TargetQueue) Pop() (SyncRequest, bool) {
	if tq.Len() == 0 {
		return SyncRequest{}, false
	}
	req := heap.Pop(&tq.q).(SyncRequest)
	popKey := req.ChainInfo.Head.String()
	delete(tq.targetSet, popKey)
	return req, true
}

// Len returns the number of targets in the queue.
func (tq *TargetQueue) Len() int {
	return tq.q.Len()
}
