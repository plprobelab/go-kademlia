package routing

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/util"
)

type check[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] struct {
	NodeInfo I
	Started  time.Time
}

type Include[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] struct {
	rt kad.RoutingTable[K, N]

	// checks is an index of checks in progress
	checks map[string]check[K, A, N, I]

	candidates *nodeQueue[K, A, N, I]

	// cfg is a copy of the optional configuration supplied to the Include
	cfg IncludeConfig
}

// IncludeConfig specifies optional configuration for an Include
type IncludeConfig struct {
	QueueCapacity int           // the maximum number of nodes that can be in the candidate queue
	Concurrency   int           // the maximum number of include checks that may be in progress at any one time
	Timeout       time.Duration // the time to wait before terminating a check that is not making progress
	Clock         clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *IncludeConfig) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Concurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}

	if cfg.Timeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.QueueCapacity < 1 {
		return &kaderr.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("queue size must be greater than zero"),
		}
	}

	return nil
}

// DefaultIncludeConfig returns the default configuration options for an Include.
// Options may be overridden before passing to NewInclude
func DefaultIncludeConfig() *IncludeConfig {
	return &IncludeConfig{
		Clock:         clock.New(), // use standard time
		Concurrency:   3,
		Timeout:       time.Minute,
		QueueCapacity: 128,
	}
}

func NewInclude[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]](rt kad.RoutingTable[K, N], cfg *IncludeConfig) (*Include[K, A, N, I], error) {
	if cfg == nil {
		cfg = DefaultIncludeConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Include[K, A, N, I]{
		candidates: newNodeQueue[K, A, N, I](cfg.QueueCapacity),
		cfg:        *cfg,
		rt:         rt,
		checks:     make(map[string]check[K, A, N, I], cfg.Concurrency),
	}, nil
}

// Advance advances the state of the include state machine by attempting to advance its query if running.
func (b *Include[K, A, N, I]) Advance(ctx context.Context, ev IncludeEvent) IncludeState {
	ctx, span := util.StartSpan(ctx, "Include.Advance")
	defer span.End()

	switch tev := ev.(type) {

	case *EventIncludeAddCandidate[K, A, N, I]:
		// Ignore if already running a check
		_, checking := b.checks[key.HexString(tev.NodeInfo.ID().Key())]
		if checking {
			break
		}

		// Ignore if node already in routing table
		if _, exists := b.rt.GetNode(tev.NodeInfo.ID().Key()); exists {
			break
		}

		// TODO: potentially time out a check and make room in the queue
		if !b.candidates.HasCapacity() {
			return &StateIncludeWaitingFull{}
		}
		b.candidates.Enqueue(ctx, tev.NodeInfo)

	case *EventIncludeMessageResponse[K, A, N, I]:
		ch, ok := b.checks[key.HexString(tev.NodeInfo.ID().Key())]
		if ok {
			delete(b.checks, key.HexString(tev.NodeInfo.ID().Key()))
			// require that the node responded with at least one closer node
			if tev.Response != nil && len(tev.Response.CloserNodes()) > 0 {
				if b.rt.AddNode(tev.NodeInfo.ID()) {
					return &StateIncludeRoutingUpdated[K, A, N, I]{
						NodeInfo: ch.NodeInfo,
					}
				}
			}
		}
	case *EventIncludeMessageFailure[K, A, N, I]:
		delete(b.checks, key.HexString(tev.NodeInfo.ID().Key()))

	case *EventIncludePoll:
	// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if len(b.checks) == b.cfg.Concurrency {
		if !b.candidates.HasCapacity() {
			return &StateIncludeWaitingFull{}
		}
		return &StateIncludeWaitingAtCapacity{}
	}

	candidate, ok := b.candidates.Dequeue(ctx)
	if !ok {
		// No candidate in queue
		if len(b.checks) > 0 {
			return &StateIncludeWaitingWithCapacity{}
		}
		return &StateIncludeIdle{}
	}

	b.checks[key.HexString(candidate.ID().Key())] = check[K, A, N, I]{
		NodeInfo: candidate,
		Started:  b.cfg.Clock.Now(),
	}

	// Ask the node to find itself
	return &StateIncludeFindNodeMessage[K, A, N, I]{
		NodeInfo: candidate,
	}
}

// nodeQueue is a bounded queue of unique NodeIDs
type nodeQueue[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] struct {
	capacity int
	nodes    []I
	keys     map[string]struct{}
}

func newNodeQueue[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]](capacity int) *nodeQueue[K, A, N, I] {
	return &nodeQueue[K, A, N, I]{
		capacity: capacity,
		nodes:    make([]I, 0, capacity),
		keys:     make(map[string]struct{}, capacity),
	}
}

// Enqueue adds a node to the queue. It returns true if the node was
// added and false otherwise.
func (q *nodeQueue[K, A, N, I]) Enqueue(ctx context.Context, n I) bool {
	if len(q.nodes) == q.capacity {
		return false
	}

	if _, exists := q.keys[key.HexString(n.ID().Key())]; exists {
		return false
	}

	q.nodes = append(q.nodes, n)
	q.keys[key.HexString(n.ID().Key())] = struct{}{}
	return true
}

// Dequeue reads an node from the queue. It returns the node and a true value
// if a node was read or nil and false if no node was read.
func (q *nodeQueue[K, A, N, I]) Dequeue(ctx context.Context) (I, bool) {
	if len(q.nodes) == 0 {
		var v I
		return v, false
	}

	var n I
	n, q.nodes = q.nodes[0], q.nodes[1:]
	delete(q.keys, key.HexString(n.ID().Key()))

	return n, true
}

func (q *nodeQueue[K, A, N, I]) HasCapacity() bool {
	return len(q.nodes) < q.capacity
}

// IncludeState is the state of a include.
type IncludeState interface {
	includeState()
}

// StateIncludeFindNodeMessage indicates that the include subsystem is waiting to send a find node message a node.
// A find node message should be sent to the node, with the target being the node's key.
type StateIncludeFindNodeMessage[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] struct {
	NodeInfo I // the node to send the mssage to
}

// StateIncludeIdle indicates that the include is not running its query.
type StateIncludeIdle struct{}

// StateIncludeWaitingAtCapacity indicates that the include subsystem is waiting for responses for checks and
// that the maximum number of concurrent checks has been reached.
type StateIncludeWaitingAtCapacity struct{}

// StateIncludeWaitingWithCapacity indicates that the include subsystem is waiting for responses for checks
// but has capacity to perform more.
type StateIncludeWaitingWithCapacity struct{}

// StateIncludeWaitingFull indicates that the include subsystem is waiting for responses for checks and
// that the maximum number of queued candidates has been reached.
type StateIncludeWaitingFull struct{}

// StateIncludeRoutingUpdated indicates the routing table has been updated with a new node.
type StateIncludeRoutingUpdated[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] struct {
	NodeInfo I
}

// includeState() ensures that only Include states can be assigned to an IncludeState.
func (*StateIncludeFindNodeMessage[K, A, N, I]) includeState() {}
func (*StateIncludeIdle) includeState()                        {}
func (*StateIncludeWaitingAtCapacity) includeState()           {}
func (*StateIncludeWaitingWithCapacity) includeState()         {}
func (*StateIncludeWaitingFull) includeState()                 {}
func (*StateIncludeRoutingUpdated[K, A, N, I]) includeState()  {}

// IncludeEvent is an event intended to advance the state of a include.
type IncludeEvent interface {
	includeEvent()
}

// EventIncludePoll is an event that signals the include that it can perform housekeeping work such as time out queries.
type EventIncludePoll struct{}

// EventIncludeAddCandidate notifies that a node should be added to the candidate list
type EventIncludeAddCandidate[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] struct {
	NodeInfo I // the candidate node
}

// EventIncludeMessageResponse notifies a include that a sent message has received a successful response.
type EventIncludeMessageResponse[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] struct {
	NodeInfo I                        // the node the message was sent to
	Response kad.Response[K, A, N, I] // the message response sent by the node
}

// EventIncludeMessageFailure notifiesa include that an attempt to send a message has failed.
type EventIncludeMessageFailure[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] struct {
	NodeInfo I     // the node the message was sent to
	Error    error // the error that caused the failure, if any
}

// includeEvent() ensures that only Include events can be assigned to the IncludeEvent interface.
func (*EventIncludePoll) includeEvent()                        {}
func (*EventIncludeAddCandidate[K, A, N, I]) includeEvent()    {}
func (*EventIncludeMessageResponse[K, A, N, I]) includeEvent() {}
func (*EventIncludeMessageFailure[K, A, N, I]) includeEvent()  {}
