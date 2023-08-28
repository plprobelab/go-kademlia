package query

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/util"
)

type Pool[K kad.Key[K], N kad.NodeID[K], I any] struct {
	// self is the node id of the system the pool is running on
	self       N
	queries    []*Query[K, N, I]
	queryIndex map[QueryID]*Query[K, N, I]

	// cfg is a copy of the optional configuration supplied to the pool
	cfg PoolConfig

	// queriesInFlight is number of queries that are waiting for message responses
	queriesInFlight int
}

// PoolConfig specifies optional configuration for a Pool
type PoolConfig struct {
	Concurrency      int           // the maximum number of queries that may be waiting for message responses at any one time
	Timeout          time.Duration // the time to wait before terminating a query that is not making progress
	Replication      int           // the 'k' parameter defined by Kademlia
	QueryConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout   time.Duration // the timeout queries should use for contacting a single node
	Clock            clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *PoolConfig) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}
	if cfg.Concurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}
	if cfg.Timeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}
	if cfg.Replication < 1 {
		return &kaderr.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("replication must be greater than zero"),
		}
	}

	if cfg.QueryConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("query concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	return nil
}

// DefaultPoolConfig returns the default configuration options for a Pool.
// Options may be overridden before passing to NewPool
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		Clock:            clock.New(), // use standard time
		Concurrency:      3,
		Timeout:          5 * time.Minute,
		Replication:      20,
		QueryConcurrency: 3,
		RequestTimeout:   time.Minute,
	}
}

func NewPool[K kad.Key[K], N kad.NodeID[K], I any](self N, cfg *PoolConfig) (*Pool[K, N, I], error) {
	if cfg == nil {
		cfg = DefaultPoolConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Pool[K, N, I]{
		self:       self,
		cfg:        *cfg,
		queries:    make([]*Query[K, N, I], 0),
		queryIndex: make(map[QueryID]*Query[K, N, I]),
	}, nil
}

// Advance advances the state of the pool by attempting to advance one of its queries
func (p *Pool[K, N, I]) Advance(ctx context.Context, ev PoolEvent) PoolState {
	ctx, span := util.StartSpan(ctx, "Pool.Advance")
	defer span.End()

	// reset the in flight counter so it can be calculated as the queries are advanced
	p.queriesInFlight = 0

	// eventQueryID keeps track of a query that was advanced via a specific event, to avoid it
	// being advanced twice
	eventQueryID := InvalidQueryID

	switch tev := ev.(type) {
	case *EventPoolAddQuery[K, N, I]:
		p.addQuery(ctx, tev.QueryID, tev.Target, tev.ProtocolID, tev.Message, tev.KnownClosestNodes)
		// TODO: return error as state
	case *EventPoolStopQuery:
		if qry, ok := p.queryIndex[tev.QueryID]; ok {
			state, terminal := p.advanceQuery(ctx, qry, &EventQueryCancel{})
			if terminal {
				return state
			}
			eventQueryID = qry.id
		}
	case *EventPoolMessageResponse[K, N, I]:
		if qry, ok := p.queryIndex[tev.QueryID]; ok {
			state, terminal := p.advanceQuery(ctx, qry, &EventQueryMessageResponse[K, N, I]{
				NodeID:   tev.NodeID,
				Response: tev.Response,
			})
			if terminal {
				return state
			}
			eventQueryID = qry.id
		}
	case *EventPoolMessageFailure[K, N]:
		if qry, ok := p.queryIndex[tev.QueryID]; ok {
			state, terminal := p.advanceQuery(ctx, qry, &EventQueryMessageFailure[K]{
				NodeID: tev.NodeID,
				Error:  tev.Error,
			})
			if terminal {
				return state
			}
			eventQueryID = qry.id
		}
	case *EventPoolPoll:
		// no event to process
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if len(p.queries) == 0 {
		return &StatePoolIdle{}
	}

	// Attempt to advance another query
	for _, qry := range p.queries {
		if eventQueryID == qry.id {
			// avoid advancing query twice
			continue
		}

		state, terminal := p.advanceQuery(ctx, qry, nil)
		if terminal {
			return state
		}

		// check if we have the maximum number of queries in flight
		if p.queriesInFlight >= p.cfg.Concurrency {
			return &StatePoolWaitingAtCapacity{}
		}
	}

	if p.queriesInFlight > 0 {
		return &StatePoolWaitingWithCapacity{}
	}

	return &StatePoolIdle{}
}

func (p *Pool[K, N, I]) advanceQuery(ctx context.Context, qry *Query[K, N, I], qev QueryEvent) (PoolState, bool) {
	state := qry.Advance(ctx, qev)
	switch st := state.(type) {
	case *StateQueryWaitingMessage[K, N, I]:
		p.queriesInFlight++
		return &StatePoolQueryMessage[K, N, I]{
			QueryID:    st.QueryID,
			Stats:      st.Stats,
			NodeID:     st.NodeID,
			ProtocolID: st.ProtocolID,
			Message:    st.Message,
		}, true
	case *StateQueryFinished:
		p.removeQuery(qry.id)
		return &StatePoolQueryFinished{
			QueryID: st.QueryID,
			Stats:   st.Stats,
		}, true
	case *StateQueryWaitingAtCapacity:
		elapsed := p.cfg.Clock.Since(qry.stats.Start)
		if elapsed > p.cfg.Timeout {
			p.removeQuery(qry.id)
			return &StatePoolQueryTimeout{
				QueryID: st.QueryID,
				Stats:   st.Stats,
			}, true
		}
		p.queriesInFlight++
	case *StateQueryWaitingWithCapacity:
		elapsed := p.cfg.Clock.Since(qry.stats.Start)
		if elapsed > p.cfg.Timeout {
			p.removeQuery(qry.id)
			return &StatePoolQueryTimeout{
				QueryID: st.QueryID,
				Stats:   st.Stats,
			}, true
		}
		p.queriesInFlight++
	}
	return nil, false
}

func (p *Pool[K, N, I]) removeQuery(queryID QueryID) {
	for i := range p.queries {
		if p.queries[i].id != queryID {
			continue
		}
		// remove from slice
		copy(p.queries[i:], p.queries[i+1:])
		p.queries[len(p.queries)-1] = nil
		p.queries = p.queries[:len(p.queries)-1]
		break
	}
	delete(p.queryIndex, queryID)
}

// addQuery adds a query to the pool, returning the new query id
// TODO: remove target argument and use msg.Target
func (p *Pool[K, N, I]) addQuery(ctx context.Context, queryID QueryID, target K, protocolID address.ProtocolID, msg kad.Request[K, N, I], knownClosestNodes []N) error {
	if _, exists := p.queryIndex[queryID]; exists {
		return fmt.Errorf("query id already in use")
	}
	iter := NewClosestNodesIter[K, N](target)

	qryCfg := DefaultQueryConfig[K]()
	qryCfg.Clock = p.cfg.Clock
	qryCfg.Concurrency = p.cfg.QueryConcurrency
	qryCfg.RequestTimeout = p.cfg.RequestTimeout

	qry, err := NewQuery[K, N, I](p.self, queryID, protocolID, msg, iter, knownClosestNodes, qryCfg)
	if err != nil {
		return fmt.Errorf("new query: %w", err)
	}

	p.queries = append(p.queries, qry)
	p.queryIndex[queryID] = qry

	return nil
}

// States

type PoolState interface {
	poolState()
}

// StatePoolIdle indicates that the pool is idle, i.e. there are no queries to process.
type StatePoolIdle struct{}

// StatePoolQueryMessage indicates that a pool query is waiting to message a node.
type StatePoolQueryMessage[K kad.Key[K], N kad.NodeID[K], I any] struct {
	QueryID    QueryID
	NodeID     N
	ProtocolID address.ProtocolID
	Message    kad.Request[K, N, I]
	Stats      QueryStats
}

// StatePoolWaitingAtCapacity indicates that at least one query is waiting for results and the pool has reached
// its maximum number of concurrent queries.
type StatePoolWaitingAtCapacity struct{}

// StatePoolWaitingWithCapacity indicates that at least one query is waiting for results but capacity to
// start more is available.
type StatePoolWaitingWithCapacity struct{}

// StatePoolQueryFinished indicates that a query has finished.
type StatePoolQueryFinished struct {
	QueryID QueryID
	Stats   QueryStats
}

// StatePoolQueryTimeout indicates that a query has timed out.
type StatePoolQueryTimeout struct {
	QueryID QueryID
	Stats   QueryStats
}

// poolState() ensures that only Pool states can be assigned to the PoolState interface.
func (*StatePoolIdle) poolState()                  {}
func (*StatePoolQueryMessage[K, N, I]) poolState() {}
func (*StatePoolWaitingAtCapacity) poolState()     {}
func (*StatePoolWaitingWithCapacity) poolState()   {}
func (*StatePoolQueryFinished) poolState()         {}
func (*StatePoolQueryTimeout) poolState()          {}

// PoolEvent is an event intended to advance the state of a pool.
type PoolEvent interface {
	poolEvent()
}

// EventPoolAddQuery is an event that attempts to add a new query
type EventPoolAddQuery[K kad.Key[K], N kad.NodeID[K], I any] struct {
	QueryID           QueryID              // the id to use for the new query
	Target            K                    // the target key for the query
	ProtocolID        address.ProtocolID   // the protocol that defines how the message should be interpreted
	Message           kad.Request[K, N, I] // the message the query should send to each node it traverses
	KnownClosestNodes []N                  // an initial set of close nodes the query should use
}

// EventPoolStopQuery notifies a pool to stop a query.
type EventPoolStopQuery struct {
	QueryID QueryID // the id of the query that should be stopped
}

// EventPoolMessageResponse notifies a pool that a query that a sent message has received a successful response.
type EventPoolMessageResponse[K kad.Key[K], N kad.NodeID[K], I any] struct {
	QueryID  QueryID               // the id of the query that sent the message
	NodeID   N                     // the node the message was sent to
	Response kad.Response[K, N, I] // the message response sent by the node
}

// EventPoolMessageFailure notifies a pool that a query that an attempt to send a message has failed.
type EventPoolMessageFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID QueryID // the id of the query that sent the message
	NodeID  N       // the node the message was sent to
	Error   error   // the error that caused the failure, if any
}

// EventPoolPoll is an event that signals the pool that it can perform housekeeping work such as time out queries.
type EventPoolPoll struct{}

// poolEvent() ensures that only Pool events can be assigned to the PoolEvent interface.
func (*EventPoolAddQuery[K, N, I]) poolEvent()        {}
func (*EventPoolStopQuery) poolEvent()                {}
func (*EventPoolMessageResponse[K, N, I]) poolEvent() {}
func (*EventPoolMessageFailure[K, N]) poolEvent()     {}
func (*EventPoolPoll) poolEvent()                     {}
