package coord

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/plprobelab/go-kademlia/event"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/network/endpoint"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/routing"
	"github.com/plprobelab/go-kademlia/util"
)

// A StateMachine progresses through a set of states in response to transition events.
type StateMachine[S any, E any] interface {
	// Advance advances the state of the state machine.
	Advance(context.Context, E) S
}

type StateMachineAction[E any] struct {
	Event     E
	AdvanceFn func(context.Context, E)
}

func (a *StateMachineAction[E]) Run(ctx context.Context) {
	a.AdvanceFn(ctx, a.Event)
}

func (a *StateMachineAction[E]) String() string {
	return fmt.Sprintf("StateMachineAction[%T]", a.Event)
}

// FindNodeRequestFunc is a function that creates a request to find the supplied node id
// TODO: consider this being a first class method of the Endpoint
type FindNodeRequestFunc[K kad.Key[K], N kad.NodeID[K], I any] func(kad.NodeID[K]) (address.ProtocolID, kad.Request[K, N, I])

// ActionQueue accepts actions and queues them for later execution
type ActionQueue interface {
	EnqueueAction(context.Context, event.Action)
}

// A Coordinator coordinates the state machines that comprise a Kademlia DHT
// It is only one possible configuration of the DHT components, others are possible.
// Currently this is only queries and bootstrapping but will expand to include other state machines such as
// routing table refresh, and reproviding.
type Coordinator[K kad.Key[K], N kad.NodeID[K], I any] struct {
	// self is the node id of the system the coordinator is running on
	self kad.NodeID[K]

	// cfg is a copy of the optional configuration supplied to the coordinator
	cfg Config

	// pool is the query pool state machine, responsible for running user-submitted queries
	pool StateMachine[query.PoolState, query.PoolEvent]

	// bootstrap is the bootstrap state machine, responsible for bootstrapping the routing table
	bootstrap StateMachine[routing.BootstrapState, routing.BootstrapEvent]

	// include is the include state machine, responsible for including candidate nodes into the routing table
	include StateMachine[routing.IncludeState, routing.IncludeEvent]

	// rt is the routing table used to look up nodes by distance
	rt kad.RoutingTable[K, N]

	// ep is the message endpoint used to send requests
	ep endpoint.Endpoint[K, N, I]

	// findNodeFn is a function that creates a find node request that may be understod by the endpoint
	// TODO: thiis should be a function of the endpoint
	findNodeFn FindNodeRequestFunc[K, N, I]

	sched ActionQueue

	outboundEvents chan KademliaEvent
}

const DefaultChanqueueCapacity = 1024

type Config struct {
	PeerstoreTTL time.Duration // duration for which a peer is kept in the peerstore

	Clock clock.Clock // a clock that may replaced by a mock when testing

	QueryConcurrency int           // the maximum number of queries that may be waiting for message responses at any one time
	QueryTimeout     time.Duration // the time to wait before terminating a query that is not making progress

	RequestConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout     time.Duration // the timeout queries should use for contacting a single node
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *Config) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.QueryConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("query concurrency must be greater than zero"),
		}
	}
	if cfg.QueryTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("query timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}
	return nil
}

func DefaultConfig() *Config {
	return &Config{
		Clock:              clock.New(), // use standard time
		PeerstoreTTL:       10 * time.Minute,
		QueryConcurrency:   3,
		QueryTimeout:       5 * time.Minute,
		RequestConcurrency: 3,
		RequestTimeout:     time.Minute,
	}
}

func NewCoordinator[K kad.Key[K], N kad.NodeID[K], I any](self N, ep endpoint.Endpoint[K, N, I], fn FindNodeRequestFunc[K, N, I], rt kad.RoutingTable[K, N], sched ActionQueue, cfg *Config) (*Coordinator[K, N, I], error) {
	if cfg == nil {
		cfg = DefaultConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	qpCfg := query.DefaultPoolConfig()
	qpCfg.Clock = cfg.Clock
	qpCfg.Concurrency = cfg.QueryConcurrency
	qpCfg.Timeout = cfg.QueryTimeout
	qpCfg.QueryConcurrency = cfg.RequestConcurrency
	qpCfg.RequestTimeout = cfg.RequestTimeout

	qp, err := query.NewPool[K, N, I](self, qpCfg)
	if err != nil {
		return nil, fmt.Errorf("query pool: %w", err)
	}

	bootstrapCfg := routing.DefaultBootstrapConfig()
	bootstrapCfg.Clock = cfg.Clock
	bootstrapCfg.Timeout = cfg.QueryTimeout
	bootstrapCfg.RequestConcurrency = cfg.RequestConcurrency
	bootstrapCfg.RequestTimeout = cfg.RequestTimeout

	bootstrap, err := routing.NewBootstrap[K, N, I](self, bootstrapCfg)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}

	includeCfg := routing.DefaultIncludeConfig()
	includeCfg.Clock = cfg.Clock
	includeCfg.Timeout = cfg.QueryTimeout

	// TODO: expose config
	// includeCfg.QueueCapacity = cfg.IncludeQueueCapacity
	// includeCfg.Concurrency = cfg.IncludeConcurrency
	// includeCfg.Timeout = cfg.IncludeTimeout

	include, err := routing.NewInclude[K, N, I](rt, includeCfg)
	if err != nil {
		return nil, fmt.Errorf("include: %w", err)
	}
	return &Coordinator[K, N, I]{
		self:           self,
		cfg:            *cfg,
		ep:             ep,
		findNodeFn:     fn,
		rt:             rt,
		pool:           qp,
		bootstrap:      bootstrap,
		include:        include,
		outboundEvents: make(chan KademliaEvent, 20),
		sched:          sched,
	}, nil
}

func (c *Coordinator[K, N, I]) Events() <-chan KademliaEvent {
	return c.outboundEvents
}

func (c *Coordinator[K, N, I]) scheduleBootstrapEvent(ctx context.Context, ev routing.BootstrapEvent) {
	// TODO: enqueue with higher priority when we have priority queues
	c.sched.EnqueueAction(ctx, &StateMachineAction[routing.BootstrapEvent]{
		Event:     ev,
		AdvanceFn: c.advanceBootstrap,
	})
}

func (c *Coordinator[K, N, I]) advanceBootstrap(ctx context.Context, ev routing.BootstrapEvent) {
	bstate := c.bootstrap.Advance(ctx, ev)
	switch st := bstate.(type) {
	case *routing.StateBootstrapMessage[K, N, I]:
		c.sendBootstrapFindNode(ctx, st.NodeID, st.QueryID, st.Stats)

	case *routing.StateBootstrapWaiting:
		// bootstrap waiting for a message response, nothing to do
	case *routing.StateBootstrapFinished:
		c.outboundEvents <- &KademliaBootstrapFinishedEvent{
			Stats: st.Stats,
		}
	case *routing.StateBootstrapIdle:
		// bootstrap not running, nothing to do
	default:
		panic(fmt.Sprintf("unexpected bootstrap state: %T", st))
	}
}

func (c *Coordinator[K, N, I]) scheduleIncludeEvent(ctx context.Context, ev routing.IncludeEvent) {
	c.sched.EnqueueAction(ctx, &StateMachineAction[routing.IncludeEvent]{
		Event:     ev,
		AdvanceFn: c.advanceInclude,
	})
}

func (c *Coordinator[K, N, I]) advanceInclude(ctx context.Context, ev routing.IncludeEvent) {
	// Attempt to advance the include state machine so candidate nodes
	// are added to the routing table
	istate := c.include.Advance(ctx, ev)
	switch st := istate.(type) {
	case *routing.StateIncludeFindNodeMessage[K, N, I]:
		// include wants to send a find node message to a node
		c.sendIncludeFindNode(ctx, st.NodeInfo)
	case *routing.StateIncludeRoutingUpdated[K, N, I]:
		// a node has been included in the routing table
		c.outboundEvents <- &KademliaRoutingUpdatedEvent[K, N, I]{
			NodeInfo: st.NodeInfo,
		}
	case *routing.StateIncludeWaitingAtCapacity:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeWaitingFull:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeIdle:
		// nothing to do except wait for message response or timeout
	default:
		panic(fmt.Sprintf("unexpected include state: %T", st))
	}
}

func (c *Coordinator[K, N, I]) schedulePoolEvent(ctx context.Context, ev query.PoolEvent) {
	c.sched.EnqueueAction(ctx, &StateMachineAction[query.PoolEvent]{
		Event:     ev,
		AdvanceFn: c.advancePool,
	})
}

func (c *Coordinator[K, N, I]) advancePool(ctx context.Context, ev query.PoolEvent) {
	state := c.pool.Advance(ctx, ev)
	switch st := state.(type) {
	case *query.StatePoolQueryMessage[K, N, I]:
		c.sendQueryMessage(ctx, st.ProtocolID, st.NodeID, st.Message, st.QueryID, st.Stats)
	case *query.StatePoolWaitingAtCapacity:
		// nothing to do except wait for message response or timeout
	case *query.StatePoolWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
	case *query.StatePoolQueryFinished:
		c.outboundEvents <- &KademliaOutboundQueryFinishedEvent{
			QueryID: st.QueryID,
			Stats:   st.Stats,
		}
	case *query.StatePoolQueryTimeout:
		// TODO
	case *query.StatePoolIdle:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
	}
}

func (c *Coordinator[K, N, I]) sendQueryMessage(ctx context.Context, protoID address.ProtocolID, to N, msg kad.Request[K, N, I], queryID query.QueryID, stats query.QueryStats) {
	ctx, span := util.StartSpan(ctx, "Coordinator.sendQueryMessage")
	defer span.End()

	onSendError := func(ctx context.Context, err error) {
		if errors.Is(err, endpoint.ErrCannotConnect) {
			// here we can notify that the peer is unroutable, which would feed into peerstore and routing table
			// TODO: remove from routing table
			return
		}

		c.advancePool(ctx, &query.EventPoolMessageFailure[K, N]{
			NodeID:  to,
			QueryID: queryID,
			Error:   err,
		})
	}

	onMessageResponse := func(ctx context.Context, resp kad.Response[K, N, I], err error) {
		if err != nil {
			onSendError(ctx, err)
			return
		}

		if resp != nil {
			candidates := resp.CloserNodes()
			if len(candidates) > 0 {
				// ignore error here
				c.AddNodes(ctx, candidates)
			}
		}

		// notify caller so they have chance to stop query
		c.outboundEvents <- &KademliaOutboundQueryProgressedEvent[K, N, I]{
			NodeID:   to,
			QueryID:  queryID,
			Response: resp,
			Stats:    stats,
		}

		c.advancePool(ctx, &query.EventPoolMessageResponse[K, N, I]{
			NodeID:   to,
			QueryID:  queryID,
			Response: resp,
		})
	}

	err := c.ep.SendRequestHandleResponse(ctx, protoID, to, msg, msg.EmptyResponse(), 0, onMessageResponse)
	if err != nil {
		onSendError(ctx, err)
	}
}

func (c *Coordinator[K, N, I]) sendBootstrapFindNode(ctx context.Context, to N, queryID query.QueryID, stats query.QueryStats) {
	ctx, span := util.StartSpan(ctx, "Coordinator.sendBootstrapFindNode")
	defer span.End()

	onSendError := func(ctx context.Context, err error) {
		if errors.Is(err, endpoint.ErrCannotConnect) {
			// here we can notify that the peer is unroutable, which would feed into peerstore and routing table
			// TODO: remove from routing table
			return
		}

		c.advanceBootstrap(ctx, &routing.EventBootstrapMessageFailure[K]{
			NodeID: to,
			Error:  err,
		})
	}

	onMessageResponse := func(ctx context.Context, resp kad.Response[K, N, I], err error) {
		if err != nil {
			onSendError(ctx, err)
			return
		}

		if resp != nil {
			candidates := resp.CloserNodes()
			if len(candidates) > 0 {
				// ignore error here
				c.AddNodes(ctx, candidates)
			}
		}

		// notify caller so they have chance to stop query
		c.outboundEvents <- &KademliaOutboundQueryProgressedEvent[K, N, I]{
			NodeID:   to,
			QueryID:  queryID,
			Response: resp,
			Stats:    stats,
		}

		c.advanceBootstrap(ctx, &routing.EventBootstrapMessageResponse[K, N, I]{
			NodeID:   to,
			Response: resp,
		})
	}

	protoID, msg := c.findNodeFn(c.self)
	err := c.ep.SendRequestHandleResponse(ctx, protoID, to, msg, msg.EmptyResponse(), 0, onMessageResponse)
	if err != nil {
		onSendError(ctx, err)
	}
}

func (c *Coordinator[K, N, I]) sendIncludeFindNode(ctx context.Context, to kad.NodeInfo[K, N, I]) {
	ctx, span := util.StartSpan(ctx, "Coordinator.sendIncludeFindNode")
	defer span.End()

	onSendError := func(ctx context.Context, err error) {
		if errors.Is(err, endpoint.ErrCannotConnect) {
			// here we can notify that the peer is unroutable, which would feed into peerstore and routing table
			// TODO: remove from routing table
			return
		}

		c.advanceInclude(ctx, &routing.EventIncludeMessageFailure[K, N, I]{
			NodeInfo: to,
			Error:    err,
		})
	}

	onMessageResponse := func(ctx context.Context, resp kad.Response[K, N, I], err error) {
		if err != nil {
			onSendError(ctx, err)
			return
		}

		c.advanceInclude(ctx, &routing.EventIncludeMessageResponse[K, N, I]{
			NodeInfo: to,
			Response: resp,
		})
	}

	// this might be new node addressing info
	c.ep.MaybeAddToPeerstore(ctx, to, c.cfg.PeerstoreTTL)

	protoID, msg := c.findNodeFn(c.self)
	err := c.ep.SendRequestHandleResponse(ctx, protoID, to.ID, msg, msg.EmptyResponse(), 0, onMessageResponse)
	if err != nil {
		onSendError(ctx, err)
	}
}

func (c *Coordinator[K, N, I]) StartQuery(ctx context.Context, queryID query.QueryID, protocolID address.ProtocolID, msg kad.Request[K, N, I]) error {
	ctx, span := util.StartSpan(ctx, "Coordinator.StartQuery")
	defer span.End()
	knownClosestPeers := c.rt.NearestNodes(msg.Target(), 20)

	c.schedulePoolEvent(ctx, &query.EventPoolAddQuery[K, N, I]{
		QueryID:           queryID,
		Target:            msg.Target(),
		ProtocolID:        protocolID,
		Message:           msg,
		KnownClosestNodes: knownClosestPeers,
	})

	return nil
}

func (c *Coordinator[K, N, I]) StopQuery(ctx context.Context, queryID query.QueryID) error {
	ctx, span := util.StartSpan(ctx, "Coordinator.StopQuery")
	defer span.End()

	c.schedulePoolEvent(ctx, &query.EventPoolStopQuery{
		QueryID: queryID,
	})

	return nil
}

// AddNodes suggests new DHT nodes and their associated addresses to be added to the routing table.
// If the routing table is updated as a result of this operation a KademliaRoutingUpdatedEvent event is emitted.
func (c *Coordinator[K, N, I]) AddNodes(ctx context.Context, infos []kad.NodeInfo[K, N, I]) error {
	ctx, span := util.StartSpan(ctx, "Coordinator.AddNodes")
	defer span.End()
	for _, info := range infos {
		if key.Equal(info.ID.Key(), c.self.Key()) {
			// skip self
			continue
		}

		c.scheduleIncludeEvent(ctx, &routing.EventIncludeAddCandidate[K, N, I]{
			NodeInfo: info,
		})
	}

	return nil
}

// Bootstrap instructs the coordinator to begin bootstrapping the routing table.
// While bootstrap is in progress, no other queries will make progress.
func (c *Coordinator[K, N, I]) Bootstrap(ctx context.Context, seeds []N) error {
	protoID, msg := c.findNodeFn(c.self)

	c.scheduleBootstrapEvent(ctx, &routing.EventBootstrapStart[K, N, I]{
		ProtocolID:        protoID,
		Message:           msg,
		KnownClosestNodes: seeds,
	})

	return nil
}

// Kademlia events emitted by the Coordinator, intended for consumption by clients of the package

type KademliaEvent interface {
	kademliaEvent()
}

// KademliaOutboundQueryProgressedEvent is emitted by the coordinator when a query has received a
// response from a node.
type KademliaOutboundQueryProgressedEvent[K kad.Key[K], N kad.NodeID[K], I any] struct {
	QueryID  query.QueryID
	NodeID   kad.NodeID[K]
	Response kad.Response[K, N, I]
	Stats    query.QueryStats
}

// KademliaOutboundQueryFinishedEvent is emitted by the coordinator when a query has finished, either through
// running to completion or by being canceled.
type KademliaOutboundQueryFinishedEvent struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

// KademliaRoutingUpdatedEvent is emitted by the coordinator when a new node has been added to the routing table.
type KademliaRoutingUpdatedEvent[K kad.Key[K], N kad.NodeID[K], I any] struct {
	NodeInfo kad.NodeInfo[K, N, I]
}

type KademliaUnroutablePeerEvent[K kad.Key[K]] struct{}

type KademliaRoutablePeerEvent[K kad.Key[K]] struct{}

// KademliaBootstrapFinishedEvent is emitted by the coordinator when a bootstrap has finished, either through
// running to completion or by being canceled.
type KademliaBootstrapFinishedEvent struct {
	Stats query.QueryStats
}

// kademliaEvent() ensures that only Kademlia events can be assigned to a KademliaEvent.
func (*KademliaRoutingUpdatedEvent[K, N, I]) kademliaEvent()          {}
func (*KademliaOutboundQueryProgressedEvent[K, N, I]) kademliaEvent() {}
func (*KademliaUnroutablePeerEvent[K]) kademliaEvent()                {}
func (*KademliaRoutablePeerEvent[K]) kademliaEvent()                  {}
func (*KademliaOutboundQueryFinishedEvent) kademliaEvent()            {}
func (*KademliaBootstrapFinishedEvent) kademliaEvent()                {}
