package coord

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/plprobelab/go-kademlia/events/action"
	"github.com/plprobelab/go-kademlia/events/planner"
	sp "github.com/plprobelab/go-kademlia/events/planner/simpleplanner"
	"github.com/plprobelab/go-kademlia/events/queue"
	"github.com/plprobelab/go-kademlia/events/queue/chanqueue"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/network/endpoint"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/util"
)

// A Coordinator coordinates the state machines that comprise a Kademlia DHT
// Currently this is only queries but will expand to include other state machines such as routing table refresh,
// and reproviding.
type Coordinator[K kad.Key[K], A kad.Address[A]] struct {
	// self is the node id of the system the coordinator is running on
	self kad.NodeID[K]

	// cfg is a copy of the optional configuration supplied to the coordinator
	cfg Config

	qp *query.Pool[K, A]

	// rt is the routing table used to look up nodes by distance
	rt kad.RoutingTable[K, kad.NodeID[K]]

	// ep is the message endpoint used to send requests
	ep endpoint.Endpoint[K, A]

	peerstoreTTL time.Duration

	queue   queue.EventQueue
	planner planner.AwareActionPlanner

	outboundEvents chan KademliaEvent
}

const DefaultChanqueueCapacity = 1024

type Config struct {
	// TODO: review if this is needed here
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

func NewCoordinator[K kad.Key[K], A kad.Address[A]](self kad.NodeID[K], ep endpoint.Endpoint[K, A], rt kad.RoutingTable[K, kad.NodeID[K]], cfg *Config) (*Coordinator[K, A], error) {
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

	qp, err := query.NewPool[K, A](self, qpCfg)
	if err != nil {
		return nil, fmt.Errorf("query pool: %w", err)
	}
	return &Coordinator[K, A]{
		self:           self,
		cfg:            *cfg,
		ep:             ep,
		rt:             rt,
		qp:             qp,
		outboundEvents: make(chan KademliaEvent, 20),
		queue:          chanqueue.NewChanQueue(DefaultChanqueueCapacity),
		planner:        sp.NewSimplePlanner(cfg.Clock),
	}, nil
}

func (c *Coordinator[K, A]) Events() <-chan KademliaEvent {
	return c.outboundEvents
}

func (c *Coordinator[K, A]) handleInboundEvent(ctx context.Context, ev interface{}) {
	ctx, span := util.StartSpan(ctx, "Coordinator.handleInboundEvent")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("EventType", fmt.Sprintf("%T", ev)))
	}

	switch tev := ev.(type) {
	case *eventUnroutablePeer[K]:
		// TODO: remove from routing table
		c.dispatchQueryPoolEvent(ctx, nil)

	case *eventMessageFailed[K]:
		qev := &query.EventPoolMessageFailure[K]{
			QueryID: tev.QueryID,
			NodeID:  tev.NodeID,
			Error:   tev.Error,
		}

		c.dispatchQueryPoolEvent(ctx, qev)

	case *eventMessageResponse[K, A]:
		if tev.Response != nil {
			candidates := tev.Response.CloserNodes()
			if len(candidates) > 0 {
				// ignore error here
				c.AddNodes(ctx, candidates)
			}
		}

		// notify caller so they have chance to stop query
		c.outboundEvents <- &KademliaOutboundQueryProgressedEvent[K, A]{
			NodeID:   tev.NodeID,
			QueryID:  tev.QueryID,
			Response: tev.Response,
			Stats:    tev.Stats,
		}

		qev := &query.EventPoolMessageResponse[K, A]{
			QueryID:  tev.QueryID,
			NodeID:   tev.NodeID,
			Response: tev.Response,
		}
		c.dispatchQueryPoolEvent(ctx, qev)
	case *eventAddQuery[K, A]:
		qev := &query.EventPoolAddQuery[K, A]{
			QueryID:           tev.QueryID,
			Target:            tev.Target,
			ProtocolID:        tev.ProtocolID,
			Message:           tev.Message,
			KnownClosestNodes: tev.KnownClosestPeers,
		}
		c.dispatchQueryPoolEvent(ctx, qev)
	case *eventStopQuery[K]:
		qev := &query.EventPoolStopQuery{
			QueryID: tev.QueryID,
		}
		c.dispatchQueryPoolEvent(ctx, qev)
	case *eventPoll:
		c.dispatchQueryPoolEvent(ctx, nil) // TODO EventPoolPoll
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}
}

func (c *Coordinator[K, A]) dispatchQueryPoolEvent(ctx context.Context, ev query.PoolEvent) {
	ctx, span := util.StartSpan(ctx, "Coordinator.dispatchQueryPoolEvent")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("EventType", fmt.Sprintf("%T", ev)))
	}

	// attempt to advance the query state machine
	state := c.qp.Advance(ctx, ev)
	switch st := state.(type) {
	case *query.StatePoolQueryMessage[K, A]:
		span.SetAttributes(attribute.String("QueryID", string(st.QueryID)))
		c.attemptSendMessage(ctx, st.ProtocolID, st.NodeID, st.Message, st.QueryID, st.Stats)
	case *query.StatePoolWaitingAtCapacity:
		// TODO
	case *query.StatePoolWaitingWithCapacity:
		// TODO
	case *query.StatePoolQueryFinished:
		span.SetAttributes(attribute.String("QueryID", string(st.QueryID)))
		c.outboundEvents <- &KademliaOutboundQueryFinishedEvent{
			QueryID: st.QueryID,
			Stats:   st.Stats,
		}

		// TODO
	case *query.StatePoolQueryTimeout:
		// TODO
	case *query.StatePoolIdle:
		// TODO
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}
}

func (c *Coordinator[K, A]) attemptSendMessage(ctx context.Context, protoID address.ProtocolID, to kad.NodeID[K], msg kad.Request[K, A], queryID query.QueryID, stats query.QueryStats) {
	ctx, span := util.StartSpan(ctx, "Coordinator.attemptSendMessage")
	defer span.End()

	onSendError := func(ctx context.Context, err error) {
		if errors.Is(err, endpoint.ErrCannotConnect) {
			// here we can notify that the peer is unroutable, which would feed into peerstore and routing table
			c.queue.Enqueue(ctx, &eventUnroutablePeer[K]{
				NodeID: to,
			})
			return
		}
		c.queue.Enqueue(ctx, &eventMessageFailed[K]{
			NodeID:  to,
			QueryID: queryID,
			Stats:   stats,
			Error:   err,
		})
	}

	onMessageResponse := func(ctx context.Context, resp kad.Response[K, A], err error) {
		if err != nil {
			onSendError(ctx, err)
			return
		}
		c.queue.Enqueue(ctx, &eventMessageResponse[K, A]{
			NodeID:   to,
			QueryID:  queryID,
			Response: resp,
			Stats:    stats,
		})
	}

	err := c.ep.SendRequestHandleResponse(ctx, protoID, to, msg, msg.EmptyResponse(), 0, onMessageResponse)
	if err != nil {
		onSendError(ctx, err)
	}
}

func (c *Coordinator[K, A]) StartQuery(ctx context.Context, queryID query.QueryID, protocolID address.ProtocolID, msg kad.Request[K, A]) error {
	ctx, span := util.StartSpan(ctx, "Coordinator.StartQuery",
		trace.WithAttributes(attribute.String("QueryID", string(queryID))))
	defer span.End()

	knownClosestPeers := c.rt.NearestNodes(msg.Target(), 20)

	ev := &eventAddQuery[K, A]{
		QueryID:           queryID,
		Target:            msg.Target(),
		ProtocolID:        protocolID,
		Message:           msg,
		KnownClosestPeers: knownClosestPeers,
	}

	c.queue.Enqueue(ctx, ev)
	// c.inboundEvents <- ev

	return nil
}

func (c *Coordinator[K, A]) StopQuery(ctx context.Context, queryID query.QueryID) error {
	ctx, span := util.StartSpan(ctx, "Coordinator.StopQuery",
		trace.WithAttributes(attribute.String("QueryID", string(queryID))))
	defer span.End()
	ev := &eventStopQuery[K]{
		QueryID: queryID,
	}

	c.queue.Enqueue(ctx, ev)
	// c.inboundEvents <- ev
	return nil
}

// AddNodes suggests new DHT nodes and their associated addresses to be added to the routing table.
// If the routing table is been updated as a result of this operation a KademliaRoutingUpdatedEvent event is emitted.
func (c *Coordinator[K, A]) AddNodes(ctx context.Context, infos []kad.NodeInfo[K, A]) error {
	ctx, span := util.StartSpan(ctx, "Coordinator.AddNodes")
	defer span.End()
	for _, info := range infos {
		if key.Equal(info.ID().Key(), c.self.Key()) {
			continue
		}
		isNew := c.rt.AddNode(info.ID())
		c.ep.MaybeAddToPeerstore(ctx, info, c.peerstoreTTL)

		if isNew {
			c.outboundEvents <- &KademliaRoutingUpdatedEvent[K, A]{
				NodeInfo: info,
			}
		}
	}

	return nil
}

// Kademlia events emitted by the Coordinator, intended for consumption by clients of the package

type KademliaEvent interface {
	kademliaEvent()
}

// KademliaOutboundQueryProgressedEvent is emitted by the coordinator when a query has received a
// response from a node.
type KademliaOutboundQueryProgressedEvent[K kad.Key[K], A kad.Address[A]] struct {
	QueryID  query.QueryID
	NodeID   kad.NodeID[K]
	Response kad.Response[K, A]
	Stats    query.QueryStats
}

// KademliaOutboundQueryFinishedEvent is emitted by the coordinator when a query has finished, either through
// running to completion or by being canceled.
type KademliaOutboundQueryFinishedEvent struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

// KademliaRoutingUpdatedEvent is emitted by the coordinator when a new node has been added to the routing table.
type KademliaRoutingUpdatedEvent[K kad.Key[K], A kad.Address[A]] struct {
	NodeInfo kad.NodeInfo[K, A]
}

type KademliaUnroutablePeerEvent[K kad.Key[K]] struct{}

type KademliaRoutablePeerEvent[K kad.Key[K]] struct{}

// kademliaEvent() ensures that only Kademlia events can be assigned to a KademliaEvent.
func (*KademliaRoutingUpdatedEvent[K, A]) kademliaEvent()          {}
func (*KademliaOutboundQueryProgressedEvent[K, A]) kademliaEvent() {}
func (*KademliaUnroutablePeerEvent[K]) kademliaEvent()             {}
func (*KademliaRoutablePeerEvent[K]) kademliaEvent()               {}
func (*KademliaOutboundQueryFinishedEvent) kademliaEvent()         {}

// Internal events for the Coordiinator

type coordinatorInternalEvent interface {
	coordinatorInternalEvent()
	Run(ctx context.Context)
}

type eventUnroutablePeer[K kad.Key[K]] struct {
	NodeID kad.NodeID[K]
}

type eventMessageFailed[K kad.Key[K]] struct {
	NodeID  kad.NodeID[K]    // the node the message was sent to
	QueryID query.QueryID    // the id of the query that sent the message
	Stats   query.QueryStats // stats for the query sending the message
	Error   error            // the error that caused the failure, if any
}

type eventMessageResponse[K kad.Key[K], A kad.Address[A]] struct {
	NodeID   kad.NodeID[K]      // the node the message was sent to
	QueryID  query.QueryID      // the id of the query that sent the message
	Response kad.Response[K, A] // the message response sent by the node
	Stats    query.QueryStats   // stats for the query sending the message
}

type eventAddQuery[K kad.Key[K], A kad.Address[A]] struct {
	QueryID           query.QueryID
	Target            K
	ProtocolID        address.ProtocolID
	Message           kad.Request[K, A]
	KnownClosestPeers []kad.NodeID[K]
}

type eventStopQuery[K kad.Key[K]] struct {
	QueryID query.QueryID
}

type eventPoll struct{}

// coordinatorInternalEvent() ensures that only an internal coordinator event can be assigned to the coordinatorInternalEvent interface.
func (*eventUnroutablePeer[K]) coordinatorInternalEvent()     {}
func (*eventMessageFailed[K]) coordinatorInternalEvent()      {}
func (*eventMessageResponse[K, A]) coordinatorInternalEvent() {}
func (*eventAddQuery[K, A]) coordinatorInternalEvent()        {}
func (*eventStopQuery[K]) coordinatorInternalEvent()          {}
func (*eventPoll) coordinatorInternalEvent()                  {}

func (*eventUnroutablePeer[K]) Run(context.Context)     {}
func (*eventMessageFailed[K]) Run(context.Context)      {}
func (*eventMessageResponse[K, A]) Run(context.Context) {}
func (*eventAddQuery[K, A]) Run(context.Context)        {}
func (*eventStopQuery[K]) Run(context.Context)          {}
func (*eventPoll) Run(context.Context)                  {}

// var _ scheduler.Scheduler = (*Coordinator[key.Key8])(nil)
func (c *Coordinator[K, A]) Clock() clock.Clock {
	return c.cfg.Clock
}

func (c *Coordinator[K, A]) EnqueueAction(ctx context.Context, a action.Action) {
	ctx, span := util.StartSpan(ctx, "Coordinator.EnqueueAction")
	defer span.End()
	c.queue.Enqueue(ctx, a)
}

func (c *Coordinator[K, A]) ScheduleAction(ctx context.Context, t time.Time, a action.Action) planner.PlannedAction {
	ctx, span := util.StartSpan(ctx, "Coordinator.ScheduleAction")
	defer span.End()
	if c.cfg.Clock.Now().After(t) {
		c.EnqueueAction(ctx, a)
		return nil
	}
	return c.planner.ScheduleAction(ctx, t, a)
}

func (c *Coordinator[K, A]) RemovePlannedAction(ctx context.Context, a planner.PlannedAction) bool {
	ctx, span := util.StartSpan(ctx, "Coordinator.RemovePlannedAction")
	defer span.End()
	return c.planner.RemoveAction(ctx, a)
}

func (c *Coordinator[K, A]) RunOne(ctx context.Context) bool {
	ctx, span := util.StartSpan(ctx, "Coordinator.RunOne")
	defer span.End()
	c.moveOverdueActions(ctx)
	if a := c.queue.Dequeue(ctx); a != nil {
		c.handleInboundEvent(ctx, a)
		return true
	}
	// c.handleInboundEvent(ctx, &eventPoll{})
	return false
}

// moveOverdueActions moves all overdue actions from the planner to the queue.
func (c *Coordinator[K, A]) moveOverdueActions(ctx context.Context) {
	overdue := c.planner.PopOverdueActions(ctx)

	queue.EnqueueMany(ctx, c.queue, overdue)
}

// NextActionTime returns the time of the next action to run, or the current
// time if there are actions to be run in the queue, or util.MaxTime if there
// are no scheduled to run.
func (c *Coordinator[K, A]) NextActionTime(ctx context.Context) time.Time {
	c.moveOverdueActions(ctx)
	nextScheduled := c.planner.NextActionTime(ctx)

	if !queue.Empty(c.queue) {
		return c.cfg.Clock.Now()
	}
	return nextScheduled
}
