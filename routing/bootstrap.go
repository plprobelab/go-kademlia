package routing

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/util"
)

type Bootstrap[K kad.Key[K], N kad.NodeID[K], I any] struct {
	// self is the node id of the system the bootstrap is running on
	self N

	// qry is the query used by the bootstrap process
	qry *query.Query[K, N, I]

	// cfg is a copy of the optional configuration supplied to the Bootstrap
	cfg BootstrapConfig
}

// BootstrapConfig specifies optional configuration for a Bootstrap
type BootstrapConfig struct {
	Timeout            time.Duration // the time to wait before terminating a query that is not making progress
	RequestConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout     time.Duration // the timeout queries should use for contacting a single node
	Clock              clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *BootstrapConfig) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Timeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	return nil
}

// DefaultBootstrapConfig returns the default configuration options for a Bootstrap.
// Options may be overridden before passing to NewBootstrap
func DefaultBootstrapConfig() *BootstrapConfig {
	return &BootstrapConfig{
		Clock:              clock.New(), // use standard time
		Timeout:            5 * time.Minute,
		RequestConcurrency: 3,
		RequestTimeout:     time.Minute,
	}
}

func NewBootstrap[K kad.Key[K], N kad.NodeID[K], I any](self N, cfg *BootstrapConfig) (*Bootstrap[K, N, I], error) {
	if cfg == nil {
		cfg = DefaultBootstrapConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Bootstrap[K, N, I]{
		self: self,
		cfg:  *cfg,
	}, nil
}

// Advance advances the state of the bootstrap by attempting to advance its query if running.
func (b *Bootstrap[K, N, I]) Advance(ctx context.Context, ev BootstrapEvent) BootstrapState {
	ctx, span := util.StartSpan(ctx, "Bootstrap.Advance")
	defer span.End()

	switch tev := ev.(type) {
	case *EventBootstrapStart[K, N, I]:
		// TODO: ignore start event if query is already in progress
		iter := query.NewClosestNodesIter[K, N](b.self.Key())

		qryCfg := query.DefaultQueryConfig[K]()
		qryCfg.Clock = b.cfg.Clock
		qryCfg.Concurrency = b.cfg.RequestConcurrency
		qryCfg.RequestTimeout = b.cfg.RequestTimeout

		queryID := query.QueryID("bootstrap")

		qry, err := query.NewQuery[K, N, I](b.self, queryID, tev.ProtocolID, tev.Message, iter, tev.KnownClosestNodes, qryCfg)
		if err != nil {
			// TODO: don't panic
			panic(err)
		}
		b.qry = qry
		return b.advanceQuery(ctx, nil)

	case *EventBootstrapMessageResponse[K, N, I]:
		return b.advanceQuery(ctx, &query.EventQueryMessageResponse[K, N, I]{
			NodeID:   tev.NodeID,
			Response: tev.Response,
		})
	case *EventBootstrapMessageFailure[K]:
		return b.advanceQuery(ctx, &query.EventQueryMessageFailure[K]{
			NodeID: tev.NodeID,
			Error:  tev.Error,
		})

	case *EventBootstrapPoll:
	// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if b.qry != nil {
		return b.advanceQuery(ctx, nil)
	}

	return &StateBootstrapIdle{}
}

func (b *Bootstrap[K, N, I]) advanceQuery(ctx context.Context, qev query.QueryEvent) BootstrapState {
	state := b.qry.Advance(ctx, qev)
	switch st := state.(type) {
	case *query.StateQueryWaitingMessage[K, N, I]:
		return &StateBootstrapMessage[K, N, I]{
			QueryID:    st.QueryID,
			Stats:      st.Stats,
			NodeID:     st.NodeID,
			ProtocolID: st.ProtocolID,
			Message:    st.Message,
		}
	case *query.StateQueryFinished:
		return &StateBootstrapFinished{
			Stats: st.Stats,
		}
	case *query.StateQueryWaitingAtCapacity:
		elapsed := b.cfg.Clock.Since(st.Stats.Start)
		if elapsed > b.cfg.Timeout {
			return &StateBootstrapTimeout{
				Stats: st.Stats,
			}
		}
		return &StateBootstrapWaiting{
			Stats: st.Stats,
		}
	case *query.StateQueryWaitingWithCapacity:
		elapsed := b.cfg.Clock.Since(st.Stats.Start)
		if elapsed > b.cfg.Timeout {
			return &StateBootstrapTimeout{
				Stats: st.Stats,
			}
		}
		return &StateBootstrapWaiting{
			Stats: st.Stats,
		}
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}
}

// BootstrapState is the state of a bootstrap.
type BootstrapState interface {
	bootstrapState()
}

// StateBootstrapMessage indicates that the bootstrap query is waiting to message a node.
type StateBootstrapMessage[K kad.Key[K], N kad.NodeID[K], I any] struct {
	QueryID    query.QueryID
	NodeID     N
	ProtocolID address.ProtocolID
	Message    kad.Request[K, N, I]
	Stats      query.QueryStats
}

// StateBootstrapIdle indicates that the bootstrap is not running its query.
type StateBootstrapIdle struct{}

// StateBootstrapFinished indicates that the bootstrap has finished.
type StateBootstrapFinished struct {
	Stats query.QueryStats
}

// StateBootstrapTimeout indicates that the bootstrap query has timed out.
type StateBootstrapTimeout struct {
	Stats query.QueryStats
}

// StateBootstrapWaiting indicates that the bootstrap query is waiting for a response.
type StateBootstrapWaiting struct {
	Stats query.QueryStats
}

// bootstrapState() ensures that only Bootstrap states can be assigned to a BootstrapState.
func (*StateBootstrapMessage[K, N, I]) bootstrapState() {}
func (*StateBootstrapIdle) bootstrapState()             {}
func (*StateBootstrapFinished) bootstrapState()         {}
func (*StateBootstrapTimeout) bootstrapState()          {}
func (*StateBootstrapWaiting) bootstrapState()          {}

// BootstrapEvent is an event intended to advance the state of a bootstrap.
type BootstrapEvent interface {
	bootstrapEvent()
}

// EventBootstrapPoll is an event that signals the bootstrap that it can perform housekeeping work such as time out queries.
type EventBootstrapPoll struct{}

// EventBootstrapStart is an event that attempts to start a new bootstrap
type EventBootstrapStart[K kad.Key[K], N kad.NodeID[K], I any] struct {
	ProtocolID        address.ProtocolID
	Message           kad.Request[K, N, I]
	KnownClosestNodes []N
}

// EventBootstrapMessageResponse notifies a bootstrap that a sent message has received a successful response.
type EventBootstrapMessageResponse[K kad.Key[K], N kad.NodeID[K], I any] struct {
	NodeID   N                     // the node the message was sent to
	Response kad.Response[K, N, I] // the message response sent by the node
}

// EventBootstrapMessageFailure notifiesa bootstrap that an attempt to send a message has failed.
type EventBootstrapMessageFailure[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node the message was sent to
	Error  error         // the error that caused the failure, if any
}

// bootstrapEvent() ensures that only Bootstrap events can be assigned to the BootstrapEvent interface.
func (*EventBootstrapPoll) bootstrapEvent()                     {}
func (*EventBootstrapStart[K, N, I]) bootstrapEvent()           {}
func (*EventBootstrapMessageResponse[K, N, I]) bootstrapEvent() {}
func (*EventBootstrapMessageFailure[K]) bootstrapEvent()        {}
