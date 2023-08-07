package endpoint

import (
	"context"
	"time"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/network/address"
)

// Connectedness signals the capacity for a connection with a given node.
// It is used to signal to services and other peers whether a node is reachable.
type Connectedness int

const (
	// NotConnected means no connection to peer, and no extra information (default)
	NotConnected Connectedness = iota

	// Connected means has an open, live connection to peer
	Connected

	// CanConnect means recently connected to peer, terminated gracefully
	CanConnect

	// CannotConnect means recently attempted connecting but failed to connect.
	// (should signal "made effort, failed")
	CannotConnect
)

// RequestHandlerFn defines a function that handles a request from a remote peer
type RequestHandlerFn[K kad.Key[K]] func(context.Context, kad.NodeID[K],
	kad.Message) (kad.Message, error)

// ResponseHandlerFn defines a function that deals with the response to a
// request previously sent to a remote peer.
type ResponseHandlerFn[K kad.Key[K], N kad.NodeID[K], A kad.Address[A], R kad.Record] func(context.Context, kad.Response[K, N, A, R], error)

// Endpoint defines how Kademlia nodes interacts with each other.
type Endpoint[K kad.Key[K], N kad.NodeID[K], A kad.Address[A]] interface {
	// MaybeAddToPeerstore adds the given address to the peerstore if it is
	// valid and if it is not already there.
	// TODO: consider returning a status of whether the nodeinfo is a new node or contains a new address
	MaybeAddToPeerstore(context.Context, kad.NodeInfo[K, N, A], time.Duration) error

	// SendRequestHandleResponse sends a request to the given peer and handles
	// the response with the given handler.
	// TODO: replace by SendMessage
	// SendRequestHandleResponse(context.Context, address.ProtocolID, kad.NodeID[K],
	//	kad.Message, kad.Message, time.Duration,
	//	ResponseHandlerFn[K, N, A]) error

	// SendMessage sends a message to the given peer and returns the response.
	// SendMessage(context.Context, address.ProtocolID, kad.NodeID[K], kad.Request[K, N, A]) (kad.Response[K, N, A], error)

	// NetworkAddress returns the network address of the given peer (if known).
	NetworkAddress(kad.NodeID[K]) (kad.NodeInfo[K, N, A], error)
}

// ServerEndpoint is a Kademlia endpoint that can handle requests from remote
// peers.
type ServerEndpoint[K kad.Key[K], N kad.NodeID[K], A kad.Address[A]] interface {
	Endpoint[K, N, A]
	// AddRequestHandler registers a handler for a given protocol ID.
	AddRequestHandler(address.ProtocolID, kad.Message, RequestHandlerFn[K]) error
	// RemoveRequestHandler removes a handler for a given protocol ID.
	RemoveRequestHandler(address.ProtocolID)
}

// NetworkedEndpoint is an endpoint keeping track of the connectedness with
// known remote peers.
type NetworkedEndpoint[K kad.Key[K], N kad.NodeID[K], A kad.Address[A]] interface {
	Endpoint[K, N, A]
	// Connectedness returns the connectedness of the given peer.
	Connectedness(kad.NodeID[K]) (Connectedness, error)
}

// StreamID is a unique identifier for a stream.
type StreamID uint64
