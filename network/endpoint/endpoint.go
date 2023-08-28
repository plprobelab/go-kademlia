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
type ResponseHandlerFn[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] func(context.Context, kad.Response[K, A, N, I], error)

// Endpoint defines how Kademlia nodes interacts with each other.
type Endpoint[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] interface {
	// MaybeAddToPeerstore adds the given address to the peerstore if it is
	// valid and if it is not already there.
	// TODO: consider returning a status of whether the nodeinfo is a new node or contains a new address
	MaybeAddToPeerstore(context.Context, I, time.Duration) error

	// SendRequestHandleResponse attempts to sends a request to the given peer and handles
	// the response with the given handler.
	// An error is returned if the endpoint is unable to initiate sending the message for
	// any reason. The handler will not be called if an error is returned.
	SendRequestHandleResponse(context.Context, address.ProtocolID, N,
		kad.Message, kad.Message, time.Duration,
		ResponseHandlerFn[K, A, N, I]) error

	// NetworkAddress returns the network address of the given peer (if known).
	NetworkAddress(N) (I, error)
}

// ServerEndpoint is a Kademlia endpoint that can handle requests from remote
// peers.
type ServerEndpoint[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] interface {
	Endpoint[K, A, N, I]
	// AddRequestHandler registers a handler for a given protocol ID.
	AddRequestHandler(address.ProtocolID, kad.Message, RequestHandlerFn[K]) error
	// RemoveRequestHandler removes a handler for a given protocol ID.
	RemoveRequestHandler(address.ProtocolID)
}

// NetworkedEndpoint is an endpoint keeping track of the connectedness with
// known remote peers.
type NetworkedEndpoint[K kad.Key[K], A kad.Address[A], N kad.NodeID[K], I kad.NodeInfo[K, A, N]] interface {
	Endpoint[K, A, N, I]
	// Connectedness returns the connectedness of the given peer.
	Connectedness(kad.NodeID[K]) (Connectedness, error)
}

// StreamID is a unique identifier for a stream.
type StreamID uint64
