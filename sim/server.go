package sim

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/endpoint"
	"github.com/plprobelab/go-kademlia/network/message"
	"github.com/plprobelab/go-kademlia/routing"
	"github.com/plprobelab/go-kademlia/util"
)

type Server[K kad.Key[K], A any] struct {
	rt       routing.Table[K]
	endpoint endpoint.Endpoint[K, A]

	peerstoreTTL              time.Duration
	numberOfCloserPeersToSend int
}

func NewServer[K kad.Key[K], A any](rt routing.Table[K], endpoint endpoint.Endpoint[K, A], cfg *ServerConfig) *Server[K, A] {
	if cfg == nil {
		cfg = DefaultServerConfig()
	}
	return &Server[K, A]{
		rt:                        rt,
		endpoint:                  endpoint,
		peerstoreTTL:              cfg.PeerstoreTTL,
		numberOfCloserPeersToSend: cfg.NumberUsefulCloserPeers,
	}
}

func (s *Server[K, A]) HandleRequest(ctx context.Context, rpeer kad.NodeID[K],
	msg message.MinKadMessage,
) (message.MinKadMessage, error) {
	switch msg := msg.(type) {
	case *Message[K, A]:
		return s.HandleFindNodeRequest(ctx, rpeer, msg)
	default:
		return nil, ErrUnknownMessageFormat
	}
}

func (s *Server[K, A]) HandleFindNodeRequest(ctx context.Context,
	rpeer kad.NodeID[K], msg message.MinKadMessage,
) (message.MinKadMessage, error) {
	var target K

	switch msg := msg.(type) {
	case *Message[K, A]:
		target = msg.Target()
	default:
		// invalid request, don't reply
		return nil, ErrUnknownMessageFormat
	}

	_, span := util.StartSpan(ctx, "Server.HandleFindNodeRequest", trace.WithAttributes(
		attribute.Stringer("Requester", rpeer),
		attribute.String("Target", key.HexString(target))))
	defer span.End()

	peers, err := s.rt.NearestPeers(ctx, target, s.numberOfCloserPeersToSend)
	if err != nil {
		span.RecordError(err)
		// invalid request, don't reply
		return nil, err
	}
	span.AddEvent("Nearest peers", trace.WithAttributes(
		attribute.Int("count", len(peers)),
	))

	var resp message.MinKadMessage
	switch msg.(type) {
	case *Message[K, A]:
		peerAddrs := make([]kad.NodeInfo[K, A], len(peers))
		var index int
		for _, p := range peers {
			na, err := s.endpoint.NetworkAddress(p)
			if err != nil {
				span.RecordError(err)
				continue
			}
			peerAddrs[index] = na
			index++
		}
		resp = NewResponse(peerAddrs[:index])
	}

	return resp, nil
}

type ServerConfig struct {
	PeerstoreTTL            time.Duration
	NumberUsefulCloserPeers int
}

func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		PeerstoreTTL:            time.Second,
		NumberUsefulCloserPeers: 4,
	}
}