package main

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/plprobelab/go-kademlia/events/scheduler/simplescheduler"
	tutil "github.com/plprobelab/go-kademlia/examples/util"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/network/address/addrinfo"
	"github.com/plprobelab/go-kademlia/network/address/peerid"
	"github.com/plprobelab/go-kademlia/network/endpoint/libp2pendpoint"
	"github.com/plprobelab/go-kademlia/network/message"
	"github.com/plprobelab/go-kademlia/network/message/ipfsv1"
	"github.com/plprobelab/go-kademlia/query/simplequery"
	"github.com/plprobelab/go-kademlia/routing/simplert"
	"github.com/plprobelab/go-kademlia/util"
)

var protocolID address.ProtocolID = "/ipfs/kad/1.0.0" // IPFS DHT network protocol ID

func FindPeer(ctx context.Context) {
	ctx, span := util.StartSpan(ctx, "FindPeer Test")
	defer span.End()

	// this example is using real time
	clk := clock.New()

	// create a libp2p host
	h, err := tutil.Libp2pHost(ctx, "8888")
	if err != nil {
		panic(err)
	}

	pid := peerid.NewPeerID(h.ID())
	// get the peer's kademlia key (derived from its peer.ID)
	kadid := pid.Key()

	// create a simple routing table, with bucket size 20
	rt := simplert.New(kadid, 20)
	// create a scheduler using real time
	sched := simplescheduler.NewSimpleScheduler(clk)
	// create a message endpoint is used to communicate with other peers
	msgEndpoint := libp2pendpoint.NewLibp2pEndpoint(ctx, h, sched)

	// friend is the first peer we know in the IPFS DHT network (bootstrap node)
	friend, err := peer.Decode("12D3KooWGjgvfDkpuVAoNhd7PRRvMTEG4ZgzHBFURqDe1mqEzAMS")
	if err != nil {
		panic(err)
	}
	friendID := peerid.NewPeerID(friend)

	// multiaddress of friend
	a, err := multiaddr.NewMultiaddr("/ip4/45.32.75.236/udp/4001/quic")
	if err != nil {
		panic(err)
	}
	// connect to friend
	friendAddr := peer.AddrInfo{ID: friend, Addrs: []multiaddr.Multiaddr{a}}
	if err := h.Connect(ctx, friendAddr); err != nil {
		panic(err)
	}
	fmt.Println("connected to friend")

	// target is the peer we want to find QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb
	target, err := peer.Decode("QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb")
	if err != nil {
		panic(err)
	}
	targetID := peerid.NewPeerID(target)

	// create a find peer request message
	req := ipfsv1.FindPeerRequest(targetID)
	// add friend to routing table
	success, err := rt.AddPeer(ctx, friendID)
	if err != nil || !success {
		panic("failed to add friend to rt")
	}

	// endCond is used to terminate the simulation once the query is done
	endCond := false
	handleResultsFn := func(ctx context.Context, id address.NodeID,
		resp message.MinKadResponseMessage,
	) (bool, []address.NodeID) {
		// parse response to ipfs dht message
		msg, ok := resp.(*ipfsv1.Message)
		if !ok {
			fmt.Println("invalid response!")
			return false, nil
		}
		var targetAddrs *addrinfo.AddrInfo
		peers := make([]address.NodeID, 0, len(msg.CloserPeers))
		for _, p := range msg.CloserPeers {
			addrInfo, err := ipfsv1.PBPeerToPeerInfo(p)
			if err != nil {
				fmt.Println("invalid peer info format")
				continue
			}
			peers = append(peers, addrInfo.PeerID())
			if addrInfo.PeerID().ID == target {
				endCond = true
				targetAddrs = addrInfo
			}
		}
		fmt.Println("---\nResponse from", id, "with", peers)
		if endCond {
			fmt.Println("\n  - target found!", target, targetAddrs.Addrs)
		}
		// return peers and not msg.CloserPeers because we want to return the
		// PeerIDs and not AddrInfos. The returned NodeID is used to update the
		// query. The AddrInfo is only useful for the message endpoint.
		return endCond, peers
	}

	// create the query, the IPFS DHT protocol ID, the IPFS DHT request message,
	// a concurrency parameter of 1, a timeout of 5 seconds, the libp2p message
	// endpoint, the node's routing table and scheduler, and the response
	// handler function.
	// The query will be executed only once actions are run on the scheduler.
	// For now, it is only scheduled to be run.
	queryOpts := []simplequery.Option{
		simplequery.WithProtocolID(protocolID),
		simplequery.WithConcurrency(1),
		simplequery.WithRequestTimeout(2 * time.Second),
		simplequery.WithHandleResultsFunc(handleResultsFn),
		simplequery.WithRoutingTable(rt),
		simplequery.WithEndpoint(msgEndpoint),
		simplequery.WithScheduler(sched),
	}
	_, err = simplequery.NewSimpleQuery(ctx, req, queryOpts...)
	if err != nil {
		panic(err)
	}

	span.AddEvent("start request execution")

	// run the actions from the scheduler until the query is done
	for i := 0; i < 1000 && !endCond; i++ {
		for sched.RunOne(ctx) {
		}
		time.Sleep(10 * time.Millisecond)
	}
}
