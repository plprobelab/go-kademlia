package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/plprobelab/go-kademlia/internal/kadtest"
	"github.com/plprobelab/go-kademlia/key"
)

func TestClosestNodesIter(t *testing.T) {
	target := key.Key8(0b00000001)
	a := kadtest.NewID(key.Key8(0b00000100)) // 4
	b := kadtest.NewID(key.Key8(0b00001000)) // 8
	c := kadtest.NewID(key.Key8(0b00010000)) // 16
	d := kadtest.NewID(key.Key8(0b00100000)) // 32

	// ensure the order of the known nodes
	require.True(t, target.Xor(a.Key()).Compare(target.Xor(b.Key())) == -1)
	require.True(t, target.Xor(b.Key()).Compare(target.Xor(c.Key())) == -1)
	require.True(t, target.Xor(c.Key()).Compare(target.Xor(d.Key())) == -1)

	iter := NewClosestNodesIter[key.Key8, kadtest.StrAddr](target)

	// add nodes in "random order"

	iter.Add(&NodeStatus[key.Key8, kadtest.StrAddr]{Node: kadtest.NewEmptyInfo[key.Key8, kadtest.StrAddr](b)})
	iter.Add(&NodeStatus[key.Key8, kadtest.StrAddr]{Node: kadtest.NewEmptyInfo[key.Key8, kadtest.StrAddr](d)})
	iter.Add(&NodeStatus[key.Key8, kadtest.StrAddr]{Node: kadtest.NewEmptyInfo[key.Key8, kadtest.StrAddr](a)})
	iter.Add(&NodeStatus[key.Key8, kadtest.StrAddr]{Node: kadtest.NewEmptyInfo[key.Key8, kadtest.StrAddr](c)})

	// Each should iterate in order of distance from target

	distances := make([]key.Key8, 0, 4)
	iter.Each(context.Background(), func(ctx context.Context, ns *NodeStatus[key.Key8, kadtest.StrAddr]) bool {
		distances = append(distances, target.Xor(ns.Node.ID().Key()))
		return false
	})

	require.True(t, key.IsSorted(distances))
}

func TestSequentialIter(t *testing.T) {
	a := kadtest.NewID(key.Key8(0b00000100)) // 4
	b := kadtest.NewID(key.Key8(0b00001000)) // 8
	c := kadtest.NewID(key.Key8(0b00010000)) // 16
	d := kadtest.NewID(key.Key8(0b00100000)) // 32

	iter := NewSequentialIter[key.Key8, kadtest.StrAddr]()

	// add nodes in "random order"

	iter.Add(&NodeStatus[key.Key8, kadtest.StrAddr]{Node: kadtest.NewEmptyInfo[key.Key8, kadtest.StrAddr](b)})
	iter.Add(&NodeStatus[key.Key8, kadtest.StrAddr]{Node: kadtest.NewEmptyInfo[key.Key8, kadtest.StrAddr](d)})
	iter.Add(&NodeStatus[key.Key8, kadtest.StrAddr]{Node: kadtest.NewEmptyInfo[key.Key8, kadtest.StrAddr](a)})
	iter.Add(&NodeStatus[key.Key8, kadtest.StrAddr]{Node: kadtest.NewEmptyInfo[key.Key8, kadtest.StrAddr](c)})

	// Each should iterate in order the nodes were added to the iiterator

	order := make([]key.Key8, 0, 4)
	iter.Each(context.Background(), func(ctx context.Context, ns *NodeStatus[key.Key8, kadtest.StrAddr]) bool {
		order = append(order, ns.Node.ID().Key())
		return false
	})

	require.Equal(t, 4, len(order))
	require.True(t, key.Equal(order[0], b.Key()))
	require.True(t, key.Equal(order[1], d.Key()))
	require.True(t, key.Equal(order[2], a.Key()))
	require.True(t, key.Equal(order[3], c.Key()))
}
