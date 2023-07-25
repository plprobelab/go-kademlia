// Package trie provides an implementation of a XOR Trie
package trie

import (
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

// Trie is a trie for equal-length bit vectors, which stores values only in the leaves.
// A node may optionally hold data of type D
// Trie node invariants:
// (1) Either both branches are nil, or both are non-nil.
// (2) If branches are non-nil, key must be nil.
// (3) If both branches are leaves, then they are both non-empty (have keys).
type Trie[K kad.Key[K], D any] struct {
	branch [2]*Trie[K, D]
	key    *K
	data   D
}

func New[K kad.Key[K], D any]() *Trie[K, D] {
	return &Trie[K, D]{}
}

func (tr *Trie[K, D]) Key() *K {
	return tr.key
}

func (tr *Trie[K, D]) Data() D {
	return tr.data
}

func (tr *Trie[K, D]) Branch(dir int) *Trie[K, D] {
	return tr.branch[dir]
}

// Size returns the number of keys added to the trie.
func (tr *Trie[K, D]) Size() int {
	return tr.sizeAtDepth(0)
}

// Size returns the number of keys added to the trie at or beyond depth d.
func (tr *Trie[K, D]) sizeAtDepth(d int) int {
	if tr.IsLeaf() {
		if !tr.HasKey() {
			return 0
		} else {
			return 1
		}
	} else {
		return tr.branch[0].sizeAtDepth(d+1) + tr.branch[1].sizeAtDepth(d+1)
	}
}

// HasKey reports whether the Trie node holds a key.
func (tr *Trie[K, D]) HasKey() bool {
	return tr.key != nil
}

// IsLeaf reports whether the Trie is a leaf node. A leaf node has no child branches but may hold a key and data.
func (tr *Trie[K, D]) IsLeaf() bool {
	return tr.branch[0] == nil && tr.branch[1] == nil
}

// IsEmptyLeaf reports whether the Trie is a leaf node without branches that also has no key.
func (tr *Trie[K, D]) IsEmptyLeaf() bool {
	return !tr.HasKey() && tr.IsLeaf()
}

// IsEmptyLeaf reports whether the Trie is a leaf node without branches but has a key.
func (tr *Trie[K, D]) IsNonEmptyLeaf() bool {
	return tr.HasKey() && tr.IsLeaf()
}

func (tr *Trie[K, D]) Copy() *Trie[K, D] {
	if tr.IsLeaf() {
		return &Trie[K, D]{key: tr.key, data: tr.data}
	}

	return &Trie[K, D]{branch: [2]*Trie[K, D]{
		tr.branch[0].Copy(),
		tr.branch[1].Copy(),
	}}
}

func (tr *Trie[K, D]) shrink() {
	b0, b1 := tr.branch[0], tr.branch[1]
	switch {
	case b0.IsEmptyLeaf() && b1.IsEmptyLeaf():
		tr.branch[0], tr.branch[1] = nil, nil
	case b0.IsEmptyLeaf() && b1.IsNonEmptyLeaf():
		tr.key = b1.key
		tr.branch[0], tr.branch[1] = nil, nil
	case b0.IsNonEmptyLeaf() && b1.IsEmptyLeaf():
		tr.key = b0.key
		tr.branch[0], tr.branch[1] = nil, nil
	}
}

// Add attempts to add a key to the trie, mutating the trie.
// Returns true if the key was added, false otherwise.
func (tr *Trie[K, D]) Add(kk K, data D) bool {
	return tr.addAtDepth(0, kk, data)
}

func (tr *Trie[K, D]) addAtDepth(depth int, kk K, data D) bool {
	switch {
	case tr.IsEmptyLeaf():
		tr.key = &kk
		tr.data = data
		return true
	case tr.IsNonEmptyLeaf():
		if key.Equal(*tr.key, kk) {
			return false
		} else {
			p := tr.key // non-nil since IsNonEmptyLeaf
			d := tr.data
			tr.key = nil
			var v D
			tr.data = v
			// both branches are nil
			tr.branch[0], tr.branch[1] = &Trie[K, D]{}, &Trie[K, D]{}
			tr.branch[(*p).Bit(depth)].key = p
			tr.branch[(*p).Bit(depth)].data = d
			return tr.branch[kk.Bit(depth)].addAtDepth(depth+1, kk, data)
		}
	default:
		return tr.branch[kk.Bit(depth)].addAtDepth(depth+1, kk, data)
	}
}

// Add adds the key to trie, returning a new trie.
// Add is immutable/non-destructive: the original trie remains unchanged.
func Add[K kad.Key[K], D any](tr *Trie[K, D], kk K, data D) (*Trie[K, D], error) {
	return addAtDepth(0, tr, kk, data), nil
}

func addAtDepth[K kad.Key[K], D any](depth int, tr *Trie[K, D], kk K, data D) *Trie[K, D] {
	switch {
	case tr.IsEmptyLeaf():
		return &Trie[K, D]{key: &kk, data: data}
	case tr.IsNonEmptyLeaf():
		eq := key.Equal(*tr.key, kk)
		if eq {
			return tr
		}
		return trieForTwo(depth, *tr.key, tr.data, kk, data)

	default:
		dir := kk.Bit(depth)
		s := &Trie[K, D]{}
		s.branch[dir] = addAtDepth(depth+1, tr.branch[dir], kk, data)
		s.branch[1-dir] = tr.branch[1-dir]
		return s
	}
}

func trieForTwo[K kad.Key[K], D any](depth int, p K, pdata D, q K, qdata D) *Trie[K, D] {
	pDir, qDir := p.Bit(depth), q.Bit(depth)
	if qDir == pDir {
		s := &Trie[K, D]{}
		s.branch[pDir] = trieForTwo(depth+1, p, pdata, q, qdata)
		s.branch[1-pDir] = &Trie[K, D]{}
		return s
	} else {
		s := &Trie[K, D]{}
		s.branch[pDir] = &Trie[K, D]{key: &p, data: pdata}
		s.branch[qDir] = &Trie[K, D]{key: &q, data: qdata}
		return s
	}
}

// Remove attempts to remove a key from the trie, mutating the trie.
// Returns true if the key was removed, false otherwise.
func (tr *Trie[K, D]) Remove(kk K) bool {
	return tr.removeAtDepth(0, kk)
}

func (tr *Trie[K, D]) removeAtDepth(depth int, kk K) bool {
	switch {
	case tr.IsEmptyLeaf():
		return false
	case tr.IsNonEmptyLeaf():
		eq := key.Equal(*tr.key, kk)
		if !eq {
			return false
		}
		tr.key = nil
		var v D
		tr.data = v
		return true
	default:
		if tr.branch[kk.Bit(depth)].removeAtDepth(depth+1, kk) {
			tr.shrink()
			return true
		}
		return false
	}
}

// Remove removes the key from the trie.
// Remove is immutable/non-destructive: the original trie remains unchanged.
// If the key did not exist in the trie then the original trie is returned.
func Remove[K kad.Key[K], D any](tr *Trie[K, D], kk K) (*Trie[K, D], error) {
	return removeAtDepth(0, tr, kk), nil
}

func removeAtDepth[K kad.Key[K], D any](depth int, tr *Trie[K, D], kk K) *Trie[K, D] {
	switch {
	case tr.IsEmptyLeaf():
		return tr
	case tr.IsNonEmptyLeaf():
		eq := key.Equal(*tr.key, kk)
		if !eq {
			return tr
		}
		return &Trie[K, D]{}

	default:
		dir := kk.Bit(depth)
		afterDelete := removeAtDepth(depth+1, tr.branch[dir], kk)
		if afterDelete == tr.branch[dir] {
			return tr
		}
		copy := &Trie[K, D]{}
		copy.branch[dir] = afterDelete
		copy.branch[1-dir] = tr.branch[1-dir]
		copy.shrink()
		return copy
	}
}

func Equal[K kad.Key[K], D any](a, b *Trie[K, D]) bool {
	switch {
	case a.IsEmptyLeaf() && b.IsEmptyLeaf():
		return true
	case a.IsNonEmptyLeaf() && b.IsNonEmptyLeaf():
		eq := key.Equal(*a.key, *b.key)
		if !eq {
			return false
		}
		return true
	case !a.IsLeaf() && !b.IsLeaf():
		return Equal(a.branch[0], b.branch[0]) && Equal(a.branch[1], b.branch[1])
	}
	return false
}

// Find looks for a key in the trie.
// It reports whether the key was found along with data value held with the key.
func Find[K kad.Key[K], D any](tr *Trie[K, D], kk K) (bool, D) {
	f, _ := findFromDepth(tr, 0, kk)
	if f == nil {
		var v D
		return false, v
	}
	return true, f.data
}

// Locate looks for the position of a key in the trie.
// It reports whether the key was found along with the depth of the leaf reached along the path
// of the key, regardless of whether the key was found in that leaf.
func Locate[K kad.Key[K], D any](tr *Trie[K, D], kk K) (bool, int) {
	f, depth := findFromDepth(tr, 0, kk)
	if f == nil {
		return false, depth
	}
	return true, depth
}

func findFromDepth[K kad.Key[K], D any](tr *Trie[K, D], depth int, kk K) (*Trie[K, D], int) {
	switch {
	case tr.IsEmptyLeaf():
		return nil, depth
	case tr.IsNonEmptyLeaf():
		eq := key.Equal(*tr.key, kk)
		if !eq {
			return nil, depth
		}
		return tr, depth
	default:
		return findFromDepth(tr.branch[kk.Bit(depth)], depth+1, kk)
	}
}
