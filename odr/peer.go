// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package access provides a layer to handle local blockchain database and
// on-demand network retrieval
package access

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
)

var (
	errAlreadyRegistered = errors.New("peer is already registered")
	errNotRegistered     = errors.New("peer is not registered")
	errNoOdr             = errors.New("peer cannot serve on-demand requests")
)

// Peer stores ODR-specific information about LES peers that are able to serve requests
type peer interface {
	id() string
	send(msg *MsgData) error
}

// peerSet represents the collection of active peer participating in the block
// download procedure.
type peerSet struct {
	rep   map[string]int
	peers map[string]peer
	lock  sync.RWMutex
}

// newPeerSet creates a new peer set top track the active download sources.
func newPeerSet() *peerSet {
	return &peerSet{
		peers: make(map[string]peer),
		rep:   make(map[string]int), // should be persisted?
	}
}

// Register injects a new peer into the working set, or returns an error if the
// peer is already known.
func (ps *peerSet) register(p peer) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if _, ok := ps.peers[p.id()]; ok {
		return errAlreadyRegistered
	}
	ps.peers[p.id()] = p
	if _, ok = rep[p.id()]; !ok {
		ps.rep[p.id()] = 0
	}
	return nil
}

// Unregister removes a remote peer from the active set, disabling any further
// actions to/from that particular entity.
func (ps *peerSet) unregister(id string) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if _, ok := ps.peers[id]; !ok {
		return errNotRegistered
	}
	delete(ps.peers, id)
	return nil
}

// Peer retrieves the registered peer with the given id.
func (ps *peerSet) get(id string) peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return ps.peers[id]
}

// Len returns if the current number of peers in the set.
func (ps *peerSet) promote(p peer) {
	ps.lock.RLock()
	defer ps.lock.RUnlock()
	ps.rep[p.id()]++
}

// BestPeers returns an ordered list of available peers, starting with the
// highest reputation
func (ps *peerSet) bestN(n int) []peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()
	list := make([]peer, len(ps.peers))
	for i, p := range ps.peers {
		list[i] = p
	}
	s = &byRep{list, self}
	sort.Sort(s)
	return s.data[:n]
}

// sort interface functions
// caller holds the lock

type byRep struct {
	list
	*peerSet
}

func (ps *byRep) Len() {
	return len(self.list)
}

func (ps *byRep) Less(i, j int) bool {
	return self.rep[self.list[i].id()] < self.rep[self.list[j].id()]
}

func (ps *byRep) Swap(i, j int) {
	self.list[i] = self.list[j]
}
