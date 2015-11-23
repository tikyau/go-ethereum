package odr

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	errInvalidMsgId   = errors.New("")
	errInvalidMsgCode = errors.New("")
	errInvalidMsgData = errors.New("")
	errRequestTimeout = errors.New("")
	errCancelled      = errors.New("")

	maxpeers = 3
)

const (
	MsgBlockBodies = iota
	MsgNodeData
	MsgReceipts
	MsgProofs
)

var (
	requestTimeout = time.Millisecond * 300
	retryPeers     = time.Second * 1
)

// Msg encodes a LES message that delivers reply data for a request
type Msg struct {
	code byte
	id   uint
	data interface{}
}

// request wraps core/query.Query with request related logic
// context can be added with quit channel
type request struct {
	requests.Request
	id          string
	deliverChan interface{}
}

// This should live within les
// implements core.Odr interface
type Odr struct {
	lock     sync.Mutex
	requests map[string]requests.Request
	peers    *peerSet
	quit     chan bool
}

func New() *Odr {
	return &Odr{
		requests: make(map[string]*Request),
		peers:    newPeerSet(),
		quit:     make(chan bool),
	}
}

func (self *Odr) Stop() {
	close(self.quit)
}

// Deliver is called by the LES protocol manager to deliver ODR reply messages to waiting requests
// can be called parallel
func (self *Odr) Deliver(peer *peer, mgs *Msg) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	req, found := self.requests[msg.id]
	if !found {
		return errInvalidMsgId
	}
	if msg.code != req.Code() {
		return errInvalidMsgCode
	}
	if err := req.Valid(msg.data); err != nil {
		return errInvalidMsgData
	}
	select {
	case req.deliveryChan <- msg.data:
		self.peers.Promote(peer)
		delete(self.requests, msg.id)
	default:
	}

}

// Request sends a request to known peers until an answer is received
// or the context is cancelled or request times out
//
func (self *Odr) Retrieve(r requests.Request, cancel chan bool) (interface{}, error) {
	id := newId()
	req := &request{
		Request:     r,
		deliverChan: make(chan err),
	}
	self.lock.Lock()
	self.requests[id] = req
	self.unlock.Lock()

	timeout := time.After(requestTimeout)
	errc := make(chan error)
	go func() {
		select {
		case resp = <-req.deliverChan:
			glog.V(LogLevel).Infof("networkRequest success")
			req.resp = resp
			close(errc)
		case <-req.timeout:
			glog.V(LogLevel).Infof("networkRequest timeout")
			errc <- errRequestTimeout
			self.lock.Lock()
			delete(self.requests, id)
			self.unlock.Lock()
		case <-cancel:
			errc <- errCancelled
		case <-self.quit:
			errc <- errCancelled
		}
	}()

	var peers []*peer
	// try until we got at least one peer
	for {
		peers := self.peers.bestN(maxpeers)
		if len(peers) > 0 {
			break
		}
		select {
		case <-req.timeout:
			return nil // no error ? times out?
		case <-time.After(retryPeers):
		}
	}
	// sending to all peers
	msg := newMsg(req)
	for _, peer := range peers {
		peer.send(msg)
	}

	err := <-errc
	if err != nil {
		return nil, err
	}
	return resp, nil
}
