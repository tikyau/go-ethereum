package swap

import (
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
)

// SwAP Swarm Accounting Protocol with
//      Swift Automatic  Payments
// a peer to peer micropayment system

// public swap profile
// public parameters for SWAP, serializable config struct passed in handshake
type Profile struct {
	BuyAt  *big.Int // accepted max price for chunk
	SellAt *big.Int // offered sale price for chunk
	PayAt  uint     // threshold that triggers payment request
	DropAt uint     // threshold that triggers disconnect
}

// Strategy encapsulates parameters relating to
// automatic deposit and automatic cashing
type Strategy struct {
	AutoCashInterval     time.Duration // default interval for autocash
	AutoCashThreshold    *big.Int      // threshold that triggers autocash (wei)
	AutoDepositInterval  time.Duration // default interval for autocash
	AutoDepositThreshold *big.Int      // threshold that triggers autodeposit (wei)
	AutoDepositBuffer    *big.Int      // buffer that is surplus for fork protection etc (wei)
}

// Params extends the public profile with private parameters relating to
// automatic deposit and automatic cashing
type Params struct {
	*Profile
	*Strategy
}

// Promise
// 3rd party Provable Promise of Payment
// issued by outPayment
// serialisable to send with Protocol
type Promise interface{}

// interface for the peer protocol for testing or external alternative payment
type Protocol interface {
	Pay(int, Promise) // units, payment proof
	Drop()
}

// interface for the (delayed) ougoing payment system with autodeposit
type OutPayment interface {
	Issue(amount *big.Int) (promise Promise, err error)
	AutoDeposit(interval time.Duration, threshold, buffer *big.Int)
	Stop()
}

// interface for the (delayed) incoming payment system with autocash
type InPayment interface {
	Receive(promise Promise) (*big.Int, error)
	AutoCash(cashInterval time.Duration, maxUncashed *big.Int)
	Stop()
}

// swap is the swarm accounting protocol instance
// * pairwise accounting and payments
type Swap struct {
	lock    sync.Mutex // mutex for balance access
	balance int        // units of chunk/retrieval request
	local   *Params    // local peer's swap parameters
	remote  *Profile   // remote peer's swap profile
	out     OutPayment // outgoing payment handler
	in      InPayment  // incoming  payment handler
	proto   Protocol   // peer communication protocol
}

// swap constructor
func New(local *Params, out OutPayment, in InPayment, proto Protocol) (self *Swap, err error) {
	self = &Swap{
		local: local,
		out:   out,
		in:    in,
		proto: proto,
	}

	self.setParams(local)

	return
}

// entry point for setting remote swap profile (e.g from handshake or other message)
func (self *Swap) SetRemote(remote *Profile) {
	defer self.lock.Unlock()
	self.lock.Lock()
	glog.V(logger.Debug).Infof("[SWAP] <%v> remote profile set: pay at: %v, drop at: %v, buy at: %v, sell at: %v", self.proto, remote.PayAt, remote.DropAt, remote.BuyAt, remote.SellAt)

	self.remote = remote
}

// to set strategy dynamically
func (self *Swap) SetParams(local *Params) {
	defer self.lock.Unlock()
	self.lock.Lock()
	self.local = local
}

// caller holds the lock
func (self *Swap) setParams(local *Params) {
	self.in.AutoCash(local.AutoCashInterval, local.AutoCashThreshold)
	glog.V(logger.Debug).Infof("[SWAP] <%v> set autocash to every %v, max uncashed limit: %v", self.proto, local.AutoCashInterval, local.AutoCashThreshold)

	self.out.AutoDeposit(local.AutoDepositInterval, local.AutoDepositThreshold, local.AutoDepositBuffer)
	glog.V(logger.Debug).Infof("[SWAP] <%v> set autodeposit to every %v, pay at: %v, buffer: %v", self.proto, local.AutoDepositInterval, local.AutoDepositThreshold, local.AutoDepositBuffer)
}

// Add(n)
// n > 0 called when promised/provided n units of service
// n < 0 called when used/requested n units of service
func (self *Swap) Add(n int) {
	defer self.lock.Unlock()
	self.lock.Lock()
	self.balance += n
	if self.balance >= int(self.local.DropAt) {
		glog.V(logger.Detail).Infof("[SWAP] peer %v has too much debt (balance: %v, target: %v)", self.out, self.balance, self.local.DropAt)
		self.proto.Drop()
	} else if self.balance <= -int(self.remote.PayAt) {
		self.send()
	}
}

func (self *Swap) Balance() int {
	defer self.lock.Unlock()
	self.lock.Lock()
	return self.balance
}

// send(units) is called when payment is due
// In case of insolvency no promise is issued and sent, safe against fraud
// No return value: no error = payment is opportunistic = hang in till dropped
func (self *Swap) send() {
	if self.local.BuyAt != nil && self.balance < 0 {
		amount := big.NewInt(int64(-self.balance))
		amount.Mul(amount, self.remote.SellAt)
		promise, err := self.out.Issue(amount)
		if err != nil {
			glog.V(logger.Warn).Infof("[SWAP] cannot issue cheque (amount: %v, channel: %v): %v", amount, self.out, err)
		} else {
			glog.V(logger.Warn).Infof("[SWAP] cheque issued (amount: %v, channel: %v)", amount, self.out)
			self.proto.Pay(-self.balance, promise)
			self.balance = 0
		}
	}
}

// receive(units, promise) is called by the protocol when a payment msg is received
// returns error if promise is invalid.
func (self *Swap) Receive(units int, promise Promise) error {
	if units <= 0 {
		return fmt.Errorf("invalid units: %v <= 0", units)
	}

	price := new(big.Int).SetInt64(int64(units))
	price.Mul(price, self.local.SellAt)

	amount, err := self.in.Receive(promise)

	if err != nil {
		err = fmt.Errorf("invalid promise: %v", err)
	} else if price.Cmp(amount) != 0 {
		// verify amount = units * unit sale price
		return fmt.Errorf("invalid amount: %v = %v * %v (units sent in msg * agreed sale unit price) != %v (signed in cheque)", price, units, self.local.SellAt, amount)
	}
	if err != nil {
		glog.V(logger.Detail).Infof("[SWAP] invalid promise (amount: %v, channel: %v): %v", amount, self.in, err)
		return err
	}

	// credit remote peer with units
	self.Add(-units)
	glog.V(logger.Detail).Infof("[SWAP] received promise (amount: %v, channel: %v): %v", amount, self.in, promise)

	return nil
}

// stop() causes autocash loop to terminate.
// Called after protocol handle loop terminates.
func (self *Swap) Stop() {
	self.out.Stop()
	self.in.Stop()
}
