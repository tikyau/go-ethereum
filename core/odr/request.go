package access

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/rlp"
)

type Request interface {
}

const (
	BlockBodies = iota
	Receipts
	NodeData
	Proofs
)

func ReqType(req Request) {
	switch reflect.TypeOf(req) {
	case *BlockBodiesReq:
		return BlockBodies
		// etc
	}
}

type ProofReq struct {
	Root common.Hash
	Key  []byte
}

type BlockBodiesReq struct {
	Data []common.Hash
}

type NodeDataReq struct {
	Data []common.Hash
}

type ReceiptsReq struct {
	Data []common.Hash
}

type ProofsReq struct {
	Data []*ProofReq
}

// validation
func (self *BlockBodiesReq) Valid(data interface{}) error {
	body, ok := data.(*types.Body)
	if !ok {
		return fmt.Errorf("invalid response type, expected body", self.blockHash[:4])
	}
	header := self.getHeader(self.db, self.blockHash)
	if header == nil {
		return fmt.Errorf("ODR: header not found for block %08x", self.blockHash[:4])
	}
	txHash := types.DeriveSha(types.Transactions(body.Transactions))
	if header.TxHash != txHash {
		return fmt.Errorf("ODR: header.TxHash %08x does not match received txHash %08x", header.TxHash[:4], txHash[:4])
	}
	uncleHash := types.CalcUncleHash(body.Uncles)
	if header.UncleHash != uncleHash {
		return fmt.Errorf("ODR: header.UncleHash %08x does not match received uncleHash %08x", header.UncleHash[:4], uncleHash[:4])
	}
	data, err := rlp.EncodeToBytes(body)
	if err != nil {
		return fmt.Errorf("ODR: body RLP encode error: %v", err)
	}
	return nil
}

func (self *BlockBodiesReq) Valid(data interface{}) error {
	receipts, ok := data.(types.Receipts)
	if !ok {
		return fmt.Errorf("invalid response type, expected body", self.blockHash[:4])
	}
	hash := types.DeriveSha(receipts)
	header := self.getHeader(self.db, self.blockHash)
	if header == nil {
		return fmt.Errorf("ODR: header not found for block %08x", self.blockHash[:4])
	}
	if !bytes.Equal(header.ReceiptHash[:], hash[:]) {
		return fmt.Errorf("ODR: header receipts hash %08x does not match calculated RLP hash %08x", header.ReceiptHash[:4], hash[:4])
	}
	self.receipts = receipts
	return nil
}

func (self *TrieEntryAccess) Valid(data interface{}) error {
	proofs, ok := data.(trie.MerkleProof)
	if !ok {
		return fmt.Errorf("ODR: invalid number of entries: %d", len(proofs))
	}
	value, err := trie.VerifyProof(self.root, self.key, proofs[0])
	if err != nil {
		return fmt.Errorf("ODR: merkle proof verification error: %v", err)
	}
	self.proof = proofs
	self.value = value
	return nil
}

func (self *NodeDataAccess) Valid(msg *access.Msg) bool {
	reply := data.([][]byte)
	if !ok {
		return fmt.Errorf("ODR: validating node data for hash %08x", self.hash[:4])
	}
	hash := crypto.Sha3Hash(reply)
	if bytes.Compare(self.hash[:], hash[:]) != 0 {
		return fmt.Errorf("ODR: requested hash %08x does not match received data hash %08x", self.hash[:4], hash[:4])
	}
	self.data = data
	return nil
}
