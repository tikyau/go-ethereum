package bzz

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

const (
	port = "8500"
)

// separate bzz directories
// allow several bzz nodes running in parallel
type Config struct {
	// serialised/persisted fields
	*StoreParams
	*ChunkerParams
	*HiveParams
	Swap      *swapParams
	Path      string
	Port      string
	PublicKey string
	BzzKey    string
	// not serialised/not persisted fields
	// address // node address
}

// config is agnostic to where private key is coming from
// so managing accounts is outside swarm and left to wrappers
func NewConfig(path string, contract common.Address, prvKey *ecdsa.PrivateKey) (self *Config, err error) {

	address := crypto.PubkeyToAddress(prvKey.PublicKey) // default beneficiary address
	dirpath := filepath.Join(path, common.Bytes2Hex(address.Bytes()))
	err = os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		return
	}
	confpath := filepath.Join(dirpath, "config.json")
	var data []byte
	pubkey := crypto.FromECDSAPub(&prvKey.PublicKey)
	pubkeyhex := common.ToHex(pubkey)
	keyhex := crypto.Sha3Hash(pubkey).Hex()

	data, err = ioutil.ReadFile(confpath)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
		// file does not exist

		self = &Config{
			HiveParams:    NewHiveParams(dirpath),
			ChunkerParams: NewChunkerParams(),
			StoreParams:   NewStoreParams(dirpath),
			Port:          port,
			Path:          dirpath,
			Swap:          defaultSwapParams(contract, prvKey),
			PublicKey:     pubkeyhex,
			BzzKey:        keyhex,
		}
		// write out config file
		data, err = json.MarshalIndent(self, "", "    ")
		if err != nil {
			return nil, fmt.Errorf("error writing config: %v", err)
		}
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return
		}
		err = ioutil.WriteFile(confpath, data, os.ModePerm)

	} else {
		// file exists, deserialise
		self = &Config{}
		err = json.Unmarshal(data, self)
		if err != nil {
			return nil, err
		}
		// check public key
		if pubkeyhex != self.PublicKey {
			return nil, fmt.Errorf("public key does not match the one in the config file %v != %v", pubkeyhex, self.PublicKey)
		}
		if keyhex != self.BzzKey {
			return nil, fmt.Errorf("bzz key does not match the one in the config file %v != %v", keyhex, self.BzzKey)
		}
		self.Swap.privateKey = prvKey
		self.Swap.publicKey = &prvKey.PublicKey

	}

	return
}