package odr

type Odr interface {
	Retrieve(r requests.Request, cancel chan bool) (interface{}, error)
}

type Access struct {
	odr Odr
}

func (self *Access) GetBodyRLP(db ethdb.Database, hash common.Hash, context chan bool) rlp.RawValue {
	req := &GetBodyReq{hash}
	self.odr.Retrieve(req, context)
}

func GetBlockReceipts(db ethdb.Database, hash common.Hash, context chan bool) types.Receipts {
	//
}
