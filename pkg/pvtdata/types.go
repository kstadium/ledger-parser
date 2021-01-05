package pvtdata

import (
	"bytes"
	"fmt"

	goproto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/common/ledger/util"
)

type KVSet interface {
	Describe() string
	Key() string
	Location() (uint64, uint64, error)
	Print()
	Type() int
	Value() []byte
}

type LastCommittedBlockKV struct {
	key   []byte
	value []byte
}

func (kv LastCommittedBlockKV) Describe() string {
	return "last committed block number"
}

func (kv LastCommittedBlockKV) Key() string {
	return ""
}

func (kv LastCommittedBlockKV) Print() {
	channel := bytes.Split(kv.key, []byte{0x00})[0]

	// last block number s
	s, _ := goproto.DecodeVarint(kv.value)

	// build message
	msg := fmt.Sprintf("<LastCommittedBlockKV>\n")
	msg += fmt.Sprintf("channel: %s\n", channel)
	msg += fmt.Sprintf("realkey: %s\n", kv.Key())
	msg += fmt.Sprintf("value: %d\n", s)
	fmt.Println(msg)

	return
}

func (kv LastCommittedBlockKV) Location() (uint64, uint64, error) {
	return 0, 0, fmt.Errorf("no location info")
}

func (kv LastCommittedBlockKV) Type() int {
	return int(lastCommittedBlkkey)
}

func (kv LastCommittedBlockKV) Value() []byte {
	return kv.value
}

type PvtDataKV struct {
	key   []byte
	value []byte
}

func (kv PvtDataKV) Describe() string {
	return "privatedata key value pair with location info"
}

func (kv PvtDataKV) Key() string {
	internalKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]

	// count block Number bytes
	_, off1, err := util.DecodeOrderPreservingVarUint64(internalKey[1:])
	if err != nil {
		return err.Error()
	}

	// count transaction Number bytes
	_, off2, err := util.DecodeOrderPreservingVarUint64(internalKey[off1+1:])
	if err != nil {
		return err.Error()
	}

	return string(bytes.SplitN(internalKey[1+off1+off2:], []byte{0x00}, 2)[1])
}

func (kv PvtDataKV) Location() (uint64, uint64, error) {

	internalKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]

	bNum, offset, err := util.DecodeOrderPreservingVarUint64(internalKey[1:])
	if err != nil {
		return 0, 0, err
	}

	txNum, _, err := util.DecodeOrderPreservingVarUint64(internalKey[offset+1:])
	if err != nil {
		return 0, 0, err
	}

	return bNum, txNum, nil

}

func (kv PvtDataKV) Print() {
	bNum, txNum, err := kv.Location()
	if err != nil {
		fmt.Println(err.Error())
	}
	splited := bytes.Split(kv.key, []byte{0x00})
	internalKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]

	// block number byte
	_, off1, err := util.DecodeOrderPreservingVarUint64(internalKey[1:])
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// transaction number byte
	_, off2, err := util.DecodeOrderPreservingVarUint64(internalKey[off1+1:])
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// parse value
	collPvtdata := &rwset.CollectionPvtReadWriteSet{}
	err = goproto.Unmarshal(kv.value, collPvtdata)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	rwset := &kvrwset.KVRWSet{}
	err = goproto.Unmarshal(collPvtdata.Rwset, rwset)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// build value message
	value := fmt.Sprintf("\tcollectionName: %s\n", collPvtdata.CollectionName)
	value += fmt.Sprintf("\trwset: %s\n", rwset.String())

	// build message
	msg := fmt.Sprintf("<PvtDataKV>\n")
	msg += fmt.Sprintf("channel: %s\n", splited[0])
	msg += fmt.Sprintf("chaincode: %s\n", bytes.SplitN(internalKey[1+off1+off2:], []byte{0x00}, 2)[0])
	msg += fmt.Sprintf("BlockNum %d\n", bNum)
	msg += fmt.Sprintf("TxNum %d\n", txNum)
	msg += fmt.Sprintf("realkey: %s\n", kv.Key())
	msg += fmt.Sprintf("value:\n%s\n", value)

	fmt.Println(msg)
}

func (kv PvtDataKV) Type() int {
	return int(pvtDataKeyPrefix)
}

func (kv PvtDataKV) Value() []byte {
	return kv.value
}
