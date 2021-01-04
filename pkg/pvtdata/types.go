package pvtdata

import (
	"bytes"
	"fmt"

	goproto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric/common/ledger/util"
)

type KVSet interface {
	Describe() string
	Key() string
	Location() (uint64, uint64, error)
	Print()
	Type() int
	Value() string
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
	msg := fmt.Sprintf("<LastCommittedBlockKV>\n")
	msg += fmt.Sprintf("channel: %s\n", channel)
	msg += fmt.Sprintf("key: %s\n", kv.Key())
	msg += fmt.Sprintf("value: %s\n", kv.Value())
	fmt.Println(msg)
	return
}

func (kv LastCommittedBlockKV) Location() (uint64, uint64, error) {
	return 0, 0, fmt.Errorf("no location info")
}

func (kv LastCommittedBlockKV) Type() int {
	return int(lastCommittedBlkkey)
}

func (kv LastCommittedBlockKV) Value() string {
	s, _ := goproto.DecodeVarint(kv.value)

	return fmt.Sprintf("%d", s)
}

type PvtDataKV struct {
	key   []byte
	value []byte
}

func (kv PvtDataKV) Describe() string {
	return "privatedata key value pair with location info"
}

func (kv PvtDataKV) Key() string {
	// splited := bytes.Split(kv.key, []byte{0x00})

	// return string(splited[len(splited)-1])

	internalKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]

	_, off1, err := util.DecodeOrderPreservingVarUint64(internalKey[1:])
	if err != nil {
		return err.Error()
	}

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

	_, off1, err := util.DecodeOrderPreservingVarUint64(internalKey[1:])
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	_, off2, err := util.DecodeOrderPreservingVarUint64(internalKey[off1+1:])
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	msg := fmt.Sprintf("<PvtDataKV>\n")
	msg += fmt.Sprintf("channel: %s\n", splited[0])
	msg += fmt.Sprintf("chaincode: %s\n", bytes.SplitN(internalKey[1+off1+off2:], []byte{0x00}, 2)[0])
	msg += fmt.Sprintf("key: %s\n", kv.Key())
	msg += fmt.Sprintf("value: %s\n", kv.Value())
	msg += fmt.Sprintf("BlockNum %d\n", bNum)
	msg += fmt.Sprintf("TxNum %d\n", txNum)
	fmt.Println(msg)
}

func (kv PvtDataKV) Type() int {
	return int(pvtDataKeyPrefix)
}

func (kv PvtDataKV) Value() string {
	collPvtdata := &rwset.CollectionPvtReadWriteSet{}
	err := goproto.Unmarshal(kv.value, collPvtdata)
	if err != nil {
		return err.Error()
	}

	return collPvtdata.String()
}
