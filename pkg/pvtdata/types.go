package pvtdata

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/gogo/protobuf/proto"
	goproto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/willf/bitset"
)

type KVSet interface {
	Describe() string
	Key() []byte
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

func (kv LastCommittedBlockKV) Key() []byte {
	return kv.key
}

func (kv LastCommittedBlockKV) Print() {
	channel := bytes.Split(kv.key, []byte{0x00})[0]

	// last block number s
	s, _ := goproto.DecodeVarint(kv.value)

	// build key message
	msgKey := fmt.Sprintf("\tchannel: %s\n", channel)

	// build message
	msg := fmt.Sprintf("<LastCommittedBlockKV>\n")
	msg += fmt.Sprintf("key:\n%s", msgKey)
	msg += fmt.Sprintf("value: %d\n", s)
	fmt.Println(msg)

	return
}

func (kv LastCommittedBlockKV) Location() (uint64, uint64, error) {
	return 0, 0, fmt.Errorf("no location info")
}

func (kv LastCommittedBlockKV) Type() int {
	return int(LastCommittedBlkkey)
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

func (kv PvtDataKV) Key() []byte {
	return kv.key
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
	splited := bytes.SplitN(kv.key, []byte{0x00}, 2)
	channel := splited[0]
	internalKey := splited[1]

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

	h1 := sha256.New()
	h1.Write([]byte(collPvtdata.Rwset))
	encodedPvtRWSetHash := base64.StdEncoding.EncodeToString(h1.Sum(nil))
	fmt.Println("encoded_pvt_rwset_hash:", encodedPvtRWSetHash)

	ccColl := bytes.SplitN(internalKey[1+off1+off2:], []byte{0x00}, 2)
	chaincodeName := ccColl[0]
	collectionName := ccColl[1]

	// build key message
	msgKey := fmt.Sprintf("\tchannel: %s\n", channel)
	msgKey += fmt.Sprintf("\tchaincode: %s\n", chaincodeName)
	msgKey += fmt.Sprintf("\tBlockNum: %d\n", bNum)
	msgKey += fmt.Sprintf("\tTxNum: %d\n", txNum)
	msgKey += fmt.Sprintf("\tcollectionName: %s\n", collectionName)

	// build value message
	msgValue := fmt.Sprintf("\tcollectionName: %s\n", collPvtdata.CollectionName)
	msgValue += fmt.Sprintf("\trwset: %s\n", rwset.String())

	// build message
	msg := fmt.Sprintf("<PvtDataKV>\n")
	msg += fmt.Sprintf("key:\n%s", msgKey)
	msg += fmt.Sprintf("value:\n%s\n", msgValue)

	fmt.Println(msg)
}

func (kv PvtDataKV) Type() int {
	return int(PvtDataKeyPrefix)
}

func (kv PvtDataKV) Value() []byte {
	return kv.value
}

type IneligibleMissingDataKV struct {
	key   []byte
	value []byte
}

func (kv IneligibleMissingDataKV) Describe() string {
	return "ineligible missing data"
}

func (kv IneligibleMissingDataKV) Key() []byte {
	return kv.key
}

func (kv IneligibleMissingDataKV) Print() {

	splittedKey := bytes.SplitN(kv.key, []byte{byte(0)}, 4)

	bNum, _, err := kv.Location()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// parse value
	bitmap := &bitset.BitSet{}
	if err := bitmap.UnmarshalBinary(kv.value); err != nil {
		fmt.Println(err.Error())
		return
	}
	channel := splittedKey[0]
	chaincodeName := splittedKey[1]
	collectionName := splittedKey[2]
	// build key message
	msgKey := fmt.Sprintf("\tchannel: %s\n", channel)
	msgKey += fmt.Sprintf("\tchaincode: %s\n", chaincodeName)
	msgKey += fmt.Sprintf("\tBlockNum: %d\n", bNum)

	msgKey += fmt.Sprintf("\tcollectionName: %s\n", collectionName)

	// build message
	msg := fmt.Sprintf("<IneligibleMissingDataKV>\n")
	msg += fmt.Sprintf("key:\n%s", msgKey)
	// maybe tx number
	msg += fmt.Sprintf("value: %s\n", bitmap.String())
	fmt.Println(msg)

	return
}

func (kv IneligibleMissingDataKV) Location() (uint64, uint64, error) {
	splittedKey := bytes.SplitN(kv.key[1:], []byte{byte(0)}, 4)
	blkNum, _ := decodeReverseOrderVarUint64(splittedKey[3])
	return blkNum, 0, nil
}

func (kv IneligibleMissingDataKV) Type() int {
	return int(IneligibleMissingDataKeyPrefix)
}

func (kv IneligibleMissingDataKV) Value() []byte {
	return kv.value
}

// decodeReverseOrderVarUint64 decodes the number from the bytes obtained from function 'EncodeReverseOrderVarUint64'.
// Also, returns the number of bytes that are consumed in the process
func decodeReverseOrderVarUint64(bytes []byte) (uint64, int) {
	s, _ := proto.DecodeVarint(bytes)
	numFFBytes := int(s)
	decodedBytes := make([]byte, 8)
	realBytesNum := 8 - numFFBytes
	copy(decodedBytes[numFFBytes:], bytes[1:realBytesNum+1])
	numBytesConsumed := realBytesNum + 1
	for i := 0; i < numFFBytes; i++ {
		decodedBytes[i] = 0xff
	}
	return (math.MaxUint64 - binary.BigEndian.Uint64(decodedBytes)), numBytesConsumed
}
