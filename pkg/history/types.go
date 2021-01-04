package history

import (
	"bytes"
	"fmt"

	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/the-medium/ledger-parser/internal/utils"
)

const (
	HistoryFormatVersion = iota
	HistorySavePoint
	HistoryGeneral
)

type KVSet interface {
	Describe() string
	Key() string
	Location() (uint64, uint64, error)
	Print()
	Type() int
	Value() string
}

type FormatVersionKV struct {
	key   []byte
	value []byte
}

func (kv FormatVersionKV) Describe() string {
	return "represents ledger version"
}

func (kv FormatVersionKV) Location() (uint64, uint64, error) {
	return 0, 0, fmt.Errorf("not supported")
}

func (kv FormatVersionKV) Key() string {
	return "formatKey"
}

func (kv FormatVersionKV) Print() {
	keys := bytes.SplitN(kv.key, []byte{0x00}, 2)
	msg := fmt.Sprintf("<FormatVersionKV>\n")
	msg += fmt.Sprintf("channel: %c\n", keys[0])
	msg += fmt.Sprintf("RealKey: %c\n", keys[1])
	msg += fmt.Sprintf("RealValue: %s\n", kv.value)

	fmt.Println(msg)
}

func (kv FormatVersionKV) Type() int {
	return HistoryFormatVersion
}

func (kv FormatVersionKV) Value() string {
	return string(kv.value)
}

type SavePointKV struct {
	key   []byte
	value []byte
}

func (kv SavePointKV) Describe() string {
	return "block height save point"
}

func (kv SavePointKV) Key() string {
	return "savePointKey"
}

func (kv SavePointKV) Location() (uint64, uint64, error) {
	offset := 1
	blockByteSize := utils.GetInt(kv.value[0:offset])
	offset += blockByteSize
	blockNum, offset, err := util.DecodeOrderPreservingVarUint64(kv.value[0:offset])
	if err != nil {
		return 0, 0, err
	}

	txByteSize := utils.GetInt(kv.value[offset : offset+1])
	txNum, _, err := util.DecodeOrderPreservingVarUint64(kv.value[offset : offset+txByteSize+1])
	if err != nil {
		return 0, 0, err
	}

	return blockNum, txNum, nil
}

func (kv SavePointKV) Print() {
	keys := bytes.SplitN(kv.key, []byte{0x00}, 2)
	msg := fmt.Sprintf("<SavePointKV>\n")
	msg += fmt.Sprintf("channel: %s\n", keys[0])
	msg += fmt.Sprintf("RealKey: %s\n", keys[1])
	msg += fmt.Sprintf("RealValue: %s\n", kv.value)
	bNum, txNum, err := kv.Location()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	msg += fmt.Sprintf("Block Number: %d\n", bNum)
	msg += fmt.Sprintf("Tx Number: %d\n", txNum)

	fmt.Println(msg)
}

func (kv SavePointKV) Type() int {
	return HistorySavePoint
}

func (kv SavePointKV) Value() string {
	return fmt.Sprintf("%d", kv.value)
}

type GeneralKV struct {
	key   []byte
	value []byte
}

func (kv GeneralKV) Describe() string {
	return "general history key value pair"
}

func (kv GeneralKV) Key() string {
	ccInternalKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]
	internalKey := bytes.SplitN(ccInternalKey, []byte{0x00}, 2)[1]
	offset := 1

	size := utils.GetInt([]byte{internalKey[0]})
	offset += size

	keySize := utils.GetInt(internalKey[1:offset])
	offset += keySize

	realKey := string(internalKey[1+size : offset])
	return realKey
}

func (kv GeneralKV) Location() (uint64, uint64, error) {
	ccInternalKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]
	internalKey := bytes.SplitN(ccInternalKey, []byte{0x00}, 2)[1]

	loc := bytes.SplitN(internalKey, []byte{0x00}, 2)[1]

	offset := 1
	blockNumSize := utils.GetInt(loc[0:offset])

	blockNum, offset, err := util.DecodeOrderPreservingVarUint64(loc[0 : offset+blockNumSize+1])
	if err != nil {
		return 0, 0, err
	}

	txNumSize := utils.GetInt(loc[offset : offset+1])
	txNum, _, err := util.DecodeOrderPreservingVarUint64(loc[offset : offset+txNumSize+1])
	if err != nil {
		return 0, 0, err
	}

	return blockNum, txNum, nil
}

func (kv GeneralKV) Print() {
	channel := string(bytes.SplitN(kv.key, []byte{0x00}, 2)[0])
	ccInternalKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]
	internalKey := bytes.SplitN(ccInternalKey, []byte{0x00}, 2)[1]
	offset := 1

	size := utils.GetInt([]byte{internalKey[0]})
	offset += size

	keySize := utils.GetInt(internalKey[1:offset])
	offset += keySize

	realKey := string(internalKey[1+size : offset])
	offset++

	blockNum, txNum, err := kv.Location()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	msg := fmt.Sprintf("<GeneralKV>\n")
	msg += fmt.Sprintf("channel: %s\n", channel)
	msg += fmt.Sprintf("RealKey: %s\n", realKey)
	msg += fmt.Sprintf("RealValue: %s\n", kv.value)
	msg += fmt.Sprintf("Block Number: %d\n", blockNum)
	msg += fmt.Sprintf("Tx Number: %d\n", txNum)
	fmt.Println(msg)
}

func (kv GeneralKV) Type() int {
	return HistoryGeneral
}

func (kv GeneralKV) Value() string {
	return string(kv.value)
}
