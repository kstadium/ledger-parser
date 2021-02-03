package history

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/the-medium/ledger-parser/internal/utils"
)

const (
	HistoryFormatVersion = iota
	HistorySavePoint
	HistoryInitialized
	HistoryGeneral
)

var InitializedKeyName = "\x00" + string(utf8.MaxRune) + "initialized"

type KVSet interface {
	Describe() string
	Key() []byte
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

func (kv FormatVersionKV) Key() []byte {
	return kv.key
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

func (kv SavePointKV) Key() []byte {
	return kv.key
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
	msg += fmt.Sprintf("RealValue: %x\n", kv.value)
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

type InitializedKV struct {
	key   []byte
	value []byte
}

func (kv InitializedKV) Describe() string {
	return "indicates if chancode is initialized"
}

func (kv InitializedKV) Key() []byte {
	return kv.key
}

func (kv InitializedKV) Location() (uint64, uint64, error) {
	ccInternalKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]
	internalKey := bytes.SplitN(ccInternalKey, []byte{0x00}, 2)[1]
	offset := 0
	sizeSize := utils.GetInt(internalKey[0 : offset+1])
	offset += 1
	keySize := utils.GetInt(internalKey[offset : offset+sizeSize])
	offset += keySize

	locBytes := bytes.SplitN(internalKey[offset:], []byte{0x00}, 2)[1]
	offset = 0
	sizeSize = utils.GetInt(locBytes[0 : offset+1])
	offset += 1
	blockNum := utils.GetInt(locBytes[offset : offset+sizeSize])
	offset += sizeSize
	sizeSize = utils.GetInt(locBytes[offset : offset+1])

	offset += 1
	txNum := utils.GetInt(locBytes[offset : offset+sizeSize])
	return uint64(blockNum), uint64(txNum), nil
}

func (kv InitializedKV) Print() {
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

	msg := fmt.Sprintf("<InitializedKV>\n")
	msg += fmt.Sprintf("channel: %s\n", channel)
	msg += fmt.Sprintf("RealKey: %s\n", realKey)
	msg += fmt.Sprintf("RealValue: %s\n", kv.value)
	msg += fmt.Sprintf("Block Number: %d\n", blockNum)
	msg += fmt.Sprintf("Tx Number: %d\n", txNum)
	fmt.Println(msg)
}

func (kv InitializedKV) Type() int {
	return HistoryInitialized
}

func (kv InitializedKV) Value() string {
	return string(kv.value)
}

type GeneralKV struct {
	key   []byte
	value []byte
}

func (kv GeneralKV) Describe() string {
	return "general history key value pair"
}

func (kv GeneralKV) Key() []byte {
	return kv.key
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
