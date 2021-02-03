package index

import (
	"bytes"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	gproto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/fsblkstorage/msgs"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/the-medium/ledger-parser/internal/utils"
)

const (
	BlockNum = iota
	BlockHash
	TxID
	BlockNumTxNum
	BlkMgrInfo
	CheckPoint
	FormatKey
)

type locPointer struct {
	Offset      int
	BytesLength int
}

type FileLocPointer struct {
	FileSuffixNum int
	locPointer
}

func (flp *FileLocPointer) unmarshal(b []byte) error {
	buffer := proto.NewBuffer(b)
	i, e := buffer.DecodeVarint()
	if e != nil {
		return e
	}
	flp.FileSuffixNum = int(i)

	i, e = buffer.DecodeVarint()
	if e != nil {
		return e
	}
	flp.Offset = int(i)
	i, e = buffer.DecodeVarint()
	if e != nil {
		return e
	}
	flp.BytesLength = int(i)
	return nil
}

func RetrieveTxID(encodedTxIDKey []byte) (string, error) {
	if len(encodedTxIDKey) == 0 {
		return "", fmt.Errorf("invalid txIDKey - zero-length slice")
	}
	if encodedTxIDKey[0] != 't' {
		return "", fmt.Errorf("invalid txIDKey {%x} - unexpected prefix", encodedTxIDKey)
	}
	if len(encodedTxIDKey) == 65 {
		return fmt.Sprintf("%s", encodedTxIDKey[1:]), nil
	}
	remainingBytes := encodedTxIDKey[utf8.RuneLen('t'):]

	txIDLen, n, err := util.DecodeOrderPreservingVarUint64(remainingBytes)
	if err != nil {
		return "", fmt.Errorf("%s invalid txIDKey {%x}", err.Error(), encodedTxIDKey)
	}
	remainingBytes = remainingBytes[n:]
	if len(remainingBytes) <= int(txIDLen) {
		return "", fmt.Errorf("invalid txIDKey {%x}, fewer bytes present", encodedTxIDKey)
	}
	return string(remainingBytes[:int(txIDLen)]), nil
}

type IndexValue struct {
	value   string
	blkFlp  *FileLocPointer
	txFlp   *FileLocPointer
	bfsInfo *BlockfilesInfo
}

func (iv IndexValue) GetBlockFLP() FileLocPointer {
	return *iv.blkFlp
}

func (iv IndexValue) GetTransactionFLP() FileLocPointer {
	return *iv.txFlp
}

func (iv IndexValue) String() string {
	str := ""
	if iv.value != "" {
		str += fmt.Sprintf("[value]: %s ", iv.value)
	}
	if iv.blkFlp != nil {
		str += fmt.Sprintf("[block FileLocaionPointer] blockFile: %4d, offset: %7d, bytesLength: %6d ", iv.blkFlp.FileSuffixNum, iv.blkFlp.Offset, iv.blkFlp.BytesLength)
	}
	if iv.txFlp != nil {
		str += fmt.Sprintf("[transaction FileLocaionPointer] blockFile: %4d, offset: %7d, bytesLength: %6d ", iv.txFlp.FileSuffixNum, iv.txFlp.Offset, iv.txFlp.BytesLength)
	}

	if iv.bfsInfo != nil {
		str += fmt.Sprintf("[BlockFilesInfo] latestFileNumber: %4d, latestFileSize: %d, noBlockFiles: %t, lastPersistedBlock: %d", iv.bfsInfo.latestFileNumber, iv.bfsInfo.latestFileSize, iv.bfsInfo.noBlockFiles, iv.bfsInfo.lastPersistedBlock)
	}
	return str
}

type IndexKV interface {
	Channel() string
	Key() []byte
	Value() (IndexValue, error)
	Print()
	Type() int
}

// IdxBlockNum is for searching file and offset where the block exists with block number. Prefix: 'n'
type IdxBlockNum struct {
	key   []byte
	value []byte
}

func (i IdxBlockNum) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxBlockNum) Key() []byte {
	return i.key
}

func (i IdxBlockNum) Value() (IndexValue, error) {
	blkFlp := &FileLocPointer{}
	err := blkFlp.unmarshal(i.value)
	if err != nil {
		return IndexValue{}, err
	}
	return IndexValue{blkFlp: blkFlp}, nil
}

func (i IdxBlockNum) Print() {
	channel := i.Channel()
	blockNum, _, err := util.DecodeOrderPreservingVarUint64(bytes.SplitN(i.key, []byte{0x00}, 2)[1][1:])
	if err != nil {
		fmt.Println(err)
		return
	}
	key, err := strconv.FormatUint(blockNum, 10), nil
	if err != nil {
		fmt.Println(err)
		return
	}
	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("[IdxBlockNum][%s] key:  %3s, value: %s\n", channel, key, value.String())
}

func (i IdxBlockNum) Type() int {
	return BlockNum
}

// IdxBlockHash is for searching file and offset where the block exists with block hash. Prefix: 'h'
type IdxBlockHash struct {
	key   []byte
	value []byte
}

func (i IdxBlockHash) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxBlockHash) Key() []byte {
	return i.key
}

func (i IdxBlockHash) Value() (IndexValue, error) {
	blkFlp := &FileLocPointer{}
	err := blkFlp.unmarshal(i.value)
	if err != nil {
		return IndexValue{}, err
	}
	return IndexValue{blkFlp: blkFlp}, nil
}

func (i IdxBlockHash) Print() {
	channel := i.Channel()
	key := bytes.SplitN(i.key, []byte{0x00}, 2)[1][1:]
	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxBlockHash][%s] key:  %x, value: %s\n", channel, key, value.String())
}

func (i IdxBlockHash) Type() int {
	return BlockHash
}

// IdxTxID is index for searching file, offset and bytesize of Transaction. Prefix: 't'
type IdxTxID struct {
	key   []byte
	value []byte
}

func (i IdxTxID) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxTxID) Key() []byte {
	return i.key
}

func (i IdxTxID) Value() (IndexValue, error) {
	// blkstorage.TxIDIndexValue{} and msgs.TxIDIndexValProto{} are same structure
	// txIdxValue := &index.TxIDIndexValue{} // v2.2.1
	txIdxValue := &msgs.TxIDIndexValProto{} // v2.1.1
	gproto.Unmarshal(i.value, txIdxValue)
	blkFlp := &FileLocPointer{}
	txFlp := &FileLocPointer{}

	err := blkFlp.unmarshal(txIdxValue.BlkLocation)
	if err != nil {
		return IndexValue{}, err
	}
	err = txFlp.unmarshal(txIdxValue.TxLocation)
	if err != nil {
		return IndexValue{}, err
	}

	return IndexValue{blkFlp: blkFlp, txFlp: txFlp}, nil
}

func (i IdxTxID) Print() {
	channel := i.Channel()
	key, err := RetrieveTxID(bytes.SplitN(i.key, []byte{0x00}, 2)[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxTxID][%s] key:  %s, value: %s\n", channel, key, value.String())
}

func (i IdxTxID) Type() int {
	return TxID
}

// IdxBlockNumTxNum is index for searching offset and bytelength of transaction with block number and countnumber of transaction. Prefix: 'a'
type IdxBlockNumTxNum struct {
	key   []byte
	value []byte
}

func (i IdxBlockNumTxNum) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxBlockNumTxNum) Key() []byte {
	return i.key
}

func (i IdxBlockNumTxNum) Value() (IndexValue, error) {
	txFlp := &FileLocPointer{}
	err := txFlp.unmarshal(i.value)
	if err != nil {
		return IndexValue{}, err
	}
	return IndexValue{txFlp: txFlp}, nil
}

func (i IdxBlockNumTxNum) Print() {
	channel := i.Channel()
	keys := bytes.SplitN(i.key, []byte{0x00}, 2)
	internalKey := keys[1]
	lenBlockNumByte := int(internalKey[1])
	offset := 2
	blockNumByte := internalKey[offset : offset+lenBlockNumByte]
	blockNum := utils.GetInt(blockNumByte)
	offset += lenBlockNumByte

	lenTxNumByte := int(internalKey[offset])
	offset++
	txNumByte := internalKey[offset : offset+lenTxNumByte]
	txNum := utils.GetInt(txNumByte)

	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxBlockNumTxNum][%s] key: %3d-%04d, value: %s\n", channel, blockNum, txNum, value.String())
}

func (i IdxBlockNumTxNum) Type() int {
	return BlockNumTxNum
}

type IdxBlkMgrInfo struct {
	key   []byte
	value []byte
}

func (i IdxBlkMgrInfo) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxBlkMgrInfo) Key() []byte {
	return i.key
}

func (i IdxBlkMgrInfo) Value() (IndexValue, error) {
	blkFileInfo := &BlockfilesInfo{}
	err := blkFileInfo.Unmarshal(i.value)
	if err != nil {
		return IndexValue{}, err
	}
	return IndexValue{bfsInfo: blkFileInfo}, nil
}

func (i IdxBlkMgrInfo) Print() {
	channel := i.Channel()
	key := string(bytes.SplitN(i.key, []byte{0x00}, 2)[1])
	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxBlkMgrInfo][%s] key:  %s, value: %s \n", channel, key, value.String())
}

func (i IdxBlkMgrInfo) Type() int {
	return BlkMgrInfo
}

type IdxCheckPoint struct {
	key   []byte
	value []byte
}

func (i IdxCheckPoint) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxCheckPoint) Key() []byte {
	return i.key
}

func (i IdxCheckPoint) Value() (IndexValue, error) {
	return IndexValue{value: fmt.Sprintf("%s", strconv.Itoa(utils.GetInt(i.value)))}, nil

}

func (i IdxCheckPoint) Print() {
	channel := i.Channel()
	key := "indexIdxCheckPointKey"

	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxCheckPoint][%s] key:  %s, value: %s\n", channel, key, value.String())
}

func (i IdxCheckPoint) Type() int {
	return CheckPoint
}

type IdxFormatKey struct {
	key   []byte
	value []byte
}

func (i IdxFormatKey) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxFormatKey) Key() []byte {
	return i.key
}

func (i IdxFormatKey) Value() (IndexValue, error) {
	return IndexValue{value: fmt.Sprintf("%s", i.value)}, nil
}

func (i IdxFormatKey) Print() {
	channel := i.Channel()
	key := "_"
	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxFormatKey][%s] key:  %s, value: %s\n", channel, key, value.String())
}

func (i IdxFormatKey) Type() int {
	return FormatKey
}
