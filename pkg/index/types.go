package index

import (
	"bytes"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	gproto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/the-medium/ledger-parser/internal/index"
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

type fileLocPointer struct {
	fileSuffixNum int
	locPointer
}

func (flp *fileLocPointer) unmarshal(b []byte) error {
	buffer := proto.NewBuffer(b)
	i, e := buffer.DecodeVarint()
	if e != nil {
		return e
	}
	flp.fileSuffixNum = int(i)

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

func retrieveTxID(encodedTxIDKey []byte) (string, error) {
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

type IndexKV interface {
	Channel() string
	Key() (string, error)
	Value() (string, error)
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

func (i IdxBlockNum) Key() (string, error) {
	blockNum, _, err := util.DecodeOrderPreservingVarUint64(bytes.SplitN(i.key, []byte{0x00}, 2)[1][1:])
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(blockNum, 10), nil
}

func (i IdxBlockNum) Value() (string, error) {
	txFLP := fileLocPointer{}
	err := txFLP.unmarshal(i.value)
	return fmt.Sprintf("[filenumber] %d, [offset] %7d, [bytelength] %d", txFLP.fileSuffixNum, txFLP.Offset, txFLP.BytesLength), err
}

func (i IdxBlockNum) Print() {
	channel := i.Channel()
	key, err := i.Key()
	if err != nil {
		fmt.Println(err)
		return
	}
	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("[IdxBlockNum][%s] key:  %3s, value: %s\n", channel, key, value)
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

func (i IdxBlockHash) Key() (string, error) {
	key := bytes.SplitN(i.key, []byte{0x00}, 2)[1][1:]

	return fmt.Sprintf("%x", key), nil
}

func (i IdxBlockHash) Value() (string, error) {
	txFLP := fileLocPointer{}
	err := txFLP.unmarshal(i.value)
	return fmt.Sprintf("[filenumber] %d, [offset] %7d, [bytelength] %d", txFLP.fileSuffixNum, txFLP.Offset, txFLP.BytesLength), err
}

func (i IdxBlockHash) Print() {
	channel := i.Channel()
	key, err := i.Key()
	if err != nil {
		fmt.Println(err)
		return
	}
	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxBlockHash][%s] key:  %s, value: %s\n", channel, key, value)
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

func (i IdxTxID) Key() (string, error) {
	key := bytes.SplitN(i.key, []byte{0x00}, 2)[1]
	txid, err := retrieveTxID(key)
	if err != nil {
		return "", err
	}

	return txid, nil
}

func (i IdxTxID) Value() (val string, err error) {
	txIdxValue := &index.TxIDIndexValue{}
	gproto.Unmarshal(i.value, txIdxValue)
	blkFlp := &fileLocPointer{}
	txFlp := &fileLocPointer{}

	err = blkFlp.unmarshal(txIdxValue.BlkLocation)
	err = txFlp.unmarshal(txIdxValue.TxLocation)
	val = fmt.Sprintf("[BlkLocation] fileNumber %d, offset %6d, bytelength %d [TxLocation]fileNumber %d, offset %6d, bytelength %d [TxValidationCode] %d", blkFlp.fileSuffixNum, blkFlp.Offset, blkFlp.BytesLength, txFlp.fileSuffixNum, txFlp.Offset, txFlp.BytesLength, txIdxValue.TxValidationCode)
	return
}

func (i IdxTxID) Print() {
	channel := i.Channel()
	key, err := i.Key()
	if err != nil {
		fmt.Println(err)
		return
	}
	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxTxID][%s] key:  %s, value: %s\n", channel, key, value)
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

func (i IdxBlockNumTxNum) Key() (string, error) {

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

	return fmt.Sprintf("%3d-%04d", blockNum, txNum), nil
}

func (i IdxBlockNumTxNum) Value() (string, error) {
	txFLP := fileLocPointer{}
	err := txFLP.unmarshal(i.value)
	return fmt.Sprintf("[filenumber] %d, [offset] %7d, [bytelength] %d", txFLP.fileSuffixNum, txFLP.Offset, txFLP.BytesLength), err
}

func (i IdxBlockNumTxNum) Print() {
	channel := i.Channel()
	key, err := i.Key()
	if err != nil {
		fmt.Println(err)
		return
	}
	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxBlockNumTxNum][%s] key:  %s, value: %s\n", channel, key, value)
}

func (i IdxBlockNumTxNum) Type() int {
	return BlockNumTxNum
}

// IdxBlockByTx is index for searching file and offset where the block exists to which the transaction belongs. Prefix: 'b'
type IdxBlockByTx struct {
	key   []byte
	value []byte
}

func (i IdxBlockByTx) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxBlockByTx) Key() (string, error) {

	txid := bytes.SplitN(i.key, []byte{0x00}, 2)[1][1:]

	return fmt.Sprintf("%s", txid), nil
}

func (i IdxBlockByTx) Value() (string, error) {
	txFLP := fileLocPointer{}
	err := txFLP.unmarshal(i.value)
	return fmt.Sprintf("[filenumber] %d, [offset] %7d, [bytelength] %d\n", txFLP.fileSuffixNum, txFLP.Offset, txFLP.BytesLength), err
}

func (i IdxBlockByTx) Print() {
	channel := i.Channel()
	key, err := i.Key()
	if err != nil {
		fmt.Println(err)
		return
	}

	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxBlockNumTxNum][%s] key:  %s, value: %s\n", channel, key, value)
}

func (i IdxBlockByTx) Type() int {
	return BlockByTx
}

type IdxBlkMgrInfo struct {
	key   []byte
	value []byte
}

func (i IdxBlkMgrInfo) Channel() string {
	return string(bytes.SplitN(i.key, []byte{0x00}, 2)[0])
}

func (i IdxBlkMgrInfo) Key() (string, error) {
	return "IdxBlkMgrInfo", nil
}

func (i IdxBlkMgrInfo) Value() (string, error) {
	blkFileInfo := &index.BlockfilesInfo{}
	err := blkFileInfo.Unmarshal(i.value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s", blkFileInfo.String()), nil

}

func (i IdxBlkMgrInfo) Print() {
	channel := i.Channel()
	key, err := i.Key()
	if err != nil {
		fmt.Println(err)
		return
	}

	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxBlkMgrInfo][%s] key:  %s, value: %s \n", channel, key, value)
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

func (i IdxCheckPoint) Key() (string, error) {
	return "indexIdxCheckPointKey", nil
}

func (i IdxCheckPoint) Value() (string, error) {
	return fmt.Sprintf("%s", strconv.Itoa(utils.GetInt(i.value))), nil

}

func (i IdxCheckPoint) Print() {
	channel := i.Channel()
	key, err := i.Key()
	if err != nil {
		fmt.Println(err)
		return
	}

	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxCheckPoint][%s] key:  %s, value: %s\n", channel, key, value)
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

func (i IdxFormatKey) Key() (string, error) {
	return "_", nil
}

func (i IdxFormatKey) Value() (string, error) {
	return fmt.Sprintf("%s", i.value), nil
}

func (i IdxFormatKey) Print() {
	channel := i.Channel()
	key, err := i.Key()
	if err != nil {
		fmt.Println(err)
		return
	}

	value, err := i.Value()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("[IdxFormatKey][%s] key:  %s, value: %s\n", channel, key, value)
}

func (i IdxFormatKey) Type() int {
	return FormatKey
}
