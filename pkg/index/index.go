package index

import (
	"bytes"
	"fmt"
)

func ParseKV(key []byte, value []byte) (idxKV IndexKV) {

	keys := bytes.SplitN(key, []byte{0x00}, 2)
	internalKey := keys[1]
	prefix := internalKey[0]

	switch prefix {
	case byte(0x6e): // 'n' : blockNumIdxKeyPrefix
		idxKV = &IdxBlockNum{key, value}
	case byte(0x68): // 'h' : blockHashIdxKeyPrefix
		idxKV = &IdxBlockHash{key, value}
	case byte(0x74): // 't' : txIDIdxKeyPrefix
		idxKV = &IdxTxID{key, value}
	case byte(0x61): // 'a' : blockNumTranNumIdxKeyPrefix
		idxKV = &IdxBlockNumTxNum{key, value}
	case byte(0x62): // 'b' : blockTxIDIdxKeyPrefix
		if bytes.Equal(keys[1], []byte("blkMgrInfo")) { // indexCheckpointKey
			idxKV = &IdxBlkMgrInfo{key, value}
		} else {
			fmt.Println("unknown prefix starting with 'b'")
			idxKV = nil
		}
	case byte(0x66):
		idxKV = &IdxFormatKey{key, value}
	default:
		if bytes.Equal(keys[1], []byte("indexCheckpointKey")) { // indexCheckpointKey
			idxKV = &IdxCheckPoint{key, value}
		} else if bytes.Equal(keys[1], []byte("blkMgrInfo")) { // indexCheckpointKey
			idxKV = &IdxBlkMgrInfo{key, value}
		} else {
			fmt.Println("unknown prefix")
			idxKV = nil
		}
	}
	return idxKV
}
