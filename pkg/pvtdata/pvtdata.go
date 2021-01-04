package pvtdata

import (
	"bytes"
	"fmt"
)

var (
	pendingCommitKey               = byte(0)
	lastCommittedBlkkey            = byte(0x01)
	pvtDataKeyPrefix               = byte(2)
	expiryKeyPrefix                = byte(3)
	eligibleMissingDataKeyPrefix   = byte(4)
	ineligibleMissingDataKeyPrefix = byte(5)
	collElgKeyPrefix               = byte(6)
	lastUpdatedOldBlocksKey        = byte(7)

	nilByte    = byte(0)
	emptyValue = []byte{}
)

func ParseKV(key []byte, value []byte) (KVSet, error) {
	prefix := bytes.SplitN(key, []byte{0x00}, 2)[1][0]
	var kvSet KVSet
	var err error
	switch prefix {
	case pendingCommitKey:
		// FAB-16298 -- the concept of pendingBatch is no longer valid
		// for pvtdataStore. We can remove it v2.1. We retain the concept in
		// v2.0 to allow rolling upgrade from v142 to v2.0
	case lastCommittedBlkkey:
		kvSet = LastCommittedBlockKV{key, value}
	case pvtDataKeyPrefix:
		kvSet = PvtDataKV{key, value}
	// TODO
	case expiryKeyPrefix:
		err = fmt.Errorf("expiryKeyPrefix")
	case eligibleMissingDataKeyPrefix:
		err = fmt.Errorf("eligibleMissingDataKeyPrefix")
	case ineligibleMissingDataKeyPrefix:
		err = fmt.Errorf("ineligibleMissingDataKeyPrefix")
	case collElgKeyPrefix:
		err = fmt.Errorf("collElgKeyPrefix")
	case lastUpdatedOldBlocksKey:
		err = fmt.Errorf("lastUpdatedOldBlocksKey")
	}
	return kvSet, err
}
