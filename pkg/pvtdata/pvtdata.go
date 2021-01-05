package pvtdata

import (
	"bytes"
	"fmt"
)

var (
	PendingCommitKey               = byte(0)
	LastCommittedBlkkey            = byte(0x01)
	PvtDataKeyPrefix               = byte(2)
	ExpiryKeyPrefix                = byte(3)
	EligibleMissingDataKeyPrefix   = byte(4)
	IneligibleMissingDataKeyPrefix = byte(5)
	CollElgKeyPrefix               = byte(6)
	LastUpdatedOldBlocksKey        = byte(7)

	NilByte    = byte(0)
	EmptyValue = []byte{}
)

func ParseKV(key []byte, value []byte) (KVSet, error) {
	prefix := bytes.SplitN(key, []byte{0x00}, 2)[1][0]
	var kvSet KVSet
	var err error
	switch prefix {
	case PendingCommitKey:
		// FAB-16298 -- the concept of pendingBatch is no longer valid
		// for pvtdataStore. We can remove it v2.1. We retain the concept in
		// v2.0 to allow rolling upgrade from v142 to v2.0
	case LastCommittedBlkkey:
		kvSet = LastCommittedBlockKV{key, value}
	case PvtDataKeyPrefix:
		kvSet = PvtDataKV{key, value}
	// TODO
	case ExpiryKeyPrefix:
		err = fmt.Errorf("expiryKeyPrefix")
	case EligibleMissingDataKeyPrefix:
		err = fmt.Errorf("eligibleMissingDataKeyPrefix")
	case IneligibleMissingDataKeyPrefix:
		err = fmt.Errorf("ineligibleMissingDataKeyPrefix")
	case CollElgKeyPrefix:
		err = fmt.Errorf("collElgKeyPrefix")
	case LastUpdatedOldBlocksKey:
		err = fmt.Errorf("lastUpdatedOldBlocksKey")
	}
	return kvSet, err
}
