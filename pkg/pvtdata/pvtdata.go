package pvtdata

import (
	"bytes"
	"fmt"
)

var (
	PendingCommitKey                      = byte(0)
	LastCommittedBlkkey                   = byte(1)
	PvtDataKeyPrefix                      = byte(2)
	ExpiryKeyPrefix                       = byte(3)
	EligiblePrioritizedMissingDataGroup   = byte(4)
	IneligibleMissingDataKeyGroup         = byte(5)
	CollEligibleKeyPrefix                 = byte(6)
	LastUpdatedOldBlocksKey               = byte(7)
	EligibleDeprioritizedMissingDataGroup = byte(8)

	NilByte    = byte(0)
	EmptyValue = []byte{}
)

func ParseKV(key []byte, value []byte, channel string) (KVSet, error) {
	nsKey := bytes.SplitN(key, []byte{0x00}, 2)
	if string(nsKey[0]) != channel && channel != "" {
		return nil, nil
	}

	prefix := nsKey[1][0]
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
	case EligiblePrioritizedMissingDataGroup:
		err = fmt.Errorf("eligibleMissingDataKeyPrefix")
	case IneligibleMissingDataKeyGroup:
		kvSet = IneligibleMissingDataKV{key, value}

	case CollEligibleKeyPrefix:
		err = fmt.Errorf("collElgKeyPrefix")
	case LastUpdatedOldBlocksKey:
		err = fmt.Errorf("lastUpdatedOldBlocksKey")
	case EligibleDeprioritizedMissingDataGroup:
		err = fmt.Errorf("EligibleDeprioritizedMissingDataGroup")
	default:
		err = fmt.Errorf("unknown prefix")
	}
	return kvSet, err
}
