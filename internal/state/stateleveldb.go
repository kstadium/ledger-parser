/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"

	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("stateleveldb")

var (
	dataKeyPrefix               = []byte{'d'}
	dataKeyStopper              = []byte{'e'}
	nsKeySep                    = []byte{0x00}
	lastKeyIndicator            = byte(0x01)
	savePointKey                = []byte{'s'}
	fullScanIteratorValueFormat = byte(1)
)

func encodeDataKey(ns, key string) []byte {
	k := append(dataKeyPrefix, []byte(ns)...)
	k = append(k, nsKeySep...)
	return append(k, []byte(key)...)
}

func decodeDataKey(encodedDataKey []byte) (string, string) {
	split := bytes.SplitN(encodedDataKey, nsKeySep, 2)
	return string(split[0][1:]), string(split[1])
}

func dataKeyStarterForNextNamespace(ns string) []byte {
	k := append(dataKeyPrefix, []byte(ns)...)
	return append(k, lastKeyIndicator)
}
