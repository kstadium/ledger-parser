package utils

import (
	"encoding/json"
	"fmt"

	proto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/stateleveldb/msgs"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
)

func toMap(i interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{}
	json.Unmarshal(b, &out)

	return out, nil
}

func printMap(i interface{}) {

	b, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}

func GetInt(s []byte) int {
	var res int
	for _, v := range s {
		res <<= 8
		res |= int(v)
	}
	return res
}

// Height represents the height of a transaction in blockchain
type Height struct {
	BlockNum uint64
	TxNum    uint64
}

// NewHeight constructs a new instance of Height
func NewHeight(blockNum, txNum uint64) *Height {
	return &Height{blockNum, txNum}
}

func NewHeightFromBytes(b []byte) (*Height, int, error) {
	blockNum, n1, err := util.DecodeOrderPreservingVarUint64(b)
	if err != nil {
		return nil, -1, err
	}
	txNum, n2, err := util.DecodeOrderPreservingVarUint64(b[n1:])
	if err != nil {
		return nil, -1, err
	}
	return NewHeight(blockNum, txNum), n1 + n2, nil
}

func DecomposeVersionedValue(versionedValue *statedb.VersionedValue) []byte {
	var value []byte
	if versionedValue != nil {
		value = versionedValue.Value
	}
	return value
}

// decodeValue decodes the statedb value bytes
func DecodeValue(encodedValue []byte) (*statedb.VersionedValue, error) {
	// stateleveldb.DBValue(2.2.1) == msg.VersionedValueProto(2.1.1)
	dbValue := &msgs.VersionedValueProto{}
	err := proto.Unmarshal(encodedValue, dbValue)
	if err != nil {
		return nil, err
	}
	ver, _, err := version.NewHeightFromBytes(dbValue.VersionBytes)
	if err != nil {
		return nil, err
	}
	val := dbValue.Value
	metadata := dbValue.Metadata
	// protobuf always makes an empty byte array as nil
	if val == nil {
		val = []byte{}
	}
	return &statedb.VersionedValue{Version: ver, Value: val, Metadata: metadata}, nil
}

// Serialize serializes metadata entries for storing in statedb
func Serialize(metadataEntries []*kvrwset.KVMetadataEntry) ([]byte, error) {
	metadata := &kvrwset.KVMetadataWrite{Entries: metadataEntries}
	return proto.Marshal(metadata)
}

// Deserialize deserializes metadata bytes from statedb
func Deserialize(metadataBytes []byte) (map[string][]byte, error) {
	if metadataBytes == nil {
		return nil, nil
	}
	metadata := &kvrwset.KVMetadataWrite{}
	if err := proto.Unmarshal(metadataBytes, metadata); err != nil {
		return nil, err
	}
	m := make(map[string][]byte, len(metadata.Entries))
	for _, metadataEntry := range metadata.Entries {
		m[metadataEntry.Name] = metadataEntry.Value
	}
	return m, nil
}
