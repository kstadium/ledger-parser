package state

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/the-medium/ledger-parser/internal/utils"
)

const (
	ChannelConfig = iota
	SystemPublic
	SystemPrivate
	UserPublic
	UserPrivate
	FormatVersion
	SavePoint
)

type KVSet interface {
	Describe() string
	Key() string
	Print()
	Type() int
	Value() string
}

type ChannelConfigKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv ChannelConfigKV) Describe() string {
	return kv.describe
}

func (kv ChannelConfigKV) Key() string {
	realKey := "CHANNEL_CONFIG_ENV_BYTES"
	return fmt.Sprintf("RealKey: %s\n", realKey)
}

func (kv ChannelConfigKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

func (kv ChannelConfigKV) Type() int {
	return ChannelConfig
}

func (kv ChannelConfigKV) Value() string {
	var versionedValue, _ = utils.DecodeValue(kv.value)

	ccenv := &common.ConfigEnvelope{}
	err := proto.Unmarshal(versionedValue.Value, ccenv)
	if err != nil {
		return err.Error()
	}

	// realValue := ccenv.String()
	b, err := json.MarshalIndent(ccenv, "\t\t", "  ")
	if err != nil {
		return err.Error()
	}
	// return fmt.Sprintf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", realValue, versionedValue.Version.String(), versionedValue.Metadata)
	return fmt.Sprintf("Value\n\tvalue:\n\t\t%s\n\tversion: %s\n\tmetadata:%s\n", b, versionedValue.Version.String(), versionedValue.Metadata)
}

type SystemPublicKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SystemPublicKV) Describe() string {
	return kv.describe
}

func (kv SystemPublicKV) Key() string {
	_, realKey, _ := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])
	return fmt.Sprintf("RealKey: %s\n", realKey)

}

func (kv SystemPublicKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

func (kv SystemPublicKV) Type() int {
	return SystemPublic
}

func (kv SystemPublicKV) Value() string {
	var realValue string
	var err error
	var versionedValue, _ = utils.DecodeValue(kv.value)
	_, realKey, _ := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])

	realValue, err = getValueByInfix([]byte(realKey), versionedValue.Value)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", realValue, versionedValue.Version.String(), versionedValue.Metadata)
}

type SystemPrivateKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SystemPrivateKV) Describe() string {
	return kv.describe
}

func (kv SystemPrivateKV) Key() string {
	_, realKey, pvtPrefix := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])

	subNameSpace := strings.Split(realKey, "/")[0]
	switch pvtPrefix {
	case byte('p'): // privateData
		switch subNameSpace {
		case "namespaces":
			// do nothing
		case "chaincode-sources":
			// do nothing
		default:
			return fmt.Sprintf("unknown subnamespace")
		}

	case byte('h'): // privateDataHash
		realKey = hex.EncodeToString([]byte(realKey))

	default: // publicData
		return fmt.Sprintf("unknown pvtPrefix")
	}

	return fmt.Sprintf("RealKey: %s\n", realKey)

}

func (kv SystemPrivateKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

func (kv SystemPrivateKV) Type() int {
	return SystemPrivate
}

func (kv SystemPrivateKV) Value() string {
	var realValue string
	var err error
	var versionedValue, _ = utils.DecodeValue(kv.value)
	_, realKey, pvtPrefix := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])

	subNameSpace := strings.Split(realKey, "/")[0]
	switch pvtPrefix {
	case byte('p'): // privateData
		switch subNameSpace {
		case "namespaces":
			// do nothing
		case "chaincode-sources":
			// do nothing
		default:
			return fmt.Sprintf("unknown subnamespace")
		}
		realValue, err = getValueByInfix([]byte(realKey), versionedValue.Value)
		if err != nil {
			return err.Error()
		}
	case byte('h'): // privateDataHash
		realKey = hex.EncodeToString([]byte(realKey))
		realValue = hex.EncodeToString(versionedValue.Value)
	default: // publicData
		return fmt.Sprintf("unknown pvtPrefix")
	}

	return fmt.Sprintf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", realValue, versionedValue.Version.String(), versionedValue.Metadata)
}

type UserPublicKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv UserPublicKV) Describe() string {
	return kv.describe
}

func (kv UserPublicKV) Key() string {
	_, realKey, _ := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])
	return fmt.Sprintf("RealKey: %s\n", realKey)
}

func (kv UserPublicKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

func (kv UserPublicKV) Type() int {
	return UserPublic
}

func (kv UserPublicKV) Value() string {
	var versionedValue, _ = utils.DecodeValue(kv.value)
	return fmt.Sprintf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", versionedValue.Value, versionedValue.Version.String(), versionedValue.Metadata)
}

type UserPrivateKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv UserPrivateKV) Describe() string {
	return kv.describe
}

func (kv UserPrivateKV) Key() string {
	_, realKey, pvtPrefix := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])
	switch pvtPrefix {
	case byte('p'): // privateData
		// do nothing
	case byte('h'): // privateDataHash
		realKey = hex.EncodeToString([]byte(realKey))
	default: // publicData?
		return "unknown pvtPrefix"
	}
	return fmt.Sprintf("RealKey: %s\n", realKey)
}

func (kv UserPrivateKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

func (kv UserPrivateKV) Type() int {
	return UserPrivate
}

func (kv UserPrivateKV) Value() string {
	var realValue string = ""
	_, _, pvtPrefix := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])
	var versionedValue, err = utils.DecodeValue(kv.value)
	if err != nil {
		return err.Error()
	}
	switch pvtPrefix {
	case byte('p'): // privateData
		realValue = fmt.Sprintf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", versionedValue.Value, versionedValue.Version.String(), versionedValue.Metadata)
	case byte('h'): // privateDataHash
		realValue = fmt.Sprintf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", hex.EncodeToString(versionedValue.Value), versionedValue.Version.String(), versionedValue.Metadata)
	default: // publicData?
		return "unknown pvtPrefix"
	}

	return realValue

}

type FormatVersionKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv FormatVersionKV) Describe() string {
	return kv.describe
}

func (kv FormatVersionKV) Key() string {
	realKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]
	return fmt.Sprintf("RealKey: %s\n", realKey)
}

func (kv FormatVersionKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

func (kv FormatVersionKV) Type() int {
	return FormatVersion
}

func (kv FormatVersionKV) Value() string {
	return fmt.Sprintf("Value\n\tvalue: %s\n", string(kv.value))

}

type SavePointKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SavePointKV) Describe() string {
	return kv.describe
}

func (kv SavePointKV) Key() string {
	realKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]
	return fmt.Sprintf("RealKey: %s\n", realKey)
}

func (kv SavePointKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

func (kv SavePointKV) Type() int {
	return SavePoint
}

func (kv SavePointKV) Value() string {
	h, _, _ := utils.NewHeightFromBytes(kv.value)
	realValue := fmt.Sprintf("BlockNum : %d\n\tTxNum : %d", h.BlockNum, h.TxNum)
	return fmt.Sprintf("Value\n\t%s\n", realValue)
}
