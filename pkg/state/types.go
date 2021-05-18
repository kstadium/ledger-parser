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
	Key() []byte
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

func (kv ChannelConfigKV) Key() []byte {
	return kv.key
}

func (kv ChannelConfigKV) Print() {
	realKey := "CHANNEL_CONFIG_ENV_BYTES"

	var versionedValue, _ = utils.DecodeValue(kv.value)

	ccenv := &common.ConfigEnvelope{}
	err := proto.Unmarshal(versionedValue.Value, ccenv)
	if err != nil {
		fmt.Println(err)
		return
	}

	// realValue := ccenv.String()
	b, err := json.MarshalIndent(ccenv, "\t\t", "  ")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("<%s>\n", kv.describe)
	fmt.Printf("channel: %s\n", bytes.SplitN(kv.key, []byte{0x00}, 2)[0])
	fmt.Printf("RealKey: %s\n", realKey)
	fmt.Printf("Value\n\tvalue:\n\t\t%s\n\tversion: %s\n\tmetadata:%s\n", b, versionedValue.Version.String(), versionedValue.Metadata)
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

	b, err := json.MarshalIndent(ccenv, "\t\t", "  ")
	if err != nil {
		return err.Error()
	}

	return string(b)
}

type SystemPublicKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SystemPublicKV) Describe() string {
	return kv.describe
}

func (kv SystemPublicKV) Key() []byte {
	return kv.key
}

func (kv SystemPublicKV) Print() {
	var realValue string
	var err error
	var versionedValue, _ = utils.DecodeValue(kv.value)
	_, realKey, _ := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])

	realValue, err = getValueByInfix([]byte(realKey), versionedValue.Value)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("<%s>\n", kv.describe)
	fmt.Printf("channel: %s\n", bytes.SplitN(kv.key, []byte{0x00}, 2)[0])
	fmt.Printf("RealKey: %s\n", realKey)
	fmt.Printf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", realValue, versionedValue.Version.String(), versionedValue.Metadata)
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
	return realValue
}

type SystemPrivateKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SystemPrivateKV) Describe() string {
	return kv.describe
}

func (kv SystemPrivateKV) Key() []byte {
	return kv.key
}

func (kv SystemPrivateKV) Print() {
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
			fmt.Printf("unknown subnamespace")
			return
		}
		realValue, err = getValueByInfix([]byte(realKey), versionedValue.Value)
		if err != nil {
			fmt.Println(err)
			return
		}
	case byte('h'): // privateDataHash
		realKey = hex.EncodeToString([]byte(realKey))
		realValue = hex.EncodeToString(versionedValue.Value)
	default: // publicData
		fmt.Printf("unknown pvtPrefix")
		return
	}

	fmt.Printf("<%s>\n", kv.describe)
	fmt.Printf("channel: %s\n", bytes.SplitN(kv.key, []byte{0x00}, 2)[0])
	fmt.Printf("RealKey: %s\n", realKey)
	fmt.Printf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", realValue, versionedValue.Version.String(), versionedValue.Metadata)
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
			return "unknown subnamespace"
		}
		realValue, err = getValueByInfix([]byte(realKey), versionedValue.Value)
		if err != nil {
			return err.Error()
		}
	case byte('h'): // privateDataHash
		realValue = hex.EncodeToString(versionedValue.Value)
	default: // publicData
		return "unknown pvtPrefix"
	}

	return realValue
}

type UserPublicKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv UserPublicKV) Describe() string {
	return kv.describe
}

func (kv UserPublicKV) Key() []byte {
	return kv.key
}

func (kv UserPublicKV) Print() {
	_, realKey, _ := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])
	var versionedValue, _ = utils.DecodeValue(kv.value)

	fmt.Printf("<%s>\n", kv.describe)
	fmt.Printf("channel: %s\n", bytes.SplitN(kv.key, []byte{0x00}, 2)[0])
	fmt.Printf("RealKey: %s\n", realKey)
	metaResult, err := utils.Deserialize(versionedValue.Metadata)
	if err != nil {
		fmt.Printf("Deserialize metadata err: %s\n", err)
	}
	if len(metaResult) == 0 {
		fmt.Printf("Value\n\tvalue: %s\n\tversion: %s\n", versionedValue.Value, versionedValue.Version.String())
		fmt.Println()
		return
	}
	msgMetadata := ""
	for key, value := range metaResult {
		if key == "VALIDATION_PARAMETER" {
			spe := &common.SignaturePolicyEnvelope{}
			err := proto.Unmarshal(value, spe)
			if err != nil {
				fmt.Printf("Unmarshal Validation Parameter Policy err : %s\n", err)
				return
			}
			value = []byte(spe.String())
		}
		msgMetadata = fmt.Sprintf("key: %s, value: %s", key, value)
	}
	fmt.Printf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:\n\t\t%s\n", versionedValue.Value, versionedValue.Version.String(), msgMetadata)
	fmt.Println()
}

func (kv UserPublicKV) Type() int {
	return UserPublic
}

func (kv UserPublicKV) Value() string {
	var versionedValue, _ = utils.DecodeValue(kv.value)
	return string(versionedValue.Value)
}

type UserPrivateKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv UserPrivateKV) Describe() string {
	return kv.describe
}

func (kv UserPrivateKV) Key() []byte {
	return kv.key
}

func (kv UserPrivateKV) Print() {
	var realValue string = ""
	_, realKey, pvtPrefix := getDataNSKey(bytes.SplitN(kv.key, []byte{0x00}, 2)[1])
	var versionedValue, err = utils.DecodeValue(kv.value)
	if err != nil {
		fmt.Println(err)
		return
	}
	switch pvtPrefix {
	case byte('p'): // privateData
		realValue = string(versionedValue.Value)
	case byte('h'): // privateDataHash
		realKey = hex.EncodeToString([]byte(realKey))
		realValue = hex.EncodeToString(versionedValue.Value)
	default: // publicData?
		fmt.Println("unknown pvtPrefix")
		return
	}

	metaResult, err := utils.Deserialize(versionedValue.Metadata)
	if err != nil {
		fmt.Printf("Deserialize metadata err: %s\n", err)
	}

	fmt.Printf("<%s>\n", kv.describe)
	fmt.Printf("channel: %s\n", bytes.SplitN(kv.key, []byte{0x00}, 2)[0])
	fmt.Printf("RealKey: %s\n", realKey)
	if len(metaResult) == 0 {
		fmt.Printf("Value\n\tvalue: %s\n\tversion: %s\n", realValue, versionedValue.Version.String())
		fmt.Println()
		return
	}

	msgMetadata := ""
	for key, value := range metaResult {
		if key == "VALIDATION_PARAMETER" {
			spe := &common.SignaturePolicyEnvelope{}
			err := proto.Unmarshal(value, spe)
			if err != nil {
				fmt.Printf("Unmarshal Validation Parameter Policy err : %s\n", err)
				return
			}
			value = []byte(spe.String())
		}
		msgMetadata = fmt.Sprintf("key: %s, value: %s", key, value)
	}

	fmt.Printf("Value\n\tvalue: %s\n\tversion: %s\n\tmetadata:%s\n", realValue, versionedValue.Version.String(), msgMetadata)
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
		realValue = string(versionedValue.Value)
	case byte('h'): // privateDataHash
		realValue = hex.EncodeToString(versionedValue.Value)
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

func (kv FormatVersionKV) Key() []byte {
	return kv.key
}

func (kv FormatVersionKV) Print() {
	realKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]

	fmt.Printf("<%s>\n", kv.describe)
	fmt.Printf("channel: %s\n", bytes.SplitN(kv.key, []byte{0x00}, 2)[0])
	fmt.Printf("RealKey: %s\n", realKey)
	fmt.Printf("Value\n\tvalue: %s\n", string(kv.value))

}

func (kv FormatVersionKV) Type() int {
	return FormatVersion
}

func (kv FormatVersionKV) Value() string {
	return string(kv.value)

}

type SavePointKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SavePointKV) Describe() string {
	return kv.describe
}

func (kv SavePointKV) Key() []byte {
	return kv.key
}

func (kv SavePointKV) Print() {
	realKey := bytes.SplitN(kv.key, []byte{0x00}, 2)[1]
	h, _, _ := utils.NewHeightFromBytes(kv.value)
	realValue := fmt.Sprintf("BlockNum : %d\n\tTxNum : %d", h.BlockNum, h.TxNum)

	fmt.Printf("<%s>\n", kv.describe)
	fmt.Printf("channel: %s\n", bytes.SplitN(kv.key, []byte{0x00}, 2)[0])

	fmt.Printf("RealKey: %s\n", realKey)
	fmt.Printf("Value\n\t%s\n", realValue)
}

func (kv SavePointKV) Type() int {
	return SavePoint
}

func (kv SavePointKV) Value() string {
	h, _, _ := utils.NewHeightFromBytes(kv.value)
	return fmt.Sprintf("BlockNum : %d\n\tTxNum : %d", h.BlockNum, h.TxNum)
}
