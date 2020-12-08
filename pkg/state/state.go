package state

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"

	goproto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	lb "github.com/hyperledger/fabric-protos-go/peer/lifecycle"
	"github.com/hyperledger/fabric/core/chaincode/lifecycle"
	"github.com/pkg/errors"
	"github.com/the-medium/ledger-parser/internal/utils"
)

const (
	nsJoiner       = "$$"
	pvtDataPrefix  = "p"
	hashDataPrefix = "h"
	couchDB        = "CouchDB"
)

var sccMatcher = regexp.MustCompile("^" + "_lifecycle")
var ProtoMessageType = reflect.TypeOf((*goproto.Message)(nil)).Elem()

func SerializableChecks(structure interface{}) (reflect.Value, []string, error) {
	value := reflect.ValueOf(structure)
	if value.Kind() != reflect.Ptr {
		return reflect.Value{}, nil, errors.Errorf("must be pointer to struct, but got non-pointer %v", value.Kind())
	}

	value = value.Elem()
	if value.Kind() != reflect.Struct {
		return reflect.Value{}, nil, errors.Errorf("must be pointers to struct, but got pointer to %v", value.Kind())
	}

	allFields := make([]string, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		fieldName := value.Type().Field(i).Name
		fieldValue := value.Field(i)
		allFields[i] = fieldName
		switch fieldValue.Kind() {
		case reflect.String:
		case reflect.Int64:
		case reflect.Slice:
			if fieldValue.Type().Elem().Kind() != reflect.Uint8 {
				return reflect.Value{}, nil, errors.Errorf("unsupported slice type %v for field %s", fieldValue.Type().Elem().Kind(), fieldName)
			}
		case reflect.Ptr:
			if !fieldValue.Type().Implements(ProtoMessageType) {
				return reflect.Value{}, nil, errors.Errorf("unsupported pointer type %v for field %s (must be proto)", fieldValue.Type().Elem(), fieldName)
			}
		default:
			return reflect.Value{}, nil, errors.Errorf("unsupported structure field kind %v for serialization for field %s", fieldValue.Kind(), fieldName)
		}
	}
	return value, allFields, nil
}

func isPvtdataNs(namespace []byte) bool {
	return strings.Contains(string(namespace), nsJoiner)
}

func getDataNSKey(prefixedKey []byte, value []byte) (namespace []byte, realKey []byte, pvtDataPrefix byte) {
	key := prefixedKey[1:]
	splited := bytes.SplitN(key, []byte{0x00}, 2)
	namespace = splited[0]
	realKey = splited[1]
	if isPvtdataNs(namespace) { // has private data collection
		splitedNS := bytes.SplitN(namespace, []byte("$$"), 2)
		namespace = append(splitedNS[0], splitedNS[1][1:]...)
		pvtDataPrefix = splitedNS[1][0]
	}
	return
}

func getValueByInfix(key []byte, value []byte) (realValue string, err error) {
	infix := strings.Split(string(key), "/")[1]
	switch infix {
	case "fields":
		chunks := strings.Split(string(key), "/")
		switch chunks[len(chunks)-1] {
		case "Collections":
			stateData := &lb.StateData{}
			goproto.Unmarshal(value, stateData)
			oneOf, ok := stateData.Type.(*lb.StateData_Bytes)
			if !ok {
				err = errors.Errorf("expected key to encode a value of type []byte, but was %T", stateData.Type)
				return
			}
			ccParameters := &lifecycle.ChaincodeParameters{}
			svalue, _, _ := SerializableChecks(ccParameters)
			fieldValue := svalue.FieldByName("Collections")
			msg := reflect.New(fieldValue.Type().Elem())

			err = goproto.Unmarshal(oneOf.Bytes, msg.Interface().(goproto.Message))
			if err != nil {
				return
			}
			collections := msg.Interface().(*pb.CollectionConfigPackage)

			realValue = fmt.Sprintf("%v", collections.Config)
		case "EndorsementInfo":
			stateData := &lb.StateData{}
			goproto.Unmarshal(value, stateData)
			oneOf, ok := stateData.Type.(*lb.StateData_Bytes)
			if !ok {
				err = errors.Errorf("expected key to encode a value of type []byte, but was %T", stateData.Type)
				return
			}
			ccParameters := &lifecycle.ChaincodeParameters{}
			svalue, _, _ := SerializableChecks(ccParameters)
			fieldValue := svalue.FieldByName("EndorsementInfo")
			msg := reflect.New(fieldValue.Type().Elem())
			err = goproto.Unmarshal(oneOf.Bytes, msg.Interface().(goproto.Message))
			if err != nil {
				return
			}
			cvInfo := msg.Interface().(*lb.ChaincodeEndorsementInfo)

			realValue = fmt.Sprintf("%s", cvInfo.String())

		case "ValidationInfo":
			stateData := &lb.StateData{}
			goproto.Unmarshal(value, stateData)
			oneOf, ok := stateData.Type.(*lb.StateData_Bytes)
			if !ok {
				err = errors.Errorf("expected key to encode a value of type []byte, but was %T", stateData.Type)
				return
			}
			ccParameters := &lifecycle.ChaincodeParameters{}
			svalue, _, _ := SerializableChecks(ccParameters)
			fieldValue := svalue.FieldByName("ValidationInfo")
			msg := reflect.New(fieldValue.Type().Elem())

			err = goproto.Unmarshal(oneOf.Bytes, msg.Interface().(goproto.Message))
			if err != nil {
				return
			}
			vinfo := msg.Interface().(*lb.ChaincodeValidationInfo)
			policy := &common.ApplicationPolicy{}
			err = goproto.Unmarshal(vinfo.ValidationParameter, policy)
			if err != nil {
				return
			}
			realValue = fmt.Sprintf("%s", policy.String())
		case "PackageID":
			stateData := &lb.StateData{}
			err = goproto.Unmarshal(value, stateData)
			if err != nil {
				return
			}
			realValue = fmt.Sprintf("%s", stateData.GetString_())
		case "Sequence":
			stateData := &lb.StateData{}
			err = goproto.Unmarshal(value, stateData)
			if err != nil {
				return
			}
			realValue = fmt.Sprintf("%s", stateData.String())
		default:
			err = fmt.Errorf("unknown field ... %s", chunks[len(chunks)-1])
		}
	case "metadata":
		metaData := &lb.StateMetadata{}
		err = goproto.Unmarshal(value, metaData)
		if err != nil {
			return
		}
		realValue = fmt.Sprintf("%s", metaData.String())
	default:
		err = fmt.Errorf("Unknown Infix")
	}
	return
}

func handleSCCKV(key []byte, value []byte, pvtPrefix byte) (subNameSpace, realKey, realValue string, err error) {
	switch pvtPrefix {
	case byte('p'): // privateData
		subNameSpace := strings.Split(string(key), "/")[0]
		switch subNameSpace {
		case "namespaces":
			realKey = string(key)
			realValue, err = getValueByInfix(key, value)

		case "chaincode-sources":
			realKey = string(key)
			realValue, err = getValueByInfix(key, value)
		default:
			err = fmt.Errorf("unknown subnamespace")
		}
	case byte('h'): // privateDataHash
		realKey = hex.EncodeToString(key)
		realValue = hex.EncodeToString(value)
	default: // publicData
		realValue, err = getValueByInfix(key, value)
		realKey = string(key)
	}
	return
}

func handleUCCKV(key []byte, value []byte, pvtPrefix byte) (realKey, realValue string, err error) {
	switch pvtPrefix {
	case byte('p'): // privateData
		realValue = string(value)
		realKey = string(key)
	case byte('h'): // privateDataHash
		realKey = hex.EncodeToString(key)
		realValue = hex.EncodeToString(value)
	default: // publicData
		realValue = string(value)
		realKey = string(key)
	}
	return
}

func getKV(key []byte, value []byte) (msg string, err error) {
	var (
		ns        string
		subNS     string
		realKey   string
		realValue string
	)
	splitedKey := bytes.SplitN(key, []byte{0x00}, 2)
	dbName := splitedKey[0] // channelName
	internalKey := splitedKey[1]
	var versionedValue, _ = utils.DecodeValue(value)
	prefix := internalKey[0]
	switch prefix {
	case byte(0x64): // 'd' : dataKeyPrefix
		bNS, bRealKey, isPvtData := getDataNSKey(internalKey, versionedValue.Value)
		ns = string(bNS)
		realKey = string(bRealKey)
		if len(sccMatcher.FindStringSubmatch(ns)) != 0 { // system chaincode
			subNS, realKey, realValue, err = handleSCCKV(bRealKey, versionedValue.Value, isPvtData)
			if err != nil {
				return
			}
			ns = ns + subNS

		} else if realKey == "CHANNEL_CONFIG_ENV_BYTES" {
			// realValue = "omitted"
			realValue = string(versionedValue.Value)

		} else { // user chaincode
			if strings.Contains(realKey, "\x00"+string(utf8.MaxRune)+"initialized") {
				realKey = "initialized"
				realValue = string(versionedValue.Value)
			} else {
				realKey, realValue, err = handleUCCKV(bRealKey, versionedValue.Value, isPvtData)
				if err != nil {
					return
				}
			}

		}
	case byte(0x66): // 'f' : formatVersionKey
		realKey = string(internalKey) + " (FormatVersion Key)"
		realValue = string(value)
	case byte(0x73): // 's' : savePointKey
		realKey = string(internalKey) + " (SavePoint Key)"
		h, _, _ := utils.NewHeightFromBytes(value)
		realValue = fmt.Sprintf("BlockNum %d, TxNum %d", h.BlockNum, h.TxNum)

	default:
	}
	msg = fmt.Sprintf("[%s] %s / %s \nvalue : %s \n", dbName, ns, realKey, realValue)
	return
}
