package state

import (
	"bytes"
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

func getDataNSKey(prefixedKey []byte) (namespace string, realKey string, pvtDataPrefix byte) {
	key := prefixedKey[1:]
	splited := bytes.SplitN(key, []byte{0x00}, 2)
	bNamespace := splited[0]
	if isPvtdataNs(bNamespace) { // has private data collection
		splitedNS := bytes.SplitN(bNamespace, []byte("$$"), 2)
		bNamespace = append(splitedNS[0], splitedNS[1][1:]...)
		pvtDataPrefix = splitedNS[1][0]
	}

	namespace = string(bNamespace)
	realKey = string(splited[1])
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

func handleSCCKV(key []byte, value []byte) (kvSet KVSet, err error) {
	_, realKey, pvtPrefix := getDataNSKey(bytes.SplitN(key, []byte{0x00}, 2)[1])
	switch pvtPrefix {
	case byte('p'): // privateData
		subNameSpace := strings.Split(realKey, "/")[0]
		var desc string
		switch subNameSpace {
		case "namespaces":
			desc = "System Chaincode privateData - namespace"
		case "chaincode-sources":
			desc = "System Chaincode privateData - chaincode sources"
		default:
			err = fmt.Errorf("unknown subnamespace")
			return
		}
		kvSet = &SystemPrivateKV{key, value, desc}
	case byte('h'): // privateDataHash
		kvSet = &SystemPrivateKV{key, value, "System Chaincode privateDataHash"}
	default: // publicData
		kvSet = &SystemPublicKV{key, value, "System Chaincode publicData"}
	}
	return
}

func handleUCCKV(key []byte, value []byte) (kvSet KVSet) {
	_, _, pvtPrefix := getDataNSKey(bytes.SplitN(key, []byte{0x00}, 2)[1])

	switch pvtPrefix {
	case byte('p'): // privateData
		kvSet = &UserPrivateKV{key, value, "User Chaincode privateData"}
	case byte('h'): // privateDataHash
		kvSet = &UserPrivateKV{key, value, "User Chaincode privateDataHash"}
	default: // publicData
		kvSet = &UserPublicKV{key, value, "User Chaincode publicData"}
	}
	return
}

// ParseKV returns kvSet that converts byte represented KV to human readable string.
// if empty string is given for parameter channel, it parses for KV from all channels.
func ParseKV(key []byte, value []byte, channel string) (kvSet KVSet, err error) {
	nsKey := bytes.SplitN(key, []byte{0x00}, 2)
	if string(nsKey[0]) != channel && channel != "" {
		return nil, nil
	}

	internalKey := nsKey[1]
	prefix := internalKey[0]
	switch prefix {
	case byte(0x64): // 'd' : dataKeyPrefix
		ns, realKey, _ := getDataNSKey(internalKey)

		if len(sccMatcher.FindStringSubmatch(ns)) != 0 { // system chaincode
			kvSet, err = handleSCCKV(key, value)
			if err != nil {
				return
			}
		} else if realKey == "CHANNEL_CONFIG_ENV_BYTES" {
			kvSet = &ChannelConfigKV{key, value, "channel config data"}
		} else { // user chaincode
			if strings.Contains(realKey, "\x00"+string(utf8.MaxRune)+"initialized") {
				kvSet = &UserPublicKV{key, value, "User Chaincode initialized"}
			} else {
				kvSet = handleUCCKV(key, value)
			}
		}
	case byte(0x66): // 'f' : formatVersionKey
		kvSet = &FormatVersionKV{key, value, "formatVersionKey"}
	case byte(0x73): // 's' : savePointKey
		kvSet = &SavePointKV{key, value, "savePointKey"}
	default:
		return
	}
	return
}
