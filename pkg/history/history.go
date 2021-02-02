package history

import (
	"bytes"
)

func ParseKV(key []byte, value []byte, channel string) (kvSet KVSet, err error) {
	nsKey := bytes.SplitN(key, []byte{0x00}, 2)
	if string(nsKey[0]) != channel && channel != "" {
		return nil, nil
	}

	ccInternalKey := nsKey[1]

	if string(nsKey[0]) == "_" {
		// formatKey
		kvSet = &FormatVersionKV{key, value}
	} else if bytes.Compare(ccInternalKey, []byte{0x73}) == 0 {
		kvSet = &SavePointKV{key, value}
	} else if bytes.Contains(ccInternalKey, []byte(InitializedKeyName)) {
		kvSet = &InitializedKV{key, value}
	} else {
		kvSet = &GeneralKV{key, value}
	}
	return kvSet, nil
}
