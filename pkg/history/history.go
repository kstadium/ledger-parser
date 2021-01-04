package history

import (
	"bytes"
)

func ParseKV(key []byte, value []byte) (kvSet KVSet, err error) {
	channel := string(bytes.SplitN(key, []byte{0x00}, 2)[0])
	ccInternalKey := bytes.SplitN(key, []byte{0x00}, 2)[1]

	if channel == "_" {
		// formatKey
		kvSet = &FormatVersionKV{key, value}
	} else if bytes.Compare(ccInternalKey, []byte{0x73}) == 0 {
		kvSet = &SavePointKV{key, value}
	} else {
		kvSet = &GeneralKV{key, value}
	}
	return kvSet, nil
}
