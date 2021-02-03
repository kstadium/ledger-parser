package pvtdata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestParseKV(t *testing.T) {
	path := "/var/hyperledger/production/ledgersData/pvtdataStore/"
	opts := opt.Options{}
	opts.ErrorIfMissing = true
	db, err := leveldb.OpenFile(path, &opts)
	assert.NoError(t, err)
	defer db.Close()

	iter := db.NewIterator(nil, nil)

	for iter.Next() {
		kvSet, err := ParseKV(iter.Key(), iter.Value(), "")
		assert.NoError(t, err)
		kvSet.Print()
	}

	iter.Release()
	err = iter.Error()
	assert.NoError(t, err)
}
