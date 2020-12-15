package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestParseKV(t *testing.T) {
	path := "/var/hyperledger/production/ledgersData/chains/index/"
	opts := opt.Options{}
	opts.ErrorIfMissing = true
	db, err := leveldb.OpenFile(path, &opts)
	assert.NoError(t, err)
	defer db.Close()

	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		var idxKV IndexKV = ParseKV(iter.Key(), iter.Value())
		if idxKV == nil {
			fmt.Println("nil idxKV")
			break
		}
		idxKV.Print()
	}
	iter.Release()
	err = iter.Error()
	assert.NoError(t, err)
}
