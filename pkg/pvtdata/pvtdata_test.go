package pvtdata

import (
	"fmt"
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestMain(m *testing.M) {
	path := "/var/hyperledger/production/ledgersData/pvtdataStore/"
	opts := opt.Options{}
	opts.ErrorIfMissing = true
	db, err := leveldb.OpenFile(path, &opts)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer db.Close()

	iter := db.NewIterator(nil, nil)

	for iter.Next() {
		kvSet, err := ParseKV(iter.Key(), iter.Value(), "")
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		kvSet.Print()
	}

	iter.Release()
	err = iter.Error()
	if err != nil {
		fmt.Println(err.Error())
	}
	os.Exit(m.Run())
}
