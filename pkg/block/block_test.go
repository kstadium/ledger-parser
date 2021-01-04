package block

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	goproto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-config/protolator"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/stretchr/testify/assert"
)

func Test_Block(t *testing.T) {
	fileName = "/var/hyperledger/production/ledgersData/chains/chains/mychannel/blockfile_000000"
	blocks, err := GetBlocksFromBlockFile(fileName)
	assert.NoError(t, err)
	for _, block := range blocks {
		var cBlock *common.Block
		if block.IsConfig() {
			cBlock = block.(*ConfigBlock).Block
		} else {
			cBlock = block.(*StandardBlock).Block
		}
		b, err := goproto.Marshal(cBlock)
		assert.NoError(t, err)

		msgType := goproto.MessageType("common.Block")
		assert.NotNil(t, msgType)

		msg := reflect.New(msgType.Elem()).Interface().(proto.Message)
		err = proto.Unmarshal(b, msg)
		assert.NoError(t, err)

		buf := new(bytes.Buffer)
		err = protolator.DeepMarshalJSON(buf, msg)
		assert.NoError(t, err)

		filename := fmt.Sprintf("block%d.json", cBlock.GetHeader().Number)
		if err := ioutil.WriteFile(filename, buf.Bytes(), 0644); err != nil {
			fmt.Printf("ERROR: Cannot write block to file:[%s], error=[%v]\n", filename, err)
		}
	}
}
