package block

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/the-medium/ledger-parser/internal/utils"

	"github.com/hyperledger/fabric-protos-go/common"

	putil "github.com/hyperledger/fabric/protoutil"
)

var ErrUnexpectedEndOfBlockfile = errors.New("unexpected end of blockfile")

var (
	file       *os.File
	fileName   string
	fileSize   int64
	fileOffset int64
	fileReader *bufio.Reader
)

// Parse a block
func handleBlock(block *common.Block) (b Block, err error) {
	// fmt.Printf("Block: Number=[%d], CurrentBlockHash=[%s], PreviousBlockHash=[%s]\n",
	// 	block.GetHeader().Number,
	// 	base64.StdEncoding.EncodeToString(block.GetHeader().DataHash),
	// 	base64.StdEncoding.EncodeToString(block.GetHeader().PreviousHash))

	if putil.IsConfigBlock(block) {
		b = &ConfigBlock{block}
	} else {
		b = &StandardBlock{block}
		// for _, txEnvBytes := range block.GetData().GetData() {
		// 	if txid, err := extractTxID(txEnvBytes); err != nil {
		// 		return "", fmt.Errorf("ERROR: Cannot extract txid, error=[%v]\n", err)
		// 	} else {
		// 		fmt.Printf("    txid=%s\n", txid)
		// 	}
		// }
	}
	return b, nil
	// write block to file
	// b, err := proto.Marshal(block)
	// if err != nil {
	// 	return "", fmt.Errorf("error: cannot marshal block, error=[%v]", err)
	// }

	// filename := fmt.Sprintf("block%d.block", block.GetHeader().Number)
	// if err := ioutil.WriteFile(filename, b, 0644); err != nil {
	// 	fmt.Printf("ERROR: Cannot write block to file:[%s], error=[%v]\n", filename, err)
	// }

	// Then you could use utility to read block content, like:
	// $ configtxlator proto_decode --input block0.block --type common.Block

	// msgType := proto.MessageType("common.Block")
	// if msgType == nil {
	// 	return "", fmt.Errorf("message of type %s unknown", msgType)
	// }
	// msg := reflect.New(msgType.Elem()).Interface().(proto.Message)

	// err = proto.Unmarshal(b, msg)
	// if err != nil {
	// 	return "", fmt.Errorf("%x", err)
	// }

	// buf := new(bytes.Buffer)

	// err = protolator.DeepMarshalJSON(buf, msg)
	// if err != nil {
	// 	return "", fmt.Errorf("%s ", err)
	// }

	// // filename := fmt.Sprintf("block%d.json", block.GetHeader().Number)
	// // if err := ioutil.WriteFile(filename, buf.Bytes(), 0644); err != nil {
	// // 	fmt.Printf("ERROR: Cannot write block to file:[%s], error=[%v]\n", filename, err)
	// // }

	// return buf.String(), nil
}

func nextBlockBytes() ([]byte, error) {
	var lenBytes []byte
	var err error

	// At the end of file
	if fileOffset == fileSize {
		return nil, nil
	}

	remainingBytes := fileSize - fileOffset
	peekBytes := 8
	if remainingBytes < int64(peekBytes) {
		peekBytes = int(remainingBytes)
	}
	if lenBytes, err = fileReader.Peek(peekBytes); err != nil {
		return nil, err
	}

	length, n := proto.DecodeVarint(lenBytes)
	if n == 0 {
		return nil, fmt.Errorf("Error in decoding varint bytes [%#v]", lenBytes)
	}

	bytesExpected := int64(n) + int64(length)
	if bytesExpected > remainingBytes {
		return nil, ErrUnexpectedEndOfBlockfile
	}

	// skip the bytes representing the block size
	if _, err = fileReader.Discard(n); err != nil {
		return nil, err
	}

	blockBytes := make([]byte, length)
	if _, err = io.ReadAtLeast(fileReader, blockBytes, int(length)); err != nil {
		return nil, err
	}

	fileOffset += int64(n) + int64(length)
	return blockBytes, nil
}

func DeserializeBlock(serializedBlockBytes []byte) (*common.Block, error) {
	block := &common.Block{}
	var err error
	b := utils.NewBuffer(serializedBlockBytes)
	if block.Header, err = extractHeader(b); err != nil {
		return nil, err
	}
	if block.Data, err = extractData(b); err != nil {
		return nil, err
	}
	if block.Metadata, err = extractMetadata(b); err != nil {
		return nil, err
	}
	return block, nil
}

func ExtractHeader(buf *utils.Buffer) (*common.BlockHeader, error) {
	header := &common.BlockHeader{}
	var err error
	if header.Number, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}
	if header.DataHash, err = buf.DecodeRawBytes(false); err != nil {
		return nil, err
	}
	if header.PreviousHash, err = buf.DecodeRawBytes(false); err != nil {
		return nil, err
	}
	if len(header.PreviousHash) == 0 {
		header.PreviousHash = nil
	}
	return header, nil
}

func ExtractData(buf *utils.Buffer) (*common.BlockData, error) {
	data := &common.BlockData{}
	var numItems uint64
	var err error

	if numItems, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}
	for i := uint64(0); i < numItems; i++ {
		var txEnvBytes []byte
		if txEnvBytes, err = buf.DecodeRawBytes(false); err != nil {
			return nil, err
		}
		data.Data = append(data.Data, txEnvBytes)
	}
	return data, nil
}

func ExtractMetadata(buf *utils.Buffer) (*common.BlockMetadata, error) {
	metadata := &common.BlockMetadata{}
	var numItems uint64
	var metadataEntry []byte
	var err error
	if numItems, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}
	for i := uint64(0); i < numItems; i++ {
		if metadataEntry, err = buf.DecodeRawBytes(false); err != nil {
			return nil, err
		}
		metadata.Metadata = append(metadata.Metadata, metadataEntry)
	}
	return metadata, nil
}

func ExtractTxID(txEnvelopBytes []byte) (string, error) {
	txEnvelope, err := putil.GetEnvelopeFromBlock(txEnvelopBytes)
	if err != nil {
		return "", err
	}
	txPayload, err := putil.UnmarshalPayload(txEnvelope.Payload)
	if err != nil {
		return "", nil
	}
	chdr, err := putil.UnmarshalChannelHeader(txPayload.Header.ChannelHeader)
	if err != nil {
		return "", err
	}
	return chdr.TxId, nil
}

func GetBlocksFromBlockFile(fileName string) ([]Block, error) {
	var blocks []Block
	var err error
	file, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("error: cannot open file: [%s], error=[%v]", fileName, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {

		return nil, fmt.Errorf("error: cannot stat file: [%s], error=[%v]", fileName, err)
	}

	fileOffset = 0
	fileSize = fileInfo.Size()
	fileReader = bufio.NewReader(file)

	// Loop each block
	for {
		if blockBytes, err := nextBlockBytes(); err != nil {

			return nil, fmt.Errorf("error: cannot read block file: [%s], error=[%v]", fileName, err)
		} else if blockBytes == nil {
			// End of file
			break
		} else {
			block, err := DeserializeBlock(blockBytes)
			if err != nil {
				return nil, fmt.Errorf("error: cannot deserialize block from file: [%s], error=[%v]", fileName, err)
			}
			b, err := handleBlock(block)
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, b)

		}
	}

	return blocks, err
}

func ReadBlock(file *os.File, fileOffset int64) ([]byte, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()

	// At the end of file
	if fileOffset > fileSize {
		return nil, nil
	}

	remainingBytes := fileSize - fileOffset
	var peekBytes int64 = 8
	if remainingBytes < int64(peekBytes) {
		peekBytes = int64(remainingBytes)
	}
	lenBytes := make([]byte, peekBytes)

	file.ReadAt(lenBytes, fileOffset)

	length, n := proto.DecodeVarint(lenBytes)
	if n == 0 {
		return nil, fmt.Errorf("Error in decoding varint bytes [%#v]", lenBytes)
	}

	bytesExpected := int64(n) + int64(length)
	if bytesExpected > remainingBytes {
		return nil, fmt.Errorf("unexpected end of blockfile")
	}

	blockBytes := make([]byte, length)
	file.ReadAt(blockBytes, fileOffset+int64(n))

	return blockBytes, nil
}

func ReadTransaction(file *os.File, fileOffset int64, byteLen int64) ([]byte, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()

	// At the end of file
	if fileOffset+byteLen > fileSize {
		return nil, nil
	}

	txBytes := make([]byte, byteLen)
	file.ReadAt(txBytes, fileOffset)

	return txBytes, nil
}
