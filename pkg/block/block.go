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
	if putil.IsConfigBlock(block) {
		b = &ConfigBlock{Block: block}
	} else {
		b = &StandardBlock{Block: block}
	}
	return b, nil
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
	if block.Header, err = ExtractHeader(b); err != nil {
		return nil, err
	}
	if block.Data, err = ExtractData(b); err != nil {
		return nil, err
	}
	if block.Metadata, err = ExtractMetadata(b); err != nil {
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
