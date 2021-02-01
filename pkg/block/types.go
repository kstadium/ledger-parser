package block

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
	putil "github.com/hyperledger/fabric/protoutil"
)

type Block interface {
	GetTransactionEnvelops() ([]*common.Envelope, error)
	GetTxRWSets(txEnvelopes []*common.Envelope) (txRWSets []*rwsetutil.TxRwSet, err error)
	GetTxFilters() []byte
	IsConfig() bool
}

type ConfigBlock struct {
	Block *common.Block
}

func (b ConfigBlock) GetTransactionEnvelops() ([]*common.Envelope, error) {
	fmt.Println("config block")
	return nil, nil
}
func (b ConfigBlock) GetTxRWSets(txEnvelopes []*common.Envelope) (txRWSets []*rwsetutil.TxRwSet, err error) {
	return nil, nil
}

func (b ConfigBlock) GetTxFilters() []byte {
	return b.Block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER]
}

func (b ConfigBlock) IsConfig() bool {
	return true
}

type StandardBlock struct {
	Block *common.Block
}

func (b StandardBlock) GetTransactionEnvelops() ([]*common.Envelope, error) {

	txs := make([]*common.Envelope, 0)
	for _, txEnvBytes := range b.Block.GetData().GetData() {
		txEnvelope, err := putil.GetEnvelopeFromBlock(txEnvBytes)
		if err != nil {
			return nil, err
		}
		txs = append(txs, txEnvelope)
	}

	return txs, nil
}

func (b StandardBlock) GetTxRWSets(txEnvelopes []*common.Envelope) (txRWSets []*rwsetutil.TxRwSet, err error) {
	txfilters := b.GetTxFilters()
	if len(txEnvelopes) != len(txfilters) {
		return nil, fmt.Errorf("The number of tx does not match the number of filters")
	}

	for idx, txEnvelope := range txEnvelopes {
		if peer.TxValidationCode(txfilters[idx]) != peer.TxValidationCode_VALID {
			continue
		}

		txPayload, err := putil.UnmarshalPayload(txEnvelope.Payload)
		if err != nil {
			return nil, err
		}

		tx, err := putil.UnmarshalTransaction(txPayload.Data)
		if err != nil {
			return nil, err
		}
		var cap *peer.ChaincodeActionPayload
		var prp *peer.ProposalResponsePayload
		var act *peer.ChaincodeAction
		for _, action := range tx.Actions {
			cap, err = protoutil.UnmarshalChaincodeActionPayload(action.Payload)
			if err != nil {
				return nil, err
			}

			prp, err = protoutil.UnmarshalProposalResponsePayload(cap.Action.ProposalResponsePayload)
			if err != nil {
				return nil, err
			}

			act, err = protoutil.UnmarshalChaincodeAction(prp.Extension)
			if err != nil {
				return nil, err
			}

			txRWSet := &rwsetutil.TxRwSet{}
			err = txRWSet.FromProtoBytes(act.Results)
			if err != nil {
				return nil, err
			}
			txRWSets = append(txRWSets, txRWSet)
		}
	}
	return txRWSets, nil
}

func (b StandardBlock) GetTxFilters() []byte {
	return b.Block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER]
}

func (b StandardBlock) IsConfig() bool {
	return false
}
