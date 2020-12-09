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
	IsConfig() bool
}

type ConfigBlock struct {
	*common.Block
}

func (b ConfigBlock) GetTransactionEnvelops() ([]*common.Envelope, error) {
	fmt.Println("config block")
	return nil, nil
}
func (b ConfigBlock) GetTxRWSets(txEnvelopes []*common.Envelope) (txRWSets []*rwsetutil.TxRwSet, err error) {
	return nil, nil
}

func (b ConfigBlock) IsConfig() bool {
	return true
}

type StandardBlock struct {
	*common.Block
}

func (b StandardBlock) GetTransactionEnvelops() ([]*common.Envelope, error) {

	txs := make([]*common.Envelope, 0)
	for _, txEnvBytes := range b.GetData().GetData() {
		txEnvelope, err := putil.GetEnvelopeFromBlock(txEnvBytes)
		if err != nil {
			return nil, err
		}
		txs = append(txs, txEnvelope)
	}

	return txs, nil
}

func (b StandardBlock) GetTxRWSets(txEnvelopes []*common.Envelope) (txRWSets []*rwsetutil.TxRwSet, err error) {
	// var txRWSets []*rwsetutil.TxRwSet
	for _, txEnvelope := range txEnvelopes {

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

func (b StandardBlock) IsConfig() bool {
	return false
}
