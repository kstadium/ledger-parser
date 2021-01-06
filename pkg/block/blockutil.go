package block

import (
	goproto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// IsConfigBlock validates whenever given block contains configuration
// update transaction
func IsConfigBlock(block *common.Block) bool {
	return protoutil.IsConfigBlock(block)
}

// GetTransactionEnvelopes returns []Envelop
// Block.BlockData.[]Data - []Envelope
func GetTransactionEnvelopes(block *common.Block) ([]*common.Envelope, error) {
	txEnvs := []*common.Envelope{}
	for _, txEnvBytes := range block.GetData().GetData() {
		txEnv, err := protoutil.GetEnvelopeFromBlock(txEnvBytes)
		if err != nil {
			return nil, err
		}

		txEnvs = append(txEnvs, txEnv)
	}

	return txEnvs, nil
}

// GetTransactionEnvelopePayload returns Payload
// Block.BlockData.[]Data - []Envelope.Payload - Payload
func GetTransactionEnvelopePayload(env *common.Envelope) (*common.Payload, error) {
	txPayload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}
	return txPayload, nil
}

// GetTxEnvPayloadChannelHeader returns ChannelHeader
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Header.ChannelHeader - ChannelHeader
func GetTxEnvPayloadChannelHeader(txPayload *common.Payload) (*common.ChannelHeader, error) {
	return protoutil.UnmarshalChannelHeader(txPayload.Header.ChannelHeader)
}

// GetTxEnvPayloadSignatureHeader returns SignatureHeader
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Header.SignatureHeader - SignatureHeader
func GetTxEnvPayloadSignatureHeader(txPayload *common.Payload) (*common.SignatureHeader, error) {
	return protoutil.UnmarshalSignatureHeader(txPayload.Header.SignatureHeader)
}

// GetTxEnvPayloadActions returns []TransactionAction
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions
func GetTxEnvPayloadActions(txPayload *common.Payload) ([]*peer.TransactionAction, error) {
	tx, err := protoutil.UnmarshalTransaction(txPayload.Data)
	if err != nil {
		return nil, err
	}

	return tx.Actions, nil
}

// GetActionHeader returns SignatureHeader
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions - []TransactionAction.Header - SignatureHeader
func GetActionHeader(txAction *peer.TransactionAction) (*common.SignatureHeader, error) {
	return protoutil.UnmarshalSignatureHeader(txAction.Header)
}

// GetActionPayload returns ChaincodeActionPayload
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions - []TransactionAction.Payload - ChaincodeActionPayload
func GetActionPayload(txAction *peer.TransactionAction) (*peer.ChaincodeActionPayload, error) {
	return protoutil.UnmarshalChaincodeActionPayload(txAction.Payload)
}

// GetActionEndorsements returns []Endorsement
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions - []TransactionAction.Payload - ChaincodeActionPayload.Action.Endorsements
func GetActionEndorsements(caPayload *peer.ChaincodeActionPayload) []*peer.Endorsement {
	return caPayload.Action.Endorsements
}

// GetActionProposalresponseExtension returns ChaincodeAction
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions - []TransactionAction.Payload - ChaincodeActionPayload.Action.ProposalResponsePayload - ProposalResponsePayload.Extension - ChaincodeAction
func GetActionProposalresponseCCAction(caPayload *peer.ChaincodeActionPayload) (*peer.ChaincodeAction, error) {
	prp, err := protoutil.UnmarshalProposalResponsePayload(caPayload.Action.ProposalResponsePayload)
	if err != nil {
		return nil, err
	}

	act, err := protoutil.UnmarshalChaincodeAction(prp.Extension)
	if err != nil {
		return nil, err
	}
	return act, nil
}

// GetActionProposalresponseResults returns TxReadWriteSet
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions - []TransactionAction.Payload - ChaincodeActionPayload.Action.ProposalResponsePayload - ProposalResponsePayload.Extension - ChaincodeAction.Results - TxReadWriteSet
func GetActionProposalresponseResults(ca *peer.ChaincodeAction) (*rwsetutil.TxRwSet, error) {
	// rwset.TxReadWriteSet -> rwsetutil.TxRwSet
	txRWSet := &rwsetutil.TxRwSet{}
	err := txRWSet.FromProtoBytes(ca.Results)
	if err != nil {
		return nil, err
	}

	return txRWSet, nil
}

// GetActionProposalresponseEvents returns ChaincodeEvent
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions - []TransactionAction.Payload - ChaincodeActionPayload.Action.ProposalResponsePayload - ProposalResponsePayload.Extension - ChaincodeAction.Events - ChaincodeEvent
func GetActionProposalresponseEvents(ca *peer.ChaincodeAction) (*peer.ChaincodeEvent, error) {

	ccEvent := &peer.ChaincodeEvent{}
	if err := goproto.Unmarshal(ca.Events, ccEvent); err != nil {
		return nil, errors.Wrapf(err, "invalid chaincode event")
	}
	return ccEvent, nil
}

// GetActionCCProposalPayload returns ChaincodeProposalPayload
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions - []TransactionAction.Payload - ChaincodeActionPayload.ChaincodeProposalPayload - ChaincodeProposalPayload
func GetActionCCProposalPayload(caPayload *peer.ChaincodeActionPayload) (*peer.ChaincodeProposalPayload, error) {
	cpPayload, err := protoutil.UnmarshalChaincodeProposalPayload(caPayload.ChaincodeProposalPayload)
	if err != nil {
		return nil, err
	}
	return cpPayload, nil
}

// GetActionCCProposalCCISpec returns ChaincodeInvocationSpec
// Block.BlockData.[]Data - []Envelope.Payload - Payload.Data - Transaction.[]Actions - []TransactionAction.Payload - ChaincodeActionPayload.ChaincodeProposalPayload - ChaincodeProposalPayload.Input - ChaincodeInvocationSpec
func GetActionCCProposalCCISpec(cpPayload *peer.ChaincodeProposalPayload) (*peer.ChaincodeInvocationSpec, error) {
	cis, err := protoutil.UnmarshalChaincodeInvocationSpec(cpPayload.Input)
	if err != nil {
		return nil, err
	}
	return cis, nil
}
