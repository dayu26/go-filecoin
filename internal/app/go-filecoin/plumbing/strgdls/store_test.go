package strgdls_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-filecoin/internal/app/go-filecoin/plumbing/strgdls"
	"github.com/filecoin-project/go-filecoin/internal/pkg/address"
	"github.com/filecoin-project/go-filecoin/internal/pkg/encoding"
	"github.com/filecoin-project/go-filecoin/internal/pkg/protocol/storage/storagedeal"
	"github.com/filecoin-project/go-filecoin/internal/pkg/repo"
	tf "github.com/filecoin-project/go-filecoin/internal/pkg/testhelpers/testflags"
	"github.com/filecoin-project/go-filecoin/internal/pkg/types"
	"github.com/filecoin-project/go-filecoin/internal/pkg/util/convert"
)

func TestDealStoreRoundTrip(t *testing.T) {
	tf.UnitTest(t)

	addressMaker := address.NewForTestGetter()

	store := strgdls.New(repo.NewInMemoryRepo().DealsDs)
	minerAddr := addressMaker()
	pieceRefCid, err := convert.ToCid("pieceRef")
	require.NoError(t, err)
	size := types.NewBytesAmount(12)
	totalPrice := types.NewAttoFILFromFIL(13)
	duration := uint64(23)
	channelID := *types.NewChannelID(19)
	channelMessageCid, err := convert.ToCid(&types.UnsignedMessage{})
	require.NoError(t, err)
	clientAddr := addressMaker()
	validAt := types.NewBlockHeight(231)
	responseMessage := "Success!"

	payment := storagedeal.PaymentInfo{PayChActor: addressMaker(),
		Payer:         addressMaker(),
		Channel:       &channelID,
		ChannelMsgCid: &channelMessageCid,
		Vouchers: []*types.PaymentVoucher{{
			Channel: channelID,
			Payer:   clientAddr,
			Target:  minerAddr,
			Amount:  totalPrice,
			ValidAt: *validAt,
		}}}

	proposal := &storagedeal.SignedProposal{
		Proposal: storagedeal.Proposal{
			PieceRef:     pieceRefCid,
			Size:         size,
			TotalPrice:   totalPrice,
			Duration:     duration,
			MinerAddress: minerAddr,
			Payment:      payment,
		},
	}

	proposalCid, err := convert.ToCid(proposal)
	require.NoError(t, err)

	messageCid, err := convert.ToCid("messageCid")
	require.NoError(t, err)

	storageDeal := &storagedeal.Deal{
		Miner:    minerAddr,
		Proposal: proposal,
		Response: &storagedeal.SignedResponse{
			Response: storagedeal.Response{
				State:       storagedeal.Accepted,
				Message:     responseMessage,
				ProposalCid: proposalCid,
				ProofInfo:   &storagedeal.ProofInfo{CommitmentMessage: messageCid},
			},
			Signature: []byte("signature"),
		},
	}

	require.NoError(t, store.Put(storageDeal))
	dealIterator, err := store.Iterator()
	require.NoError(t, err)

	dealResult := <-(*dealIterator).Next()
	var retrievedDeal storagedeal.Deal
	err = encoding.Decode(dealResult.Value, &retrievedDeal)
	require.NoError(t, err)

	assert.Equal(t, minerAddr, retrievedDeal.Miner)

	assert.Equal(t, pieceRefCid, retrievedDeal.Proposal.PieceRef)
	assert.Equal(t, size, retrievedDeal.Proposal.Size)
	assert.Equal(t, totalPrice, retrievedDeal.Proposal.TotalPrice)
	assert.Equal(t, duration, retrievedDeal.Proposal.Duration)
	assert.Equal(t, minerAddr, retrievedDeal.Proposal.MinerAddress)

	assert.Equal(t, channelID, retrievedDeal.Proposal.Payment.Vouchers[0].Channel)
	assert.Equal(t, clientAddr, retrievedDeal.Proposal.Payment.Vouchers[0].Payer)
	assert.Equal(t, minerAddr, retrievedDeal.Proposal.Payment.Vouchers[0].Target)
	assert.Equal(t, totalPrice, retrievedDeal.Proposal.Payment.Vouchers[0].Amount)
	assert.Equal(t, *validAt, retrievedDeal.Proposal.Payment.Vouchers[0].ValidAt)
}
