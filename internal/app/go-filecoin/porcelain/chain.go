package porcelain

import (
	"context"

	"github.com/filecoin-project/go-filecoin/internal/pkg/block"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-filecoin/internal/pkg/types"
)

type chainHeadPlumbing interface {
	ChainHeadKey() block.TipSetKey
	ChainTipSet(key block.TipSetKey) (block.TipSet, error)
}

// ChainHead gets the current head tipset from plumbing.
func ChainHead(plumbing chainHeadPlumbing) (block.TipSet, error) {
	return plumbing.ChainTipSet(plumbing.ChainHeadKey())
}

type fullBlockPlumbing interface {
	ChainGetBlock(context.Context, cid.Cid) (*block.Block, error)
	ChainGetMessages(context.Context, types.TxMeta) ([]*types.SignedMessage, error)
	ChainGetReceipts(context.Context, cid.Cid) ([]*types.MessageReceipt, error)
}

// GetFullBlock returns a full block: header, messages, receipts.
func GetFullBlock(ctx context.Context, plumbing fullBlockPlumbing, id cid.Cid) (*block.FullBlock, error) {
	var out block.FullBlock
	var err error

	out.Header, err = plumbing.ChainGetBlock(ctx, id)
	if err != nil {
		return nil, err
	}

	out.Messages, err = plumbing.ChainGetMessages(ctx, out.Header.Messages)
	if err != nil {
		return nil, err
	}

	out.Receipts, err = plumbing.ChainGetReceipts(ctx, out.Header.MessageReceipts)
	if err != nil {
		return nil, err
	}

	return &out, nil
}
