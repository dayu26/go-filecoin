package block

import "github.com/filecoin-project/go-filecoin/internal/pkg/types"

// FullBlock carries a block header and the message and receipt collections
// referenced from the header.
type FullBlock struct {
	Header   *Block
	Messages []*types.SignedMessage
	Receipts []*types.MessageReceipt
}

// NewFullBlock constructs a new full block.
func NewFullBlock(header *Block, msgs []*types.SignedMessage, rcpts []*types.MessageReceipt) *FullBlock {
	return &FullBlock{
		Header:   header,
		Messages: msgs,
		Receipts: rcpts,
	}
}
