package consensus

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-filecoin/internal/pkg/block"
	"github.com/filecoin-project/go-filecoin/internal/pkg/clock"
	"github.com/filecoin-project/go-filecoin/internal/pkg/types"
	"github.com/filecoin-project/go-filecoin/internal/pkg/version"
)

// BlockValidator defines an interface used to validate a blocks syntax and
// semantics.
type BlockValidator interface {
	BlockSemanticValidator
	BlockSyntaxValidator
}

// SyntaxValidator defines and interface used to validate block's syntax and the
// syntax of constituent messages
type SyntaxValidator interface {
	BlockSyntaxValidator
	MessageSyntaxValidator
}

// BlockSemanticValidator defines an interface used to validate a blocks
// semantics.
type BlockSemanticValidator interface {
	ValidateSemantic(ctx context.Context, child *block.Block, parents *block.TipSet, parentWeight uint64) error
}

// BlockSyntaxValidator defines an interface used to validate a blocks
// syntax.
type BlockSyntaxValidator interface {
	ValidateSyntax(ctx context.Context, blk *block.Block) error
}

// MessageSyntaxValidator defines an interface used to validate collections
// of messages and receipts syntax
type MessageSyntaxValidator interface {
	ValidateMessagesSyntax(ctx context.Context, messages []*types.SignedMessage) error
	ValidateUnsignedMessagesSyntax(ctx context.Context, messages []*types.UnsignedMessage) error
	ValidateReceiptsSyntax(ctx context.Context, receipts []*types.MessageReceipt) error
}

// DefaultBlockValidator implements the BlockValidator interface.
type DefaultBlockValidator struct {
	clock.Clock
	blockTime time.Duration
	pvt       *version.ProtocolVersionTable
}

// NewDefaultBlockValidator returns a new DefaultBlockValidator. It uses `blkTime`
// to validate blocks and uses the DefaultBlockValidationClock.
func NewDefaultBlockValidator(blkTime time.Duration, c clock.Clock, pvt *version.ProtocolVersionTable) *DefaultBlockValidator {
	return &DefaultBlockValidator{
		Clock:     c,
		blockTime: blkTime,
		pvt:       pvt,
	}
}

// ValidateSemantic validates a block is correctly derived from its parent.
func (dv *DefaultBlockValidator) ValidateSemantic(ctx context.Context, child *block.Block, parents *block.TipSet, parentWeight uint64) error {
	pmin, err := parents.MinTimestamp()
	if err != nil {
		return err
	}

	ph, err := parents.Height()
	if err != nil {
		return err
	}

	parentVersion, err := dv.pvt.VersionAt(types.NewBlockHeight(ph))
	if err != nil {
		return err
	}
	// Protocol version 1 upgrade introduces validation of the weight field
	// on the header.  During protocol version 0 validators do not validate
	// that the parent weight written to the header actually corresponds to
	// the weight measured by the validators.  Introducing this check
	// prevents a validator from writing arbitrary parent weight values
	// into a header and trivially generating the heaviest chain.
	if parentVersion >= version.Protocol1 {
		// Protocol Version 1 upgrade
		if uint64(child.ParentWeight) != parentWeight {
			return fmt.Errorf("block %s has invalid parent weight %d", child.Cid().String(), parentWeight)
		}
	}

	if uint64(child.Height) <= ph {
		return fmt.Errorf("block %s has invalid height %d", child.Cid().String(), child.Height)
	}

	// check that child is appropriately delayed from its parents including
	// null blocks.
	// TODO replace check on height when #2222 lands
	limit := uint64(pmin) + uint64(dv.BlockTime().Seconds())*(uint64(child.Height)-ph)
	if uint64(child.Timestamp) < limit {
		return fmt.Errorf("block %s with timestamp %d generated too far past parent, expected timestamp < %d", child.Cid().String(), child.Timestamp, limit)
	}
	return nil
}

// ValidateSyntax validates a single block is correctly formed.
// TODO this is an incomplete implementation #3277
func (dv *DefaultBlockValidator) ValidateSyntax(ctx context.Context, blk *block.Block) error {
	// TODO special handling for genesis block #3121
	if blk.Height == 0 {
		return nil
	}
	now := uint64(dv.Now().Unix())
	if uint64(blk.Timestamp) > now {
		return fmt.Errorf("block %s with timestamp %d generate in future at time %d", blk.Cid().String(), blk.Timestamp, now)
	}
	if !blk.StateRoot.Defined() {
		return fmt.Errorf("block %s has nil StateRoot", blk.Cid().String())
	}
	if blk.Miner.Empty() {
		return fmt.Errorf("block %s has nil miner address", blk.Cid().String())
	}
	if len(blk.Ticket.VRFProof) == 0 {
		return fmt.Errorf("block %s has nil ticket", blk.Cid().String())
	}

	return nil
}

// BlockTime returns the block time the DefaultBlockValidator uses to validate
/// blocks against.
func (dv *DefaultBlockValidator) BlockTime() time.Duration {
	return dv.blockTime
}

// ValidateMessagesSyntax validates a set of messages are correctly formed.
// TODO: Create a real implementation
// See: https://github.com/filecoin-project/go-filecoin/issues/3312
func (dv *DefaultBlockValidator) ValidateMessagesSyntax(ctx context.Context, messages []*types.SignedMessage) error {
	return nil
}

// ValidateUnsignedMessagesSyntax validates a set of messages are correctly formed.
// TODO: Create a real implementation
// See: https://github.com/filecoin-project/go-filecoin/issues/3312
func (dv *DefaultBlockValidator) ValidateUnsignedMessagesSyntax(ctx context.Context, messages []*types.UnsignedMessage) error {
	return nil
}

// ValidateReceiptsSyntax validates a set of receipts are correctly formed.
// TODO: Create a real implementation
// See: https://github.com/filecoin-project/go-filecoin/issues/3312
func (dv *DefaultBlockValidator) ValidateReceiptsSyntax(ctx context.Context, receipts []*types.MessageReceipt) error {
	return nil
}
