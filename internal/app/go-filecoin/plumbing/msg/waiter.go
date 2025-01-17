package msg

import (
	"context"
	"fmt"

	"github.com/cskr/pubsub"
	"github.com/filecoin-project/go-filecoin/internal/pkg/block"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-hamt-ipld"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/pkg/errors"

	"github.com/filecoin-project/go-filecoin/internal/pkg/chain"
	"github.com/filecoin-project/go-filecoin/internal/pkg/consensus"
	"github.com/filecoin-project/go-filecoin/internal/pkg/state"
	"github.com/filecoin-project/go-filecoin/internal/pkg/types"
	"github.com/filecoin-project/go-filecoin/internal/pkg/vm"
)

var log = logging.Logger("messageimpl")

// Abstracts over a store of blockchain state.
type waiterChainReader interface {
	GetHead() block.TipSetKey
	GetTipSet(block.TipSetKey) (block.TipSet, error)
	GetTipSetState(context.Context, block.TipSetKey) (state.Tree, error)
	HeadEvents() *pubsub.PubSub
}

// Waiter waits for a message to appear on chain.
type Waiter struct {
	chainReader     waiterChainReader
	messageProvider chain.MessageProvider
	cst             *hamt.CborIpldStore
	bs              bstore.Blockstore
}

// ChainMessage is an on-chain message with its block and receipt.
type ChainMessage struct {
	Message *types.SignedMessage
	Block   *block.Block
	Receipt *types.MessageReceipt
}

// NewWaiter returns a new Waiter.
func NewWaiter(chainStore waiterChainReader, messages chain.MessageProvider, bs bstore.Blockstore, cst *hamt.CborIpldStore) *Waiter {
	return &Waiter{
		chainReader:     chainStore,
		cst:             cst,
		bs:              bs,
		messageProvider: messages,
	}
}

// Find searches the blockchain history for a message (but doesn't wait).
func (w *Waiter) Find(ctx context.Context, msgCid cid.Cid) (*ChainMessage, bool, error) {
	headTipSet, err := w.chainReader.GetTipSet(w.chainReader.GetHead())
	if err != nil {
		return nil, false, err
	}
	return w.findMessage(ctx, headTipSet, msgCid)
}

// Wait invokes the callback when a message with the given cid appears on chain.
// See api description.
//
// Note: this method does too much -- the callback should just receive the tipset
// containing the message and the caller should pull the receipt out of the block
// if in fact that's what it wants to do, using something like receiptFromTipset.
// Something like receiptFromTipset is necessary because not every message in
// a block will have a receipt in the tipset: it might be a duplicate message.
//
// TODO: This implementation will become prohibitively expensive since it
// traverses the entire chain. We should use an index instead.
// https://github.com/filecoin-project/go-filecoin/issues/1518
func (w *Waiter) Wait(ctx context.Context, msgCid cid.Cid, cb func(*block.Block, *types.SignedMessage, *types.MessageReceipt) error) error {
	log.Infof("Calling Waiter.Wait CID: %s", msgCid.String())

	ch := w.chainReader.HeadEvents().Sub(chain.NewHeadTopic)
	defer w.chainReader.HeadEvents().Unsub(ch, chain.NewHeadTopic)

	chainMsg, found, err := w.Find(ctx, msgCid)
	if err != nil {
		return err
	}
	if found {
		return cb(chainMsg.Block, chainMsg.Message, chainMsg.Receipt)
	}

	chainMsg, found, err = w.waitForMessage(ctx, ch, msgCid)
	if found {
		return cb(chainMsg.Block, chainMsg.Message, chainMsg.Receipt)
	}
	return err
}

// findMessage looks for a message CID in the chain and returns the message,
// block and receipt, when it is found. Returns the found message/block or nil
// if now block with the given CID exists in the chain.
func (w *Waiter) findMessage(ctx context.Context, ts block.TipSet, msgCid cid.Cid) (*ChainMessage, bool, error) {
	var err error
	for iterator := chain.IterAncestors(ctx, w.chainReader, ts); !iterator.Complete(); err = iterator.Next() {
		if err != nil {
			log.Errorf("Waiter.Wait: %s", err)
			return nil, false, err
		}
		for i := 0; i < iterator.Value().Len(); i++ {
			blk := iterator.Value().At(i)
			secpMsgs, _, err := w.messageProvider.LoadMessages(ctx, blk.Messages)
			if err != nil {
				return nil, false, err
			}
			for _, msg := range secpMsgs {
				c, err := msg.Cid()
				if err != nil {
					return nil, false, err
				}
				if c.Equals(msgCid) {
					recpt, err := w.receiptFromTipSet(ctx, msgCid, iterator.Value())
					if err != nil {
						return nil, false, errors.Wrap(err, "error retrieving receipt from tipset")
					}
					return &ChainMessage{msg, blk, recpt}, true, nil
				}
			}
		}
	}
	return nil, false, nil
}

// waitForMessage looks for a message CID in a channel of tipsets and returns
// the message, block and receipt, when it is found. Reads until the channel is
// closed or the context done. Returns the found message/block (or nil if the
// channel closed without finding it), whether it was found, or an error.
func (w *Waiter) waitForMessage(ctx context.Context, ch <-chan interface{}, msgCid cid.Cid) (*ChainMessage, bool, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case raw, more := <-ch:
			if !more {
				return nil, false, nil
			}
			switch raw := raw.(type) {
			case error:
				e := raw.(error)
				log.Errorf("Waiter.Wait: %s", e)
				return nil, false, e
			case block.TipSet:
				for i := 0; i < raw.Len(); i++ {
					blk := raw.At(i)
					secpMsgs, _, err := w.messageProvider.LoadMessages(ctx, blk.Messages)
					if err != nil {
						return nil, false, err
					}
					for _, msg := range secpMsgs {
						c, err := msg.Cid()
						if err != nil {
							return nil, false, err
						}
						if c.Equals(msgCid) {
							recpt, err := w.receiptFromTipSet(ctx, msgCid, raw)
							if err != nil {
								return nil, false, errors.Wrap(err, "error retrieving receipt from tipset")
							}
							return &ChainMessage{msg, blk, recpt}, true, nil
						}
					}
				}
			default:
				return nil, false, fmt.Errorf("unexpected type in channel: %T", raw)
			}
		}
	}
}

// receiptFromTipSet finds the receipt for the message with msgCid in the
// input tipset.  This can differ from the message's receipt as stored in its
// parent block in the case that the message is in conflict with another
// message of the tipset.
func (w *Waiter) receiptFromTipSet(ctx context.Context, msgCid cid.Cid, ts block.TipSet) (*types.MessageReceipt, error) {
	// Receipts always match block if tipset has only 1 member.
	var rcpt *types.MessageReceipt
	if ts.Len() == 1 {
		b := ts.At(0)
		// TODO #3194: this should return an error if a receipt doesn't exist.
		// Right now doing so breaks tests because our test helpers
		// don't correctly apply messages when making test chains.
		//
		j, err := w.msgIndexOfTipSet(ctx, msgCid, ts, make(map[cid.Cid]struct{}))
		if err != nil {
			return nil, err
		}

		receipts, err := w.messageProvider.LoadReceipts(ctx, b.MessageReceipts)
		if err != nil {
			return nil, err
		}
		if j < len(receipts) {
			rcpt = receipts[j]
		}
		return rcpt, nil
	}

	// Apply all the tipset's messages to determine the correct receipts.
	ids, err := ts.Parents()
	if err != nil {
		return nil, err
	}
	st, err := w.chainReader.GetTipSetState(ctx, ids)
	if err != nil {
		return nil, err
	}

	tsHeight, err := ts.Height()
	if err != nil {
		return nil, err
	}
	ancestorHeight := types.NewBlockHeight(tsHeight).Sub(types.NewBlockHeight(consensus.AncestorRoundsNeeded))
	parentTs, err := w.chainReader.GetTipSet(ids)
	if err != nil {
		return nil, err
	}
	ancestors, err := chain.GetRecentAncestors(ctx, parentTs, w.chainReader, ancestorHeight)
	if err != nil {
		return nil, err
	}

	var tsMessages [][]*types.SignedMessage
	for i := 0; i < ts.Len(); i++ {
		blk := ts.At(i)
		secpMsgs, _, err := w.messageProvider.LoadMessages(ctx, blk.Messages)
		if err != nil {
			return nil, err
		}
		tsMessages = append(tsMessages, secpMsgs)
	}

	res, err := consensus.NewDefaultProcessor().ProcessTipSet(ctx, st, vm.NewStorageMap(w.bs), ts, tsMessages, ancestors)
	if err != nil {
		return nil, err
	}

	// If this is a failing conflict message there is no application receipt.
	_, failed := res.Failures[msgCid]
	if failed {
		return nil, nil
	}

	j, err := w.msgIndexOfTipSet(ctx, msgCid, ts, res.Failures)
	if err != nil {
		return nil, err
	}
	// TODO #3194: out of bounds receipt index should return an error.
	if j < len(res.Results) {
		rcpt = res.Results[j].Receipt
	}
	return rcpt, nil
}

// msgIndexOfTipSet returns the order in which msgCid appears in the canonical
// message ordering of the given tipset, or an error if it is not in the
// tipset.
// TODO: find a better home for this method
func (w *Waiter) msgIndexOfTipSet(ctx context.Context, msgCid cid.Cid, ts block.TipSet, fails map[cid.Cid]struct{}) (int, error) {
	duplicates := make(map[cid.Cid]struct{})
	var msgCnt int
	for i := 0; i < ts.Len(); i++ {
		secpMsgs, _, err := w.messageProvider.LoadMessages(ctx, ts.At(i).Messages)
		if err != nil {
			return -1, err
		}
		for _, msg := range secpMsgs {
			c, err := msg.Cid()
			if err != nil {
				return -1, err
			}
			_, failed := fails[c]
			if failed {
				continue
			}
			_, isDup := duplicates[c]
			if isDup {
				continue
			}
			duplicates[c] = struct{}{}
			if c.Equals(msgCid) {
				return msgCnt, nil
			}
			msgCnt++
		}
	}

	return -1, fmt.Errorf("message cid %s not in tipset", msgCid.String())
}
