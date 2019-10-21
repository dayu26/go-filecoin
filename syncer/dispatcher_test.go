package syncer_test

import (
	"testing"
	"strconv"
	"sync"

	"github.com/filecoin-project/go-filecoin/block"
	"github.com/filecoin-project/go-filecoin/types"	
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-filecoin/syncer"
	tf "github.com/filecoin-project/go-filecoin/testhelpers/testflags"
)

func TestQueueHappy(t *testing.T) {
	tf.UnitTest(t)
	testQ := syncer.NewTargetQueue()

	// Add syncRequests out of order
	sR0 := &syncer.SyncRequest{ChainInfo: chainInfoFromHeight(t, 0)}
	sR1 := &syncer.SyncRequest{ChainInfo: chainInfoFromHeight(t, 1)}
	sR2 := &syncer.SyncRequest{ChainInfo: chainInfoFromHeight(t, 2)}
	sR47 := &syncer.SyncRequest{ChainInfo: chainInfoFromHeight(t, 47)}

	requirePush(t, sR2, testQ)
	requirePush(t, sR47, testQ)
	requirePush(t, sR0, testQ)
	requirePush(t, sR1, testQ)

	assert.Equal(t, 4, testQ.Len())

	// Pop in order
	out0 := requirePop(t, testQ)
	out1 := requirePop(t, testQ)
	out2 := requirePop(t, testQ)
	out3 := requirePop(t, testQ)

	assert.Equal(t, uint64(47), out0.ChainInfo.Height)
	assert.Equal(t, uint64(2), out1.ChainInfo.Height)
	assert.Equal(t, uint64(1), out2.ChainInfo.Height)
	assert.Equal(t, uint64(0), out3.ChainInfo.Height)

	assert.Equal(t, 0, testQ.Len())
}

func TestQueueDuplicates(t *testing.T) {
	tf.UnitTest(t)
	testQ := syncer.NewTargetQueue()

	// Add syncRequests with same height
	sR0 := &syncer.SyncRequest{ChainInfo: chainInfoFromHeight(t, 0)}
	sR0dup := &syncer.SyncRequest{ChainInfo: chainInfoFromHeight(t, 0)}

	err := testQ.Push(sR0)
	assert.NoError(t, err)

	err = testQ.Push(sR0dup)
	assert.NoError(t, err)

	// Only one of these makes it onto the queue
	assert.Equal(t, 1, testQ.Len())

	// Pop 
	first := requirePop(t, testQ)
	assert.Equal(t, uint64(0), first.ChainInfo.Height)

	// Now if we push the duplicate it goes back on
	err = testQ.Push(sR0dup)
	assert.NoError(t, err)
	assert.Equal(t, 1, testQ.Len())

	second := requirePop(t, testQ)
	assert.Equal(t, uint64(0), second.ChainInfo.Height)	
}

func TestQueueEmptyPopBlocks(t *testing.T) {
	tf.UnitTest(t)
	testQ := syncer.NewTargetQueue()
	sR0 := &syncer.SyncRequest{ChainInfo: chainInfoFromHeight(t, 0)}
	sR47 := &syncer.SyncRequest{ChainInfo: chainInfoFromHeight(t, 47)}

	// Push 2
	requirePush(t, sR47, testQ)
	requirePush(t, sR0, testQ)

	// Pop 3
	assert.Equal(t, 2, testQ.Len())
	_ = requirePop(t, testQ)
	assert.Equal(t, 1, testQ.Len())
	_ = requirePop(t, testQ)
	assert.Equal(t, 0, testQ.Len())

	var start,done  sync.WaitGroup
	start.Add(1)
	done.Add(1)
	go func() {
		start.Wait()
		async := requirePop(t, testQ)
		assert.Equal(t, uint64(47), async.ChainInfo.Height)
		done.Done()
	}()
	start.Done() // trigger Pop before Push
	requirePush(t, sR47, testQ)
	done.Wait() // wait for goroutine checks to pass
}

// requirePop is a helper requiring that pop does not error
func requirePop(t *testing.T, q *syncer.TargetQueue) *syncer.SyncRequest {
	req, err := q.Pop()
	require.NoError(t, err)
	return req
}

// requirePush is a helper requiring that push does not error
func requirePush(t *testing.T, req *syncer.SyncRequest, q *syncer.TargetQueue) {
	require.NoError(t, q.Push(req))
}

// chainInfoFromHeight is a helper that constructs a unique chain info off of
// an int. The tipset key is a faked cid from the string of that integer and
// the height is that integer.
func chainInfoFromHeight(t *testing.T, h int) types.ChainInfo {
	hStr := strconv.Itoa(h)
	c := types.CidFromString(t, hStr)
	return block.ChainInfo{
		Head: types.NewTipSetKey(c),
		Height: uint64(h),
	}
}
