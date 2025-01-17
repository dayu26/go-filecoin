package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-filecoin/internal/pkg/version"

	"github.com/filecoin-project/go-filecoin/internal/pkg/address"
	"github.com/filecoin-project/go-filecoin/internal/pkg/consensus"
	th "github.com/filecoin-project/go-filecoin/internal/pkg/testhelpers"
	tf "github.com/filecoin-project/go-filecoin/internal/pkg/testhelpers/testflags"
	"github.com/filecoin-project/go-filecoin/internal/pkg/types"
)

// TestMessagePropagation is a high level check that messages are propagated between message
// pools of connected ndoes.
func TestMessagePropagation(t *testing.T) {
	tf.UnitTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Generate a key and install an account actor at genesis which will be able to send messages.
	ki := types.MustGenerateKeyInfo(1, 42)[0]
	senderAddress, err := ki.Address()
	require.NoError(t, err)
	genesis := consensus.MakeGenesisFunc(
		consensus.ActorAccount(senderAddress, types.NewAttoFILFromFIL(100)),
		consensus.Network(version.TEST),
	)

	// Initialize the first node to be the message sender.
	senderNodeOpts := TestNodeOptions{
		GenesisFunc: genesis,
		BuilderOpts: DefaultTestingConfig(),
		InitOpts: []InitOpt{
			DefaultKeyOpt(&ki),
		},
	}
	sender := GenNode(t, &senderNodeOpts)

	// Initialize other nodes to receive the message.
	receiverCount := 2
	receivers := MakeNodesUnstartedWithGif(t, receiverCount, false, genesis)

	nodes := append([]*Node{sender}, receivers...)
	StartNodes(t, nodes)
	defer StopNodes(nodes)

	// Connect nodes in series
	connect(t, nodes[0], nodes[1])
	connect(t, nodes[1], nodes[2])
	// Wait for network connection notifications to propagate
	time.Sleep(time.Millisecond * 50)

	require.Equal(t, 0, len(nodes[1].Messaging.Inbox.Pool().Pending()))
	require.Equal(t, 0, len(nodes[2].Messaging.Inbox.Pool().Pending()))
	require.Equal(t, 0, len(nodes[0].Messaging.Inbox.Pool().Pending()))

	t.Run("message propagates", func(t *testing.T) {
		_, err := sender.PorcelainAPI.MessageSend(
			ctx,
			senderAddress,
			address.NetworkAddress,
			types.NewAttoFILFromFIL(1),
			types.NewGasPrice(1),
			types.NewGasUnits(0),
			"foo",
		)
		require.NoError(t, err)

		require.NoError(t, th.WaitForIt(50, 100*time.Millisecond, func() (bool, error) {
			return len(nodes[0].Messaging.Inbox.Pool().Pending()) == 1 &&
				len(nodes[1].Messaging.Inbox.Pool().Pending()) == 1 &&
				len(nodes[2].Messaging.Inbox.Pool().Pending()) == 1, nil
		}), "failed to propagate messages")

		assert.True(t, nodes[0].Messaging.Inbox.Pool().Pending()[0].Message.Method == "foo")
		assert.True(t, nodes[1].Messaging.Inbox.Pool().Pending()[0].Message.Method == "foo")
		assert.True(t, nodes[2].Messaging.Inbox.Pool().Pending()[0].Message.Method == "foo")
	})
}
