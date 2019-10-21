package validation

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-hamt-ipld"
	blockstore "github.com/ipfs/go-ipfs-blockstore"

	vstate "github.com/filecoin-project/chain-validation/pkg/state"

	"github.com/filecoin-project/go-filecoin/actor"
	"github.com/filecoin-project/go-filecoin/address"
	"github.com/filecoin-project/go-filecoin/crypto"
	"github.com/filecoin-project/go-filecoin/state"
	"github.com/filecoin-project/go-filecoin/types"
	"github.com/filecoin-project/go-filecoin/vm"
	wutil "github.com/filecoin-project/go-filecoin/wallet/util"
)

type StateWrapper struct {
	state.Tree
	vm.StorageMap
	keys *keyStore
}

var _ vstate.Wrapper = &StateWrapper{}

func NewState() *StateWrapper {
	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	cst := hamt.CSTFromBstore(bs)
	treeImpl := state.NewEmptyStateTree(cst)
	storageImpl := vm.NewStorageMap(bs)
	return &StateWrapper{treeImpl, storageImpl, newKeyStore()}
}

func (s *StateWrapper) Cid() cid.Cid {
	panic("implement me")
}

func (s *StateWrapper) Actor(addr vstate.Address) (vstate.Actor, error) {
	vaddr, err := address.NewFromBytes([]byte(addr))
	if err != nil {
		return nil, err
	}
	fcActor, err := s.Tree.GetActor(context.TODO(), vaddr)
	if err != nil {
		return nil, err
	}
	return &actorWrapper{*fcActor}, nil
}

func (s *StateWrapper) Storage(addr vstate.Address) (vstate.Storage, error) {
	addrInt, err := address.NewFromBytes([]byte(addr))
	if err != nil {
		return nil, err
	}

	actor, err := s.Tree.GetActor(context.TODO(), addrInt)
	if err != nil {
		return nil, err
	}

	storageInt := s.StorageMap.NewStorage(addrInt, actor)
	// The internal storage implements vstate.Storage directly for now.
	return storageInt, nil
}

func (s *StateWrapper) NewAccountAddress() (vstate.Address, error) {
	return s.keys.newAddress()
}

func (s *StateWrapper) SetActor(addr vstate.Address, code cid.Cid, balance vstate.AttoFIL) (vstate.Actor, vstate.Storage, error) {
	ctx := context.TODO()
	addrInt, err := address.NewFromBytes([]byte(addr))
	if err != nil {
		return nil, nil, err
	}
	actr := &actorWrapper{actor.Actor{
		Code:    code,
		Balance: types.NewAttoFIL(balance),
	}}
	if err := s.Tree.SetActor(ctx, addrInt, &actr.Actor); err != nil {
		return nil, nil, err
	}
	_, err = s.Tree.Flush(ctx)
	if err != nil {
		return nil, nil, err
	}

	storage := s.NewStorage(addrInt, &actr.Actor)
	return actr, storage, nil
}

func (s *StateWrapper) Signer() *keyStore {
	return s.keys
}

//
// Key store
//

type keyStore struct {
	// Private keys by address
	keys map[address.Address]*types.KeyInfo
	// Seed for deterministic key generation.
	seed int64
}

func newKeyStore() *keyStore {
	return &keyStore{
		keys: make(map[address.Address]*types.KeyInfo),
		seed: 0,
	}
}

func (s *keyStore) newAddress() (vstate.Address, error) {
	randSrc := rand.New(rand.NewSource(s.seed))
	prv, err := crypto.GenerateKeyFromSeed(randSrc)
	if err != nil {
		return "", err
	}

	ki := &types.KeyInfo{
		PrivateKey: prv,
		Curve:      "secp256k1",
	}
	addr, err := ki.Address()
	if err != nil {
		return "", err
	}
	s.keys[addr] = ki
	s.seed++
	return vstate.Address(addr.Bytes()), nil
}

func (as *keyStore) SignBytes(data []byte, addr address.Address) (types.Signature, error) {
	ki, ok := as.keys[addr]
	if !ok {
		return types.Signature{}, fmt.Errorf("unknown address %v", addr)
	}
	return wutil.Sign(ki.Key(), data)
}

//
// Actor Wrapper
//

type actorWrapper struct {
	actor.Actor
}

func (a *actorWrapper) Code() cid.Cid {
	return a.Actor.Code
}

func (a *actorWrapper) Head() cid.Cid {
	return a.Actor.Head
}

func (a *actorWrapper) Nonce() uint64 {
	return uint64(a.Actor.Nonce)
}

func (a *actorWrapper) Balance() vstate.AttoFIL {
	return a.Actor.Balance.AsBigInt()
}
