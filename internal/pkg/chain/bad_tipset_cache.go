package chain

import (
	"sync"

	"github.com/filecoin-project/go-filecoin/internal/pkg/block"
)

// badTipSetCache keeps track of bad tipsets that the syncer should not try to
// download. Readers and writers grab a lock. The purpose of this cache is to
// prevent a node from having to repeatedly invalidate a block (and its children)
// in the event that the tipset does not conform to the rules of consensus. Note
// that the cache is only in-memory, so it is reset whenever the node is restarted.
// TODO: this needs to be limited.
type badTipSetCache struct {
	mu  sync.Mutex
	bad map[string]struct{}
}

// AddChain adds the chain of tipsets to the badTipSetCache.  For now it just
// does the simplest thing and adds all blocks of the chain to the cache.
// TODO: might want to cache a random subset once cache size is limited.
func (cache *badTipSetCache) AddChain(chain []block.TipSet) {
	for _, ts := range chain {
		cache.Add(ts.String())
	}
}

// Add adds a single tipset key to the badTipSetCache.
func (cache *badTipSetCache) Add(tsKey string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.bad[tsKey] = struct{}{}
}

// Has checks for membership in the badTipSetCache.
func (cache *badTipSetCache) Has(tsKey string) bool {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	_, ok := cache.bad[tsKey]
	return ok
}
