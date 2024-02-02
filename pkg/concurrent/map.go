package concurrent

import (
	"sync"
)

const SHARD_COUNT = 32

// A concurrent map
type Map[K comparable, V any] struct {
	shards   []*mapShard[K, V]
	hashFunc func(key K) uint32
}

type mapShard[K comparable, V any] struct {
	items map[K]V
	sync.RWMutex
}

func create[K comparable, V any](hashFunc func(key K) uint32) *Map[K, V] {
	m := Map[K, V]{
		shards:   make([]*mapShard[K, V], SHARD_COUNT),
		hashFunc: hashFunc,
	}
	for i := 0; i < SHARD_COUNT; i++ {
		m.shards[i] = &mapShard[K, V]{items: make(map[K]V)}
	}
	return &m
}

func (m *Map[K, V]) getShard(k K) *mapShard[K, V] {
	hash := m.hashFunc(k)
	shard := hash % SHARD_COUNT
	return m.shards[shard]
}

func NewMap[V any]() *Map[string, V] {
	return create[string, V](stringKeyhash)
}

func (m *Map[K, V]) Remove(key K) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	delete(shard.items, key)
}

func (m *Map[K, V]) Set(key K, value V) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.items[key] = value
}

func (m *Map[K, V]) Exist(key K) bool {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	_, ok := shard.items[key]
	return ok
}

func (m *Map[K, V]) Get(key K) *V {
	shard := m.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	if val, ok := shard.items[key]; ok {
		return &val
	} else {
		return nil
	}
}

func stringKeyhash(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
