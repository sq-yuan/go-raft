package concurrent

import (
	"sync"
)

const SHARD_COUNT = 32

// A concurrent map
type Map[K comparable, V any] struct {
	shards   []*MapShard[K, V]
	hashFunc func(key K) uint32
}

type MapShard[K comparable, V any] struct {
	Items map[K]V
	sync.RWMutex
}

func create[K comparable, V any](hashFunc func(key K) uint32, existingData map[K]V) *Map[K, V] {
	m := Map[K, V]{
		shards:   make([]*MapShard[K, V], SHARD_COUNT),
		hashFunc: hashFunc,
	}
	for i := 0; i < SHARD_COUNT; i++ {
		m.shards[i] = &MapShard[K, V]{Items: make(map[K]V)}
	}
	for k, v := range existingData {
		hash := m.hashFunc(k)
		shard := hash % SHARD_COUNT
		m.shards[shard].Items[k] = v
	}
	return &m
}

func (m *Map[K, V]) GetShardCount() int {
	return SHARD_COUNT
}

func (m *Map[K, V]) GetShardByIndex(i int) *MapShard[K, V] {
	return m.shards[i]
}

func (m *Map[K, V]) GetShard(k K) *MapShard[K, V] {
	hash := m.hashFunc(k)
	shard := hash % SHARD_COUNT
	return m.shards[shard]
}

func NewMap[V any]() *Map[string, V] {
	return create[string, V](stringKeyhash, nil)
}

func NewMapFrom[V any](data map[string]V) *Map[string, V] {
	return create[string, V](stringKeyhash, data)
}

func (m *Map[K, V]) Remove(key K) {
	shard := m.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	delete(shard.Items, key)
}

func (m *Map[K, V]) Set(key K, value V) {
	shard := m.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.Items[key] = value
}

func (m *Map[K, V]) Exist(key K) bool {
	shard := m.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	_, ok := shard.Items[key]
	return ok
}

func (m *Map[K, V]) Get(key K) *V {
	shard := m.GetShard(key)
	shard.RLock()
	defer shard.RUnlock()
	if val, ok := shard.Items[key]; ok {
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
