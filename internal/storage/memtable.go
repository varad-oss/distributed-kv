package storage

import (
	"sync"
)

type MemTable struct {
	sl *SkipList
	mu sync.RWMutex
}

func NewMemTable() *MemTable {
	return &MemTable{
		sl: NewSkipList(),
	}
}

func (m *MemTable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Put(key, value)
}

func (m *MemTable) Get(key string) ([]byte, bool, bool) { // returns value, found, deleted
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Direct skip list node lookup to see if it's deleted
	current := m.sl.head
	for i := m.sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}
	current = current.forward[0]

	if current != nil && current.key == key {
		if current.deleted {
			return nil, true, true
		}
		valCopy := make([]byte, len(current.value))
		copy(valCopy, current.value)
		return valCopy, true, false
	}
	
	return nil, false, false
}

func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Delete(key)
}

func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Size()
}

func (m *MemTable) Iterator() *SkipListIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Iterator()
}
