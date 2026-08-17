package storage

import (
	"math/rand"
	"time"
)

const (
	maxLevel    = 16
	probability = 0.5
)

type SkipListNode struct {
	key     string
	value   []byte
	deleted bool
	forward []*SkipListNode
}

type SkipList struct {
	head  *SkipListNode
	level int
	size  int // estimated bytes
	len   int // number of items
	rand  *rand.Rand
}

func NewSkipList() *SkipList {
	return &SkipList{
		head: &SkipListNode{
			forward: make([]*SkipListNode, maxLevel),
		},
		level: 1,
		rand:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for sl.rand.Float64() < probability && level < maxLevel {
		level++
	}
	return level
}

func (sl *SkipList) Put(key string, value []byte) {
	update := make([]*SkipListNode, maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	if current != nil && current.key == key {
		// Update existing
		sl.size += len(value) - len(current.value)
		current.value = make([]byte, len(value))
		copy(current.value, value)
		current.deleted = false
		return
	}

	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	newNode := &SkipListNode{
		key:     key,
		value:   valCopy,
		forward: make([]*SkipListNode, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.len++
	sl.size += len(key) + len(value) + 8 + (newLevel * 8)
}

func (sl *SkipList) Get(key string) ([]byte, bool) {
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}
	current = current.forward[0]

	if current != nil && current.key == key {
		if current.deleted {
			return nil, false
		}
		return current.value, true
	}
	return nil, false
}

func (sl *SkipList) Delete(key string) bool {
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}
	current = current.forward[0]

	if current != nil && current.key == key && !current.deleted {
		current.deleted = true
		sl.size -= len(current.value)
		current.value = nil
		return true
	}
	
	// If not found, insert a tombstone
	if current == nil || current.key != key {
		sl.Put(key, nil)
		// immediately mark deleted
		// Re-find and mark
		c2 := sl.head
		for i := sl.level - 1; i >= 0; i-- {
			for c2.forward[i] != nil && c2.forward[i].key < key {
				c2 = c2.forward[i]
			}
		}
		c2 = c2.forward[0]
		if c2 != nil && c2.key == key {
			c2.deleted = true
		}
		return true
	}
	
	return false
}

func (sl *SkipList) Len() int {
	return sl.len
}

func (sl *SkipList) Size() int {
	return sl.size
}

type SkipListIterator struct {
	sl      *SkipList
	current *SkipListNode
}

func (sl *SkipList) Iterator() *SkipListIterator {
	it := &SkipListIterator{
		sl: sl,
	}
	it.SeekToFirst()
	return it
}

func (it *SkipListIterator) SeekToFirst() {
	it.current = it.sl.head.forward[0]
}

func (it *SkipListIterator) Next() {
	if it.current != nil {
		it.current = it.current.forward[0]
	}
}

func (it *SkipListIterator) Valid() bool {
	return it.current != nil
}

func (it *SkipListIterator) Key() string {
	if it.current != nil {
		return it.current.key
	}
	return ""
}

func (it *SkipListIterator) Value() []byte {
	if it.current != nil {
		return it.current.value
	}
	return nil
}

func (it *SkipListIterator) Deleted() bool {
	if it.current != nil {
		return it.current.deleted
	}
	return false
}
