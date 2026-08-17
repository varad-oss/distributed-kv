package storage

import (
	"encoding/binary"
)

type BloomFilter struct {
	bitset []byte
	k      int
}

func NewBloomFilter(numKeys int, bitsPerKey int) *BloomFilter {
	if numKeys < 0 {
		numKeys = 0
	}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	bits := numKeys * bitsPerKey
	if bits < 64 {
		bits = 64
	}
	bytes := (bits + 7) / 8
	bits = bytes * 8

	// k = ln(2) * (bits / keys)
	k := int(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	return &BloomFilter{
		bitset: make([]byte, bytes),
		k:      k,
	}
}

func murmur3(data []byte, seed uint32) uint32 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
		r1 = 15
		r2 = 13
		m  = 5
		n  = 0xe6546b64
	)
	hash := seed

	l := len(data)
	numBlocks := l / 4
	for i := 0; i < numBlocks; i++ {
		k := binary.LittleEndian.Uint32(data[i*4:])
		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2

		hash ^= k
		hash = (hash << r2) | (hash >> (32 - r2))
		hash = hash*m + n
	}

	tail := data[numBlocks*4:]
	var k1 uint32
	switch len(tail) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << r1) | (k1 >> (32 - r1))
		k1 *= c2
		hash ^= k1
	}

	hash ^= uint32(l)
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16

	return hash
}

func hash(key string) (uint32, uint32) {
	h := murmur3([]byte(key), 0x9747b28c)
	delta := (h >> 17) | (h << 15)
	return h, delta
}

func (bf *BloomFilter) Add(key string) {
	if len(bf.bitset) == 0 {
		return
	}
	h, delta := hash(key)
	bits := uint32(len(bf.bitset) * 8)
	for i := 0; i < bf.k; i++ {
		bitPos := h % bits
		bf.bitset[bitPos/8] |= 1 << (bitPos % 8)
		h += delta
	}
}

func (bf *BloomFilter) MayContain(key string) bool {
	if len(bf.bitset) == 0 {
		return false
	}
	h, delta := hash(key)
	bits := uint32(len(bf.bitset) * 8)
	for i := 0; i < bf.k; i++ {
		bitPos := h % bits
		if (bf.bitset[bitPos/8] & (1 << (bitPos % 8))) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func (bf *BloomFilter) Serialize() []byte {
	out := make([]byte, 4+len(bf.bitset))
	binary.LittleEndian.PutUint32(out[0:4], uint32(bf.k))
	copy(out[4:], bf.bitset)
	return out
}

func (bf *BloomFilter) Deserialize(data []byte) {
	if len(data) < 4 {
		return
	}
	bf.k = int(binary.LittleEndian.Uint32(data[0:4]))
	bf.bitset = make([]byte, len(data)-4)
	copy(bf.bitset, data[4:])
}
