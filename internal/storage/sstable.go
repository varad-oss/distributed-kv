package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type IndexEntry struct {
	Key    string
	Offset uint32
}

type SSTableWriter struct {
	file       *os.File
	offset     uint32
	index      []IndexEntry
	bloom      *BloomFilter
	entryCount int
}

func NewSSTableWriter(path string, expectedKeys int) (*SSTableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	
	return &SSTableWriter{
		file:  f,
		bloom: NewBloomFilter(expectedKeys, 10),
	}, nil
}

func (w *SSTableWriter) Add(key string, value []byte, deleted bool) error {
	// Add to index
	w.index = append(w.index, IndexEntry{
		Key:    key,
		Offset: w.offset,
	})
	
	// Add to bloom filter
	w.bloom.Add(key)
	
	// Write data block
	// Format: [deleted bool byte][key len uint16][key][value len uint32][value]
	keyLen := uint16(len(key))
	valLen := uint32(len(value))
	
	size := 1 + 2 + int(keyLen) + 4 + int(valLen)
	buf := make([]byte, size)
	
	if deleted {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	
	binary.LittleEndian.PutUint16(buf[1:3], keyLen)
	copy(buf[3:3+keyLen], key)
	
	binary.LittleEndian.PutUint32(buf[3+keyLen:7+keyLen], valLen)
	if valLen > 0 {
		copy(buf[7+keyLen:], value)
	}
	
	n, err := w.file.Write(buf)
	if err != nil {
		return err
	}
	
	w.offset += uint32(n)
	w.entryCount++
	return nil
}

func (w *SSTableWriter) Finish() error {
	indexOffset := w.offset
	
	// Write index block
	// Format: [num entries uint32][entry 1... entry N]
	// Entry format: [key len uint16][key][offset uint32]
	var indexBuf []byte
	numEntriesBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(numEntriesBuf, uint32(len(w.index)))
	indexBuf = append(indexBuf, numEntriesBuf...)
	
	for _, entry := range w.index {
		kLen := uint16(len(entry.Key))
		b := make([]byte, 2+kLen+4)
		binary.LittleEndian.PutUint16(b[0:2], kLen)
		copy(b[2:2+kLen], entry.Key)
		binary.LittleEndian.PutUint32(b[2+kLen:], entry.Offset)
		indexBuf = append(indexBuf, b...)
	}
	
	n, err := w.file.Write(indexBuf)
	if err != nil {
		return err
	}
	w.offset += uint32(n)
	
	bloomOffset := w.offset
	
	// Write bloom filter
	bloomData := w.bloom.Serialize()
	n, err = w.file.Write(bloomData)
	if err != nil {
		return err
	}
	w.offset += uint32(n)
	
	// Write footer
	// Format: [index offset uint32][bloom offset uint32][entry count uint32]
	footer := make([]byte, 12)
	binary.LittleEndian.PutUint32(footer[0:4], indexOffset)
	binary.LittleEndian.PutUint32(footer[4:8], bloomOffset)
	binary.LittleEndian.PutUint32(footer[8:12], uint32(w.entryCount))
	
	if _, err := w.file.Write(footer); err != nil {
		return err
	}
	
	return w.file.Close()
}

type SSTableReader struct {
	file        *os.File
	index       []IndexEntry
	bloom       *BloomFilter
	indexOffset uint32
}

func NewSSTableReader(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	
	if stat.Size() < 12 {
		f.Close()
		return nil, fmt.Errorf("file too small to be SSTable")
	}
	
	// Read footer
	footer := make([]byte, 12)
	_, err = f.ReadAt(footer, stat.Size()-12)
	if err != nil {
		f.Close()
		return nil, err
	}
	
	indexOffset := binary.LittleEndian.Uint32(footer[0:4])
	bloomOffset := binary.LittleEndian.Uint32(footer[4:8])
	
	// Read Bloom Filter
	bloomSize := uint32(stat.Size()) - 12 - bloomOffset
	bloomData := make([]byte, bloomSize)
	if _, err := f.ReadAt(bloomData, int64(bloomOffset)); err != nil {
		f.Close()
		return nil, err
	}
	
	bloom := &BloomFilter{}
	bloom.Deserialize(bloomData)
	
	// Read Index
	indexSize := bloomOffset - indexOffset
	indexData := make([]byte, indexSize)
	if _, err := f.ReadAt(indexData, int64(indexOffset)); err != nil {
		f.Close()
		return nil, err
	}
	
	numEntries := binary.LittleEndian.Uint32(indexData[0:4])
	index := make([]IndexEntry, 0, numEntries)
	
	pos := uint32(4)
	for i := uint32(0); i < numEntries; i++ {
		kLen := binary.LittleEndian.Uint16(indexData[pos : pos+2])
		key := string(indexData[pos+2 : pos+2+uint32(kLen)])
		offset := binary.LittleEndian.Uint32(indexData[pos+2+uint32(kLen) : pos+2+uint32(kLen)+4])
		
		index = append(index, IndexEntry{
			Key:    key,
			Offset: offset,
		})
		
		pos += 2 + uint32(kLen) + 4
	}
	
	return &SSTableReader{
		file:        f,
		index:       index,
		bloom:       bloom,
		indexOffset: indexOffset,
	}, nil
}

func (r *SSTableReader) Contains(key string) bool {
	return r.bloom.MayContain(key)
}

func (r *SSTableReader) Get(key string) ([]byte, bool, bool, error) {
	if !r.Contains(key) {
		return nil, false, false, nil
	}
	
	// Binary search index
	left, right := 0, len(r.index)-1
	foundIdx := -1
	
	for left <= right {
		mid := left + (right-left)/2
		if r.index[mid].Key == key {
			foundIdx = mid
			break
		} else if r.index[mid].Key < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	
	if foundIdx == -1 {
		return nil, false, false, nil
	}
	
	offset := r.index[foundIdx].Offset
	
	// Read data at offset
	headerBuf := make([]byte, 3)
	if _, err := r.file.ReadAt(headerBuf, int64(offset)); err != nil {
		return nil, false, false, err
	}
	
	deleted := headerBuf[0] == 1
	if deleted {
		return nil, true, true, nil
	}
	
	keyLen := binary.LittleEndian.Uint16(headerBuf[1:3])
	valLenBuf := make([]byte, 4)
	if _, err := r.file.ReadAt(valLenBuf, int64(offset)+3+int64(keyLen)); err != nil {
		return nil, false, false, err
	}
	
	valLen := binary.LittleEndian.Uint32(valLenBuf)
	value := make([]byte, valLen)
	
	if valLen > 0 {
		if _, err := r.file.ReadAt(value, int64(offset)+3+int64(keyLen)+4); err != nil {
			return nil, false, false, err
		}
	}
	
	return value, true, false, nil
}

func (r *SSTableReader) Close() error {
	return r.file.Close()
}

type SSTableIterator struct {
	reader *SSTableReader
	pos    uint32
}

func (r *SSTableReader) NewIterator() *SSTableIterator {
	return &SSTableIterator{
		reader: r,
		pos:    0,
	}
}

func (it *SSTableIterator) Next() (string, []byte, bool, error) {
	if it.pos >= it.reader.indexOffset {
		return "", nil, false, io.EOF
	}
	
	headerBuf := make([]byte, 3)
	if _, err := it.reader.file.ReadAt(headerBuf, int64(it.pos)); err != nil {
		return "", nil, false, err
	}
	
	deleted := headerBuf[0] == 1
	keyLen := binary.LittleEndian.Uint16(headerBuf[1:3])
	
	keyBuf := make([]byte, keyLen)
	if _, err := it.reader.file.ReadAt(keyBuf, int64(it.pos)+3); err != nil {
		return "", nil, false, err
	}
	key := string(keyBuf)
	
	valLenBuf := make([]byte, 4)
	if _, err := it.reader.file.ReadAt(valLenBuf, int64(it.pos)+3+int64(keyLen)); err != nil {
		return "", nil, false, err
	}
	valLen := binary.LittleEndian.Uint32(valLenBuf)
	
	var value []byte
	if valLen > 0 {
		value = make([]byte, valLen)
		if _, err := it.reader.file.ReadAt(value, int64(it.pos)+3+int64(keyLen)+4); err != nil {
			return "", nil, false, err
		}
	}
	
	it.pos += 3 + uint32(keyLen) + 4 + valLen
	
	return key, value, deleted, nil
}
