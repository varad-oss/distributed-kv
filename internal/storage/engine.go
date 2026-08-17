package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Engine struct {
	dir             string
	wal             *WAL
	memtable        *MemTable
	immutable       *MemTable
	sstables        []*SSTableReader
	nextSSTableID   int
	memtableMaxSize int
	bloomBitsPerKey int
	mu              sync.RWMutex
	flushWg         sync.WaitGroup
}

func NewEngine(dataDir string, memtableMaxSize int, bloomBitsPerKey int) (*Engine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	wal, err := NewWAL(dataDir)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		dir:             dataDir,
		wal:             wal,
		memtable:        NewMemTable(),
		memtableMaxSize: memtableMaxSize,
		bloomBitsPerKey: bloomBitsPerKey,
	}

	if err := e.loadSSTables(); err != nil {
		return nil, err
	}

	if err := e.Recover(); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Engine) loadSSTables() error {
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return err
	}

	var sstFiles []string
	maxID := 0

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
			sstFiles = append(sstFiles, entry.Name())
			idStr := strings.TrimSuffix(entry.Name(), ".sst")
			id, err := strconv.Atoi(idStr)
			if err == nil && id > maxID {
				maxID = id
			}
		}
	}

	e.nextSSTableID = maxID + 1

	// Sort descending so we search newest first
	sort.Slice(sstFiles, func(i, j int) bool {
		idI, _ := strconv.Atoi(strings.TrimSuffix(sstFiles[i], ".sst"))
		idJ, _ := strconv.Atoi(strings.TrimSuffix(sstFiles[j], ".sst"))
		return idI > idJ
	})

	for _, file := range sstFiles {
		reader, err := NewSSTableReader(filepath.Join(e.dir, file))
		if err != nil {
			return err
		}
		e.sstables = append(e.sstables, reader)
	}

	return nil
}

func (e *Engine) Put(key string, value []byte) error {
	e.mu.RLock()
	// Check if memtable is too large
	needsFlush := e.memtable.Size() >= e.memtableMaxSize && e.immutable == nil
	e.mu.RUnlock()

	if needsFlush {
		e.mu.Lock()
		if e.memtable.Size() >= e.memtableMaxSize && e.immutable == nil {
			e.immutable = e.memtable
			e.memtable = NewMemTable()
			
			// Start background flush
			e.flushWg.Add(1)
			go e.flushMemTable()
		}
		e.mu.Unlock()
	}

	if err := e.wal.Write(WALEntry{Type: TypePut, Key: key, Value: value}); err != nil {
		return err
	}

	e.memtable.Put(key, value)
	return nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	e.mu.RLock()
	mem := e.memtable
	imm := e.immutable
	sstables := e.sstables
	e.mu.RUnlock()

	val, found, deleted := mem.Get(key)
	if found {
		if deleted {
			return nil, nil // Or "not found" error, nil means not found
		}
		return val, nil
	}

	if imm != nil {
		val, found, deleted = imm.Get(key)
		if found {
			if deleted {
				return nil, nil
			}
			return val, nil
		}
	}

	for _, sst := range sstables {
		val, found, deleted, err := sst.Get(key)
		if err != nil {
			return nil, err
		}
		if found {
			if deleted {
				return nil, nil
			}
			return val, nil
		}
	}

	return nil, nil
}

func (e *Engine) Delete(key string) error {
	e.mu.RLock()
	needsFlush := e.memtable.Size() >= e.memtableMaxSize && e.immutable == nil
	e.mu.RUnlock()

	if needsFlush {
		e.mu.Lock()
		if e.memtable.Size() >= e.memtableMaxSize && e.immutable == nil {
			e.immutable = e.memtable
			e.memtable = NewMemTable()
			e.flushWg.Add(1)
			go e.flushMemTable()
		}
		e.mu.Unlock()
	}

	if err := e.wal.Write(WALEntry{Type: TypeDelete, Key: key}); err != nil {
		return err
	}

	e.memtable.Delete(key)
	return nil
}

func (e *Engine) flushMemTable() {
	defer e.flushWg.Done()

	e.mu.RLock()
	imm := e.immutable
	nextID := e.nextSSTableID
	e.mu.RUnlock()

	if imm == nil {
		return
	}

	sstPath := filepath.Join(e.dir, fmt.Sprintf("%06d.sst", nextID))
	writer, err := NewSSTableWriter(sstPath, imm.sl.Len())
	if err != nil {
		// Log error in a real system
		return
	}

	it := imm.Iterator()
	for it.Valid() {
		writer.Add(it.Key(), it.Value(), it.Deleted())
		it.Next()
	}

	if err := writer.Finish(); err != nil {
		return
	}

	reader, err := NewSSTableReader(sstPath)
	if err != nil {
		return
	}

	e.mu.Lock()
	// Insert at the beginning (newest)
	e.sstables = append([]*SSTableReader{reader}, e.sstables...)
	e.nextSSTableID++
	e.immutable = nil
	e.wal.Truncate() // Safe because all previous edits are in SSTable
	e.mu.Unlock()
}

func (e *Engine) Recover() error {
	entries, err := e.wal.ReadAll()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.Type == TypePut {
			e.memtable.Put(entry.Key, entry.Value)
		} else if entry.Type == TypeDelete {
			e.memtable.Delete(entry.Key)
		}
	}
	return nil
}

func (e *Engine) Snapshot() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Wait for any ongoing flush to avoid concurrent issues with iteration
	// For simplicity, a true snapshot might compact everything. Here we just serialize everything.
	// Easiest is to iterate all SSTables backwards (oldest to newest), then immutable, then memtable
	// building a fresh map, then serialize it.

	kv := make(map[string][]byte)

	for i := len(e.sstables) - 1; i >= 0; i-- {
		sst := e.sstables[i]
		it := sst.NewIterator()
		for {
			k, v, del, err := it.Next()
			if err != nil {
				break
			}
			if del {
				delete(kv, k)
			} else {
				kv[k] = v
			}
		}
	}

	if e.immutable != nil {
		it := e.immutable.Iterator()
		for it.Valid() {
			if it.Deleted() {
				delete(kv, it.Key())
			} else {
				kv[it.Key()] = it.Value()
			}
			it.Next()
		}
	}

	it := e.memtable.Iterator()
	for it.Valid() {
		if it.Deleted() {
			delete(kv, it.Key())
		} else {
			kv[it.Key()] = it.Value()
		}
		it.Next()
	}

	var buf bytes.Buffer
	for k, v := range kv {
		kLen := uint16(len(k))
		vLen := uint32(len(v))
		binary.Write(&buf, binary.LittleEndian, kLen)
		buf.WriteString(k)
		binary.Write(&buf, binary.LittleEndian, vLen)
		buf.Write(v)
	}

	return buf.Bytes(), nil
}

func (e *Engine) Restore(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Wait for flush to complete just in case
	e.flushWg.Wait()

	// Close all SSTables
	for _, sst := range e.sstables {
		sst.Close()
	}
	e.sstables = nil
	e.memtable = NewMemTable()
	e.immutable = nil
	e.wal.Truncate()

	// Remove old SST files
	entries, err := os.ReadDir(e.dir)
	if err == nil {
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".sst") {
				os.Remove(filepath.Join(e.dir, entry.Name()))
			}
		}
	}

	pos := 0
	for pos < len(data) {
		kLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2
		k := string(data[pos : pos+int(kLen)])
		pos += int(kLen)
		vLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		v := data[pos : pos+int(vLen)]
		pos += int(vLen)

		e.memtable.Put(k, v)
		e.wal.Write(WALEntry{Type: TypePut, Key: k, Value: v})
	}

	return nil
}

func (e *Engine) Close() error {
	e.flushWg.Wait() // Wait for flushes to complete
	
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.wal != nil {
		e.wal.Close()
	}
	for _, sst := range e.sstables {
		sst.Close()
	}
	return nil
}
