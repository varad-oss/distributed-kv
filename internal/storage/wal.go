package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	walFileName = "wal.log"
	TypePut    = byte(1)
	TypeDelete = byte(2)
)

type WALEntry struct {
	Type  byte
	Key   string
	Value []byte
}

type WAL struct {
	dir  string
	file *os.File
}

func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	
	path := filepath.Join(dir, walFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	
	return &WAL{
		dir:  dir,
		file: file,
	}, nil
}

func (w *WAL) Write(entry WALEntry) error {
	// Format: [4-byte length][type byte][key-len uint16][key][value-len uint32][value][4-byte CRC32]
	keyLen := uint16(len(entry.Key))
	valLen := uint32(len(entry.Value))
	
	entrySize := 1 + 2 + int(keyLen) + 4 + int(valLen) + 4
	totalSize := 4 + entrySize // Includes the length prefix
	
	buf := make([]byte, totalSize)
	
	binary.LittleEndian.PutUint32(buf[0:4], uint32(entrySize))
	buf[4] = entry.Type
	binary.LittleEndian.PutUint16(buf[5:7], keyLen)
	copy(buf[7:7+keyLen], entry.Key)
	
	valOffset := 7 + keyLen
	binary.LittleEndian.PutUint32(buf[valOffset:valOffset+4], valLen)
	if valLen > 0 {
		copy(buf[valOffset+4:valOffset+4+uint16(valLen)], entry.Value)
	}
	
	crcOffset := valOffset + 4 + uint16(valLen)
	
	// Calculate CRC32 of everything except the CRC32 itself
	checksum := crc32.ChecksumIEEE(buf[0:crcOffset])
	binary.LittleEndian.PutUint32(buf[crcOffset:crcOffset+4], checksum)
	
	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	
	return w.file.Sync()
}

func (w *WAL) ReadAll() ([]WALEntry, error) {
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}
	
	var entries []WALEntry
	
	for {
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(w.file, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, err
		}
		
		entrySize := binary.LittleEndian.Uint32(lenBuf)
		entryBuf := make([]byte, entrySize)
		_, err = io.ReadFull(w.file, entryBuf)
		if err != nil {
			return entries, err
		}
		
		// Verify checksum
		expectedChecksum := binary.LittleEndian.Uint32(entryBuf[entrySize-4:])
		
		// Recalculate checksum: length prefix + entry data without checksum
		crcData := make([]byte, 4+entrySize-4)
		copy(crcData[0:4], lenBuf)
		copy(crcData[4:], entryBuf[:entrySize-4])
		
		actualChecksum := crc32.ChecksumIEEE(crcData)
		if expectedChecksum != actualChecksum {
			return entries, fmt.Errorf("WAL corruption: CRC mismatch")
		}
		
		entryType := entryBuf[0]
		keyLen := binary.LittleEndian.Uint16(entryBuf[1:3])
		key := string(entryBuf[3 : 3+keyLen])
		
		valOffset := 3 + keyLen
		valLen := binary.LittleEndian.Uint32(entryBuf[valOffset : valOffset+4])
		
		var value []byte
		if valLen > 0 {
			value = make([]byte, valLen)
			copy(value, entryBuf[valOffset+4:valOffset+4+uint16(valLen)])
		}
		
		entries = append(entries, WALEntry{
			Type:  entryType,
			Key:   key,
			Value: value,
		})
	}
	
	// Seek to end for future writes
	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, err
	}
	
	return entries, nil
}

func (w *WAL) Truncate() error {
	w.file.Close()
	path := filepath.Join(w.dir, walFileName)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = file
	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
