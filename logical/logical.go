package logical

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// format on file:
// | uuid 16 bytes | length 8 bytes | length bytes for content |
//
// we cannot store just the file data as we need to be able to recover the state from the logical files.
const uuidLength = 16
const headerLength = uuidLength + 8

// FileMetadata contains information needed to read a file from disk.
type FileMetadata struct {
	Size   int64
	Offset int64
}

// Logical represents a large file that contains multiple different smaller files the metadata
// then shows how much to read and where. Since the storage is mainly append-only
// that means that we don't need locking for reading as its safe to read the file as no parts
// are modified or deleted.
type Logical struct {
	writeMutex sync.Mutex
	offset     int64
	file       *os.File
}

// NewLogical creates a new logical file at a given path.
func NewLogical(path string) (*Logical, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// ensure that the offset points to an existing place if the offset doesn't exist.
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	offset := stat.Size()
	return &Logical{
		offset: offset,
		file:   file,
	}, nil
}

// ReadBytes reads amount bytes at a given offset.
func (l *Logical) ReadBytes(offset, amount int64) ([]byte, error) {
	length := make([]byte, 8)

	// we skip the uuid here and subsequent reads
	_, err := l.file.ReadAt(length, offset+uuidLength)
	if err != nil {
		return nil, fmt.Errorf("error reading uuid length")
	}

	data := make([]byte, int(binary.LittleEndian.Uint64(length)))
	_, err = l.file.ReadAt(data, offset+headerLength)
	if err != nil {
		return nil, fmt.Errorf("error reading data")
	}

	return data, nil
}

// WriteImage returns the offset for a given image file the UUID is parsed and then
// encoded.
func (l *Logical) WriteFile(id string, data []byte) (int64, error) {
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()

	parsed, err := uuid.Parse(id)
	if err != nil {
		return 0, fmt.Errorf("error parsing uuid: %s", err)
	}

	encodedEntry := make([]byte, 0, headerLength)
	encodedEntry = append(encodedEntry, parsed[:]...)
	encodedEntry = binary.LittleEndian.AppendUint64(encodedEntry, uint64(len(data)))
	encodedEntry = append(encodedEntry, data...)

	storedOffset := atomic.LoadInt64(&l.offset)
	written, err := l.file.Write(encodedEntry)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&l.offset, int64(written))

	return storedOffset, nil
}

// Close closes the underlying file
func (l *Logical) Close() error {
	return l.file.Close()
}

func (l *Logical) Size() (int64, error) {
	stat, err := l.file.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

// Load constructs the metadata mapping of a file from the given
func (l *Logical) Recover() (map[string]FileMetadata, error) {
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()

	metadata := make(map[string]FileMetadata)

	var metadataEntry [headerLength]byte
	for {
		currOffset, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		_, err = io.ReadFull(l.file, metadataEntry[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		contentLength := binary.LittleEndian.Uint64(metadataEntry[16:])
		_, err = l.file.Seek(int64(contentLength), io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		uuid, err := uuid.FromBytes(metadataEntry[:16])
		if err != nil {
			return nil, fmt.Errorf("corrupted uuid: %s", err)
		}

		metadata[uuid.String()] = FileMetadata{
			Offset: currOffset,
			Size:   int64(contentLength),
		}
	}

	return metadata, nil
}
