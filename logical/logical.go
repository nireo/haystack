package logical

import (
	"os"
	"sync"
)

// logical represents a large file that contains multiple different files stored in a large file
// the metadata then nows how much to read and where. Since the storage is mainly append-only
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
	data := make([]byte, amount)

	_, err := l.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// WriteImage returns the offset for a given image file.
func (l *Logical) WriteBytes(data []byte) (int64, error) {
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()
	storedOffset := l.offset

	written, err := l.file.Write(data)
	if err != nil {
		return 0, err
	}
	l.offset += int64(written)

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
