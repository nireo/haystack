package store

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/nireo/haystack/logical"
)

var ErrLogicalAlreadyExists = errors.New("logical already exists")

// Store has the logic for managing multiple logical volumes. It keeps track of all of the logical
// volumes that exist.
type Store struct {
	mu       sync.RWMutex
	index    map[string]map[string]logical.FileMetadata // logical file id -> ( index of file with file id -> location on file)
	logicals map[string]*logical.Logical                // logical file id -> logile file on disk
	dir      string
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %s", err)
	}

	return &Store{
		index:    make(map[string]map[string]logical.FileMetadata),
		logicals: make(map[string]*logical.Logical),
		dir:      dir,
	}, nil
}

func (s *Store) NewLogical(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.logicals[id]; ok {
		log.Printf("[INFO] store: logical volume %s already exists", id)
		return nil
	}
	filePath := path.Join(s.dir, id)
	logicalVol, err := logical.NewLogical(filePath)
	if err != nil {
		return err
	}

	s.logicals[id] = logicalVol
	if s.index[id] == nil {
		s.index[id] = make(map[string]logical.FileMetadata)
	}

	log.Printf("[INFO] store: created and registered logical volume %s", id)
	return nil
}

func (s *Store) getLogicalVolume(id string) (*logical.Logical, bool) {
	s.mu.RLock()
	logvol, ok := s.logicals[id]
	s.mu.RUnlock()

	return logvol, ok
}

// CreateFile creates a file in a given logical file. The logical file is decided by the
// directory which handles logic related to which file to write to. Only thing that store does is
// just write data to some logical file and maintain information about logical files.
func (s *Store) CreateFile(fileID, logicalID string, data []byte) error {
	logvol, ok := s.getLogicalVolume(logicalID)
	if !ok {
		return fmt.Errorf("could not file logical volume with id: %s", logicalID)
	}

	offset, err := logvol.WriteFile(fileID, data)
	if err != nil {
		return fmt.Errorf("error writing to logical volume: %s", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.index[logicalID]; !ok {
		s.index[logicalID] = make(map[string]logical.FileMetadata)
	}

	s.index[logicalID][fileID] = logical.FileMetadata{
		Offset: offset,
		Size:   int64(len(data)),
	}

	return nil
}

// Shutdown shuts down all of the logical files.
func (s *Store) Shutdown() {
	for _, logvol := range s.logicals {
		logvol.Close()
	}
}

// ReadFile reads a given file that is stored in a logical ID.
func (s *Store) ReadFile(fileID, logicalID string) ([]byte, error) {
	logvol, ok := s.getLogicalVolume(logicalID)
	if !ok {
		return nil, fmt.Errorf("could not get logical volume with id: %s", logicalID)
	}

	s.mu.RLock()
	metadata, ok := s.index[logicalID][fileID]
	if !ok {
		return nil, fmt.Errorf("no information about file [%s] found", fileID)
	}
	s.mu.RUnlock()

	// Files are append only so we don't need any kind of locking when reading the files.
	data, err := logvol.ReadBytes(metadata.Offset, metadata.Size)
	if err != nil {
		return nil, fmt.Errorf("error reading logical volume: %s", err)
	}

	return data, nil
}

// Recover reads all of the log files and restores the index in parallel. This is a slow operation as
// to reduce latency no WAL is kept and therefore we have to read through the entire file to build an index.
func (s *Store) Recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var wg sync.WaitGroup
	files, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("error reading logical volume directory: %s", err)
	}

	errs := make(chan error)
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			path := filepath.Join(s.dir, name)
			logvol, err := logical.NewLogical(path)
			if err != nil {
				errs <- err
				return
			}

			index, err := logvol.Recover()
			if err != nil {
				errs <- err
				return
			}

			s.index[name] = index
			s.logicals[name] = logvol
		}(file.Name())
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}
