package store

import (
	"fmt"
	"path"

	"github.com/nireo/haystack/logical"
)

type FileMetadata struct {
	size   int64
	offset int64
	file   *logical.Logical
}

// Store has the logic for managing multiple logical volumes. It keeps track of all of the logical
// volumes that exist.
type Store struct {
	metadata map[string]FileMetadata     // file id -> metadata
	logicals map[string]*logical.Logical // logical id -> the file itself
	dir      string
}

func (s *Store) NewLogical(id string) error {
	logical, err := logical.NewLogical(path.Join(s.dir, id))
	if err != nil {
		return err
	}

	s.logicals[id] = logical
	return nil
}

func (s *Store) StoreFile(id, logicalId string, data []byte) error {
	logical, ok := s.logicals[logicalId]
	if !ok {
		return fmt.Errorf("logical volume not found: %s", logicalId)
	}

	offset, err := logical.WriteBytes(data)
	if err != nil {
		return fmt.Errorf("failed to write bytes to logical: %s", err)
	}

	s.metadata[id] = FileMetadata{
		offset: offset,
		size:   int64(len(data)),
		file:   logical,
	}

	return nil
}

func (s *Store) Getfile(id string) ([]byte, error) {
	metadata, ok := s.metadata[id]
	if !ok {
		return nil, fmt.Errorf("failed to find metadata for file: %s", id)
	}

	data, err := metadata.file.ReadBytes(metadata.offset, metadata.size)
	if err != nil {
		return nil, fmt.Errorf("failed to read bytes of metadata: %s", err)
	}

	return data, nil
}
