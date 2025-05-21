package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/nireo/haystack/logical"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(tb testing.TB) (*Store, func()) {
	tb.Helper()
	tmpDir, err := os.MkdirTemp("", "storetest_")
	if err != nil {
		tb.Fatalf("Failed to create temp dir: %v", err)
	}

	s := &Store{
		index:    make(map[string]map[string]logical.FileMetadata),
		logicals: make(map[string]*logical.Logical),
		dir:      tmpDir,
	}

	cleanup := func() {
		s.Shutdown()
		os.RemoveAll(tmpDir)
	}

	return s, cleanup
}

func TestStore_NewLogical(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	logicalID := "vol1"
	err := s.NewLogical(logicalID)
	if err != nil {
		t.Fatalf("NewLogical failed: %v", err)
	}

	if _, ok := s.logicals[logicalID]; !ok {
		t.Errorf("Expected logical volume '%s' to be in logicals map", logicalID)
	}

	expectedPath := filepath.Join(s.dir, logicalID)
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected logical volume file '%s' to exist, but it doesn't", expectedPath)
	}
}

func TestStore_CreateAndReadFile(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	logicalID := uuid.NewString()
	fileID := uuid.NewString()
	data := []byte("hello world")

	err := s.NewLogical(logicalID)
	if err != nil {
		t.Fatalf("NewLogical failed: %v", err)
	}

	err = s.CreateFile(fileID, logicalID, data)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	metaMap, ok := s.index[logicalID]
	if !ok {
		t.Fatalf("Expected index to contain logicalID '%s'", logicalID)
	}
	meta, ok := metaMap[fileID]
	if !ok {
		t.Fatalf("Expected index for logicalID '%s' to contain fileID '%s'", logicalID, fileID)
	}

	if meta.Size != int64(len(data)) {
		t.Errorf("Expected metadata size %d, got %d", len(data), meta.Size)
	}

	readData, err := s.ReadFile(fileID, logicalID)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(readData, data) {
		t.Errorf("Read data mismatch: expected '%s', got '%s'", data, readData)
	}
}

func TestStore_CreateFile_NonExistentLogicalVolume(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	logicalID := uuid.NewString()
	fileID := uuid.NewString()
	data := []byte("test data")

	err := s.CreateFile(fileID, logicalID, data)
	require.Error(t, err, "Expected CreateFile to fail for non-existent logical volume, but it succeeded")
}

func TestStore_Recover(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storerecover_")
	require.NoError(t, err, "Failed to create temp dir for s1")
	defer os.RemoveAll(tmpDir)

	s1 := &Store{
		index:    make(map[string]map[string]logical.FileMetadata),
		logicals: make(map[string]*logical.Logical),
		dir:      tmpDir,
	}

	logicalID1 := uuid.NewString()
	logicalID2 := uuid.NewString()

	fileID1_1 := uuid.NewString()
	data1_1 := []byte("data for file1_1") // len = 16

	fileID1_2 := uuid.NewString()
	data1_2 := []byte("data for file1_2 in vol1")

	fileID2_1 := uuid.NewString()
	data2_1 := []byte("data for file2_1 in vol2")

	require.NoError(t, s1.NewLogical(logicalID1))
	require.NoError(t, s1.NewLogical(logicalID2))

	require.NoError(t, s1.CreateFile(fileID1_1, logicalID1, data1_1))

	require.NoError(t, s1.CreateFile(fileID1_2, logicalID1, data1_2))
	require.NoError(t, s1.CreateFile(fileID2_1, logicalID2, data2_1))

	s1.mu.RLock() // Ensure read consistency for fetching original metadata
	s1StoredMeta1_1 := s1.index[logicalID1][fileID1_1]
	s1StoredMeta1_2 := s1.index[logicalID1][fileID1_2]
	s1StoredMeta2_1 := s1.index[logicalID2][fileID2_1]
	s1.mu.RUnlock()

	s1.Shutdown() // Close files before recovery

	s2 := &Store{
		index:    make(map[string]map[string]logical.FileMetadata),
		logicals: make(map[string]*logical.Logical),
		dir:      tmpDir,
	}

	require.NoError(t, s2.Recover(), "s2.Recover() failed")

	assert.Len(t, s2.logicals, 2, "Expected 2 logical volumes after recover")
	assert.Contains(t, s2.logicals, logicalID1)
	assert.Contains(t, s2.logicals, logicalID2)

	assert.Len(t, s2.index, 2, "Expected 2 entries in s2.index map after recover")

	// --- Check logicalID1/fileID1_1 ---
	recoveredIndex1, ok1 := s2.index[logicalID1]
	require.True(t, ok1, "Missing index for %s after recover", logicalID1)
	assert.Len(t, recoveredIndex1, 2, "Expected 2 files in index for %s", logicalID1)

	recoveredMeta1_1, ok1_1 := recoveredIndex1[fileID1_1]
	require.True(t, ok1_1, "Missing metadata for %s/%s after recover", logicalID1, fileID1_1)
	// Assert against the original data's properties and the initially stored offset
	assert.Equal(t, s1StoredMeta1_1.Offset, recoveredMeta1_1.Offset, "Offset mismatch for %s/%s", logicalID1, fileID1_1)
	assert.Equal(t, int64(len(data1_1)), recoveredMeta1_1.Size, "Size mismatch for %s/%s (recovered vs original data len)", logicalID1, fileID1_1)

	// --- Check logicalID1/fileID1_2 ---
	recoveredMeta1_2, ok1_2 := recoveredIndex1[fileID1_2]
	require.True(t, ok1_2, "Missing metadata for %s/%s after recover", logicalID1, fileID1_2)
	assert.Equal(t, s1StoredMeta1_2.Offset, recoveredMeta1_2.Offset, "Offset mismatch for %s/%s", logicalID1, fileID1_2)
	assert.Equal(t, int64(len(data1_2)), recoveredMeta1_2.Size, "Size mismatch for %s/%s (recovered vs original data len)", logicalID1, fileID1_2)

	// --- Check logicalID2/fileID2_1 ---
	recoveredIndex2, ok2 := s2.index[logicalID2]
	require.True(t, ok2, "Missing index for %s after recover", logicalID2)
	assert.Len(t, recoveredIndex2, 1, "Expected 1 file in index for %s", logicalID2)

	recoveredMeta2_1, ok2_1 := recoveredIndex2[fileID2_1]
	require.True(t, ok2_1, "Missing metadata for %s/%s after recover", logicalID2, fileID2_1)
	assert.Equal(t, s1StoredMeta2_1.Offset, recoveredMeta2_1.Offset, "Offset mismatch for %s/%s", logicalID2, fileID2_1)
	assert.Equal(t, int64(len(data2_1)), recoveredMeta2_1.Size, "Size mismatch for %s/%s (recovered vs original data len)", logicalID2, fileID2_1)

	// Verify file readability after recovery
	readData, err := s2.ReadFile(fileID1_1, logicalID1)
	require.NoError(t, err)
	assert.Equal(t, data1_1, readData)

	readData, err = s2.ReadFile(fileID1_2, logicalID1)
	require.NoError(t, err)
	assert.Equal(t, data1_2, readData)

	readData, err = s2.ReadFile(fileID2_1, logicalID2)
	require.NoError(t, err)
	assert.Equal(t, data2_1, readData)

	s2.Shutdown()
}
