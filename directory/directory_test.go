package directory

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockStore struct{}

func (s *MockStore) CreateLogicalVolume(args CreateLogicalVolumeStoreArgs, reply *CreateLogicalVolumeStoreReply) error {
	return nil
}

var mockStoreRegistered sync.Once

func registerMockStoreService() {
	mockStoreRegistered.Do(func() {
		err := rpc.RegisterName("Store", new(MockStore))
		if err != nil {
			log.Fatalf("Failed to register MockStore for testing: %v", err)
		}
	})
}

func startNMockStores(t *testing.T, numStores int) ([]net.Listener, []string) {
	t.Helper()
	registerMockStoreService()

	listeners := make([]net.Listener, numStores)
	addresses := make([]string, numStores)
	for i := range numStores {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err, "Failed to listen on a free port for mock store %d", i)

		go func(l net.Listener) {
			for {
				conn, errAccept := l.Accept()
				if errAccept != nil {
					if errors.Is(errAccept, net.ErrClosed) || strings.Contains(errAccept.Error(), "use of closed network connection") {
						return
					}
					log.Printf("Mock store server accept error: %v. Continuing...", errAccept)
					return
				}
				go rpc.ServeConn(conn)
			}
		}(listener)

		listeners[i] = listener
		addresses[i] = listener.Addr().String()
	}
	return listeners, addresses
}

func TestMain(m *testing.M) {
	log.SetOutput(os.Stdout)
	code := m.Run()
	os.Exit(code)
}

func TestNewDirectory(t *testing.T) {
	t.Run("Defaults", func(t *testing.T) {
		dir := NewDirectory(0, 0)
		require.NotNil(t, dir)
		assert.Equal(t, defaultReplicationFactor, dir.replicationFactor)
		assert.Equal(t, defaultMaxLVSize, dir.maxLVSize)
		assert.NotNil(t, dir.stores)
		assert.NotNil(t, dir.logicalVolumes)
		assert.NotNil(t, dir.fileIndex)
		assert.NotNil(t, dir.writableLVs)
	})

	t.Run("CustomValid", func(t *testing.T) {
		dir := NewDirectory(5, 1024)
		require.NotNil(t, dir)
		assert.Equal(t, 5, dir.replicationFactor)
		assert.Equal(t, int64(1024), dir.maxLVSize)
	})
}

func TestDirectory_RegisterStore(t *testing.T) {
	dir := NewDirectory(defaultReplicationFactor, defaultMaxLVSize)
	defer dir.Close()

	args1 := RegisterStoreArgs{StoreID: "store1", Address: "127.0.0.1:8001"}
	err := dir.RegisterStore(args1, &RegisterStoreReply{})
	require.NoError(t, err)
	assert.Contains(t, dir.stores, "store1")
	assert.Equal(t, "127.0.0.1:8001", dir.stores["store1"].Address)

	args2 := RegisterStoreArgs{StoreID: "store1", Address: "127.0.0.1:8002"}
	err = dir.RegisterStore(args2, &RegisterStoreReply{})
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8002", dir.stores["store1"].Address)

	err = dir.RegisterStore(RegisterStoreArgs{}, &RegisterStoreReply{})
	require.Error(t, err)
}

func TestDirectory_AssignAndGetLocations_Basic(t *testing.T) {
	const numStores = 3
	const testReplicationFactor = 3
	listeners, storeAddresses := startNMockStores(t, numStores)
	for _, l := range listeners {
		defer l.Close()
	}

	dir := NewDirectory(testReplicationFactor, defaultMaxLVSize)
	defer dir.Close()

	for i := range numStores {
		storeID := "mockstore" + strconv.Itoa(i)
		err := dir.RegisterStore(RegisterStoreArgs{StoreID: storeID, Address: storeAddresses[i]}, &RegisterStoreReply{})
		require.NoError(t, err, "Failed to register store %s", storeID)
	}
	time.Sleep(10 * time.Millisecond) // Brief pause for RPC clients to potentially establish

	fileID := uuid.New().String()
	fileSize := int64(1024)
	assignArgs := AssignWriteLocationsArgs{FileID: fileID, FileSize: fileSize}
	var assignReply AssignWriteLocationsReply

	err := dir.AssignWriteLocations(assignArgs, &assignReply)
	require.NoError(t, err)
	require.NotEmpty(t, assignReply.LogicalVolumeID)
	require.Len(t, assignReply.Locations, testReplicationFactor)

	assert.NotNil(t, dir.fileIndex[fileID])
	assert.Equal(t, assignReply.LogicalVolumeID, dir.fileIndex[fileID].LogicalVolumeID)
	lv, ok := dir.logicalVolumes[assignReply.LogicalVolumeID]
	require.True(t, ok)
	assert.Equal(t, fileSize, lv.CurrentSize)

	// Verify the LV is in the writable set since it's not full
	_, inWritableSet := dir.writableLVs[assignReply.LogicalVolumeID]
	assert.True(t, inWritableSet, "LV should be in writable set when not full")

	storeLVID := ""
	for i, loc := range assignReply.Locations {
		assert.NotEmpty(t, loc.StoreID)
		assert.NotEmpty(t, loc.StoreAddress)
		assert.NotEmpty(t, loc.LogicalVolumeID)
		if i == 0 {
			storeLVID = loc.LogicalVolumeID
		} else {
			assert.Equal(t, storeLVID, loc.LogicalVolumeID, "All placements should have the same LV ID on stores for a given ALV")
		}
	}

	getArgs := GetReadLocationsArgs{FileID: fileID}
	var getReply GetReadLocationsReply
	err = dir.GetReadLocations(getArgs, &getReply)
	require.NoError(t, err)
	require.Len(t, getReply.Locations, testReplicationFactor)

	for i, readLoc := range getReply.Locations {
		found := false
		for _, writeLoc := range assignReply.Locations {
			if readLoc.StoreID == writeLoc.StoreID &&
				readLoc.StoreAddress == writeLoc.StoreAddress &&
				readLoc.LogicalVolumeID == writeLoc.LogicalVolumeID {
				found = true
				break
			}
		}
		assert.True(t, found, "Read location %d not found in assigned write locations: %+v", i, readLoc)
		assert.Equal(t, storeLVID, readLoc.LogicalVolumeID)
	}
}

func TestDirectory_AssignWriteLocations_FileSizeExceedsMaxLV(t *testing.T) {
	dir := NewDirectory(1, 100) // maxLVSize = 100
	defer dir.Close()

	listeners, storeAddresses := startNMockStores(t, 1)
	defer listeners[0].Close()
	err := dir.RegisterStore(RegisterStoreArgs{StoreID: "s1", Address: storeAddresses[0]}, &RegisterStoreReply{})
	require.NoError(t, err)

	assignArgs := AssignWriteLocationsArgs{FileID: "bigfile", FileSize: 200}
	var assignReply AssignWriteLocationsReply
	err = dir.AssignWriteLocations(assignArgs, &assignReply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds max LV size")
}

func TestDirectory_AssignWriteLocations_NotEnoughStoresForInitialLV(t *testing.T) {
	dir := NewDirectory(3, defaultMaxLVSize) // replicationFactor = 3
	defer dir.Close()

	listeners, storeAddresses := startNMockStores(t, 2) // Only 2 stores
	for _, l := range listeners {
		defer l.Close()
	}
	err := dir.RegisterStore(RegisterStoreArgs{StoreID: "s1", Address: storeAddresses[0]}, &RegisterStoreReply{})
	require.NoError(t, err)
	err = dir.RegisterStore(RegisterStoreArgs{StoreID: "s2", Address: storeAddresses[1]}, &RegisterStoreReply{})
	require.NoError(t, err)

	assignArgs := AssignWriteLocationsArgs{FileID: "somefile", FileSize: 100}
	var assignReply AssignWriteLocationsReply
	err = dir.AssignWriteLocations(assignArgs, &assignReply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not enough registered stores")
	assert.Contains(t, err.Error(), "to meet replication factor")
}

func TestDirectory_GetReadLocations_FileNotFound(t *testing.T) {
	dir := NewDirectory(1, defaultMaxLVSize)
	defer dir.Close()

	getArgs := GetReadLocationsArgs{FileID: "nonexistentfile"}
	var getReply GetReadLocationsReply
	err := dir.GetReadLocations(getArgs, &getReply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDirectory_LVFullAndNewLVCreation(t *testing.T) {
	const numStores = 1
	const testReplicationFactor = 1
	const maxLVSizeSmall = 100

	listeners, storeAddresses := startNMockStores(t, numStores)
	for _, l := range listeners {
		defer l.Close()
	}

	dir := NewDirectory(testReplicationFactor, maxLVSizeSmall)
	defer dir.Close()

	for i := range numStores {
		storeID := "mockstore-lvfull" + strconv.Itoa(i)
		err := dir.RegisterStore(RegisterStoreArgs{StoreID: storeID, Address: storeAddresses[i]}, &RegisterStoreReply{})
		require.NoError(t, err)
	}
	time.Sleep(10 * time.Millisecond)

	// First file: partially fills LV
	var reply1 AssignWriteLocationsReply
	err := dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "file1", FileSize: 60}, &reply1)
	require.NoError(t, err)
	lv1ID := reply1.LogicalVolumeID
	lv1, ok := dir.logicalVolumes[lv1ID]
	require.True(t, ok)
	assert.Equal(t, int64(60), lv1.CurrentSize)
	assert.True(t, lv1.IsWritable)

	// Verify LV is in writable set
	_, inWritableSet := dir.writableLVs[lv1ID]
	assert.True(t, inWritableSet, "LV should be in writable set when not full")

	// Second file: fills LV completely
	var reply2 AssignWriteLocationsReply
	err = dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "file2", FileSize: 40}, &reply2)
	require.NoError(t, err)
	assert.Equal(t, lv1ID, reply2.LogicalVolumeID)
	assert.Equal(t, int64(100), lv1.CurrentSize)
	assert.False(t, lv1.IsWritable)

	// Verify LV is removed from writable set when full
	_, inWritableSet = dir.writableLVs[lv1ID]
	assert.False(t, inWritableSet, "Full LV should be removed from writable set")
	assert.Len(t, dir.writableLVs, 0, "No writable LVs should remain")

	// Third file: should create new LV
	var reply3 AssignWriteLocationsReply
	err = dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "file3", FileSize: 10}, &reply3)
	require.NoError(t, err)
	lv2ID := reply3.LogicalVolumeID
	assert.NotEqual(t, lv1ID, lv2ID, "A new LV should have been created")

	lv2, ok := dir.logicalVolumes[lv2ID]
	require.True(t, ok)
	assert.Equal(t, int64(10), lv2.CurrentSize)
	assert.True(t, lv2.IsWritable)

	// Verify new LV is in writable set
	_, inWritableSet = dir.writableLVs[lv2ID]
	assert.True(t, inWritableSet, "New LV should be in writable set")
	assert.Len(t, dir.writableLVs, 1, "Should have exactly one writable LV")
}

func TestDirectory_DuplicateFileAssignment(t *testing.T) {
	const numStores = 1
	const testReplicationFactor = 1

	listeners, storeAddresses := startNMockStores(t, numStores)
	for _, l := range listeners {
		defer l.Close()
	}

	dir := NewDirectory(testReplicationFactor, defaultMaxLVSize)
	defer dir.Close()

	err := dir.RegisterStore(RegisterStoreArgs{StoreID: "store1", Address: storeAddresses[0]}, &RegisterStoreReply{})
	require.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// First assignment should succeed
	var reply1 AssignWriteLocationsReply
	err = dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "samefile", FileSize: 100}, &reply1)
	require.NoError(t, err)

	// Second assignment of same file should fail
	var reply2 AssignWriteLocationsReply
	err = dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "samefile", FileSize: 100}, &reply2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestDirectory_WritableLVManagement(t *testing.T) {
	const numStores = 1
	const testReplicationFactor = 1
	const maxLVSizeSmall = 150

	listeners, storeAddresses := startNMockStores(t, numStores)
	for _, l := range listeners {
		defer l.Close()
	}

	dir := NewDirectory(testReplicationFactor, maxLVSizeSmall)
	defer dir.Close()

	err := dir.RegisterStore(RegisterStoreArgs{StoreID: "store1", Address: storeAddresses[0]}, &RegisterStoreReply{})
	require.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// Initially no writable LVs
	assert.Len(t, dir.writableLVs, 0)

	// Create first LV
	var reply1 AssignWriteLocationsReply
	err = dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "file1", FileSize: 50}, &reply1)
	require.NoError(t, err)
	lv1ID := reply1.LogicalVolumeID

	// Should have one writable LV
	assert.Len(t, dir.writableLVs, 1)
	_, exists := dir.writableLVs[lv1ID]
	assert.True(t, exists)

	// Add more files to same LV
	var reply2 AssignWriteLocationsReply
	err = dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "file2", FileSize: 50}, &reply2)
	require.NoError(t, err)
	assert.Equal(t, lv1ID, reply2.LogicalVolumeID) // Same LV used

	// Still writable
	assert.Len(t, dir.writableLVs, 1)
	_, exists = dir.writableLVs[lv1ID]
	assert.True(t, exists)

	// Fill LV completely
	var reply3 AssignWriteLocationsReply
	err = dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "file3", FileSize: 50}, &reply3)
	require.NoError(t, err)
	assert.Equal(t, lv1ID, reply3.LogicalVolumeID)

	// LV should be removed from writable set when full
	assert.Len(t, dir.writableLVs, 0)
	_, exists = dir.writableLVs[lv1ID]
	assert.False(t, exists)

	// Next file should create new LV
	var reply4 AssignWriteLocationsReply
	err = dir.AssignWriteLocations(AssignWriteLocationsArgs{FileID: "file4", FileSize: 10}, &reply4)
	require.NoError(t, err)
	lv2ID := reply4.LogicalVolumeID
	assert.NotEqual(t, lv1ID, lv2ID)

	// New LV should be in writable set
	assert.Len(t, dir.writableLVs, 1)
	_, exists = dir.writableLVs[lv2ID]
	assert.True(t, exists)
}

func TestDirectory_Close(t *testing.T) {
	dir := NewDirectory(1, 100)

	listeners, storeAddresses := startNMockStores(t, 1)
	l := listeners[0]
	storeAddr := storeAddresses[0]

	err := dir.RegisterStore(RegisterStoreArgs{StoreID: "s1", Address: storeAddr}, &RegisterStoreReply{})
	require.NoError(t, err)

	_, err = dir.getOrEstablishStoreClient("s1")
	require.NoError(t, err)
	require.NotNil(t, dir.stores["s1"].Client)

	dir.Close()
	assert.Nil(t, dir.stores["s1"].Client, "Client should be nil after close")

	l.Close() // Close mock server listener after test
}
