package directory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCluster struct {
	Nodes    []*RaftService
	DataDirs []string
	mu       sync.RWMutex
}

func CreateTestCluster(t *testing.T, nodeCount int, replicationFactor int, maxLVSize int64) *TestCluster {
	if nodeCount < 1 {
		t.Fatal("nodeCount must be at least 1")
	}

	cluster := &TestCluster{
		Nodes:    make([]*RaftService, nodeCount),
		DataDirs: make([]string, nodeCount),
	}

	for i := range nodeCount {
		dataDir, err := os.MkdirTemp("", fmt.Sprintf("raft-test-node-%d-", i))
		if err != nil {
			t.Fatalf("Failed to create temp dir for node %d: %v", i, err)
		}
		cluster.DataDirs[i] = dataDir
	}

	ports := make([]int, nodeCount)
	for i := range nodeCount {
		port, err := getAvailablePort()
		if err != nil {
			cluster.Cleanup()
			t.Fatalf("Failed to get available port for node %d: %v", i, err)
		}
		ports[i] = port
	}

	nodeID := fmt.Sprintf("node-%d", 0)
	bindAddr := fmt.Sprintf("127.0.0.1:%d", ports[0])

	raftService, err := NewRaftService(nodeID, bindAddr, cluster.DataDirs[0], true, replicationFactor, maxLVSize, &mockHttpClient{})
	if err != nil {
		cluster.Cleanup()
		t.Fatalf("Failed to create bootstrap node: %v", err)
	}
	cluster.Nodes[0] = raftService

	if !waitForLeader(raftService, 10*time.Second) {
		cluster.Cleanup()
		t.Fatal("Bootstrap node failed to become leader")
	}

	for i := 1; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		bindAddr := fmt.Sprintf("127.0.0.1:%d", ports[i])

		raftService, err := NewRaftService(nodeID, bindAddr, cluster.DataDirs[i], false, replicationFactor, maxLVSize, &mockHttpClient{})
		if err != nil {
			cluster.Cleanup()
			t.Fatalf("Failed to create node %d: %v", i, err)
		}
		cluster.Nodes[i] = raftService

		err = cluster.Nodes[0].AddVoter(nodeID, bindAddr)
		if err != nil {
			cluster.Cleanup()
			t.Fatalf("Failed to add node %d to cluster: %v", i, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)

	return cluster
}

func (tc *TestCluster) GetLeader() *RaftService {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	for _, node := range tc.Nodes {
		if node == nil {
			continue
		}

		if node.IsLeader() {
			return node
		}
	}
	return nil
}

func (tc *TestCluster) GetFollowers() []*RaftService {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	var followers []*RaftService
	for _, node := range tc.Nodes {
		if !node.IsLeader() {
			followers = append(followers, node)
		}
	}
	return followers
}

func (tc *TestCluster) WaitForLeader(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if tc.GetLeader() != nil {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func (tc *TestCluster) WaitForConsensus(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		stats := make([]map[string]string, len(tc.Nodes))
		for i, node := range tc.Nodes {
			stats[i] = node.Stats()
		}

		if len(stats) > 0 {
			firstLastIndex := stats[0]["last_log_index"]
			consensus := true
			for i := 1; i < len(stats); i++ {
				if stats[i]["last_log_index"] != firstLastIndex {
					consensus = false
					break
				}
			}
			if consensus {
				return true
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func (tc *TestCluster) StopNode(index int) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if index < 0 || index >= len(tc.Nodes) {
		return fmt.Errorf("invalid node index: %d", index)
	}

	if tc.Nodes[index] != nil {
		err := tc.Nodes[index].Shutdown()
		tc.Nodes[index] = nil
		return err
	}
	return nil
}

func (tc *TestCluster) Cleanup() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for i, node := range tc.Nodes {
		if node != nil {
			_ = node.Shutdown()
			tc.Nodes[i] = nil
		}
	}

	for _, dataDir := range tc.DataDirs {
		_ = os.RemoveAll(dataDir)
	}
}

func waitForLeader(node *RaftService, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if node.IsLeader() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func getAvailablePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

func CreateTestClusterWithStores(t *testing.T, nodeCount int, storeCount int, replicationFactor int, maxLVSize int64) (*TestCluster, []string) {
	if storeCount < replicationFactor {
		t.Fatalf("storeCount (%d) must be >= replicationFactor (%d)", storeCount, replicationFactor)
	}

	cluster := CreateTestCluster(t, nodeCount, replicationFactor, maxLVSize)

	addresses := createNAddrs(t, storeCount)

	if !cluster.WaitForLeader(5 * time.Second) {
		cluster.Cleanup()
		t.Fatal("No leader elected within timeout")
	}

	leader := cluster.GetLeader()
	for i, address := range addresses {
		storeID := fmt.Sprintf("store-%d", i)
		err := leader.RegisterStore(storeID, address)
		if err != nil {
			cluster.Cleanup()
			t.Fatalf("Failed to register store %s: %v", storeID, err)
		}
	}

	if !cluster.WaitForConsensus(5 * time.Second) {
		cluster.Cleanup()
		t.Fatal("Cluster failed to reach consensus after store registration")
	}

	return cluster, addresses
}

func CleanupClusterWithStores(cluster *TestCluster) {
	cluster.Cleanup()
}

func TestRaftClusterBasic(t *testing.T) {
	cluster, _ := CreateTestClusterWithStores(t, 3, 3, 2, 1024*1024)
	defer CleanupClusterWithStores(cluster)

	leader := cluster.GetLeader()
	if leader == nil {
		t.Fatal("Expected to have a leader")
	}

	lvID, locations, err := leader.AssignWriteLocations("file-1", 1024)
	if err != nil {
		t.Fatalf("Failed to assign write locations: %v", err)
	}

	if lvID == "" {
		t.Fatal("Expected logical volume ID")
	}

	if len(locations) != 2 {
		t.Fatalf("Expected %d write locations, got %d", 2, len(locations))
	}

	for i, location := range locations {
		if location.StoreID == "" {
			t.Fatalf("Location %d has empty store ID", i)
		}
		if location.StoreAddress == "" {
			t.Fatalf("Location %d has empty store address", i)
		}
		if location.LogicalVolumeID == "" {
			t.Fatalf("Location %d has empty logical volume ID", i)
		}
	}

	readLocations, err := leader.GetReadLocations("file-1")
	if err != nil {
		t.Fatalf("Failed to get read locations: %v", err)
	}

	if len(readLocations) != len(locations) {
		t.Fatalf("Expected %d read locations, got %d", len(locations), len(readLocations))
	}

	t.Logf("Successfully assigned file to LV %s with %d locations", lvID, len(locations))
}

func TestRaftLeaderElectionWithStores(t *testing.T) {
	cluster, _ := CreateTestClusterWithStores(t, 3, 4, 2, 1024*1024)
	defer CleanupClusterWithStores(cluster)

	var leaderIndex int = -1
	for i, node := range cluster.Nodes {
		if node.IsLeader() {
			leaderIndex = i
			break
		}
	}

	if leaderIndex == -1 {
		t.Fatal("Could not find leader index")
	}

	leader := cluster.GetLeader()
	_, locations, err := leader.AssignWriteLocations("test-file", 512)
	if err != nil {
		t.Fatalf("Failed to assign write locations before leader failure: %v", err)
	}

	if len(locations) != 2 {
		t.Fatalf("Expected 2 locations, got %d", len(locations))
	}

	err = cluster.StopNode(leaderIndex)
	if err != nil {
		t.Fatalf("Failed to stop leader node: %v", err)
	}

	if !cluster.WaitForLeader(10 * time.Second) {
		t.Fatal("No new leader elected after leader failure")
	}

	newLeader := cluster.GetLeader()
	if newLeader == nil {
		t.Fatal("Expected new leader after failure")
	}

	readLocations, err := newLeader.GetReadLocations("test-file")
	if err != nil {
		t.Fatalf("Failed to get read locations after leader change: %v", err)
	}

	if len(readLocations) != 2 {
		t.Fatalf("Expected 2 read locations after leader change, got %d", len(readLocations))
	}

	_, newLocations, err := newLeader.AssignWriteLocations("test-file-2", 1024)
	if err != nil {
		t.Fatalf("Failed to assign new write locations after leader change: %v", err)
	}

	if len(newLocations) != 2 {
		t.Fatalf("Expected 2 new locations after leader change, got %d", len(newLocations))
	}

	t.Log("Successfully elected new leader and maintained functionality after leader failure")
}

func TestMultipleFileOperations(t *testing.T) {
	cluster, _ := CreateTestClusterWithStores(t, 3, 5, 3, 2048)
	defer CleanupClusterWithStores(cluster)

	leader := cluster.GetLeader()
	if leader == nil {
		t.Fatal("Expected to have a leader")
	}

	files := []struct {
		id   string
		size int64
	}{
		{"file-1", 512},
		{"file-2", 1024},
		{"file-3", 256},
		{"file-4", 1536},
	}

	for _, file := range files {
		lvID, locations, err := leader.AssignWriteLocations(file.id, file.size)
		if err != nil {
			t.Fatalf("Failed to assign write locations for %s: %v", file.id, err)
		}

		if len(locations) != 3 {
			t.Fatalf("Expected 3 write locations for %s, got %d", file.id, len(locations))
		}

		t.Logf("Assigned %s (size %d) to LV %s", file.id, file.size, lvID)
	}

	if !cluster.WaitForConsensus(5 * time.Second) {
		t.Fatal("Cluster failed to reach consensus after file assignments")
	}

	for _, file := range files {
		readLocations, err := leader.GetReadLocations(file.id)
		if err != nil {
			t.Fatalf("Failed to get read locations for %s: %v", file.id, err)
		}

		if len(readLocations) != 3 {
			t.Fatalf("Expected 3 read locations for %s, got %d", file.id, len(readLocations))
		}
	}

	_, _, err := leader.AssignWriteLocations("", 1024)
	if err == nil {
		t.Fatal("Expected error for empty file ID")
	}

	_, _, err = leader.AssignWriteLocations("file-1", 512)
	if err == nil {
		t.Fatal("Expected error for duplicate file")
	}

	_, err = leader.GetReadLocations("nonexistent-file")
	if err == nil {
		t.Fatal("Expected error for nonexistent file")
	}

	t.Log("Successfully completed multiple file operations with proper error handling")
}

type mockSnapshotSink struct {
	*bytes.Buffer
	closed   bool
	canceled bool
}

func newMockSnapshotSink() *mockSnapshotSink {
	return &mockSnapshotSink{
		Buffer: new(bytes.Buffer),
	}
}

func (m *mockSnapshotSink) Close() error {
	m.closed = true
	return nil
}

func (m *mockSnapshotSink) Cancel() error {
	m.canceled = true
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "test-snapshot"
}

func TestDirectorySnapshot_Basic(t *testing.T) {
	cluster, _ := CreateTestClusterWithStores(t, 3, 4, 3, 2048)
	defer CleanupClusterWithStores(cluster)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	files := []struct {
		id   string
		size int64
	}{
		{"file-1", 512},
		{"file-2", 1024},
		{"file-3", 256},
		{"file-4", 1536},
	}

	lvIDs := make([]string, len(files))
	for i, file := range files {
		lvID, locations, err := leader.AssignWriteLocations(file.id, file.size)
		require.NoError(t, err)
		assert.Len(t, locations, 3)
		lvIDs[i] = lvID
	}

	require.True(t, cluster.WaitForConsensus(5*time.Second))

	snapshot, err := leader.fsm.Snapshot()
	require.NoError(t, err)

	sink := newMockSnapshotSink()
	require.NoError(t, snapshot.Persist(sink))

	var state SerializableDirectoryState
	require.NoError(t, json.Unmarshal(sink.Bytes(), &state))

	assert.Len(t, state.Stores, 4)
	assert.Len(t, state.FileIndex, 4)
	assert.NotEmpty(t, state.LogicalVolumes)
	assert.NotEmpty(t, state.WritableLVs)
	assert.NotZero(t, state.LVCounter)
}

func TestSnapshotRestoreWithFileOperations(t *testing.T) {
	cluster, _ := CreateTestClusterWithStores(t, 3, 3, 2, 4096)
	defer CleanupClusterWithStores(cluster)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	originalFiles := map[string]int64{
		"file-1": 1024,
		"file-2": 2048,
		"file-3": 512,
	}

	for fileID, size := range originalFiles {
		_, locations, err := leader.AssignWriteLocations(fileID, size)
		require.NoError(t, err)
		assert.Len(t, locations, 2)
	}

	require.True(t, cluster.WaitForConsensus(5*time.Second))

	snapshot, err := leader.fsm.Snapshot()
	require.NoError(t, err)

	sink := newMockSnapshotSink()
	require.NoError(t, snapshot.Persist(sink))

	newFSM := NewDirectoryFSM(2, 4096, &mockHttpClient{})
	reader := bytes.NewReader(sink.Bytes())
	require.NoError(t, newFSM.Restore(io.NopCloser(reader)))

	for fileID := range originalFiles {
		readLocations, err := newFSM.directory.GetReadLocations(fileID)
		require.NoError(t, err, "Failed to get read locations for %s after restore", fileID)
		assert.Len(t, readLocations, 2)
	}
}
