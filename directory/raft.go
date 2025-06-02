package directory

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

var ErrNotLeader = errors.New("node is not leader")

const (
	defaultTimeout = 10 * time.Second
)

type CmdType uint8

const (
	CmdRegisterStore CmdType = iota
	CmdAssignWriteLocations
)

type ClusterNode struct {
	RaftAddr string `json:"raft_addr,omitempty"`
	HTTPAddr string `json:"http_addr,omitempty"`
	IsLeader bool   `json:"is_leader,omitempty"`
}

type Cmd struct {
	Type CmdType `json:"type"`
	Data any     `json:"data"`
}

type RegisterStoreData struct {
	StoreID string `json:"store_id"`
	Address string `json:"address"`
}

type AssignWriteLocationData struct {
	FileID   string `json:"file_id"`
	FileSize int64  `json:"file_size"`
}

type DirectoryFSM struct {
	directory *Directory
}

func NewDirectoryFSM(replicationFactor int, maxLVSize int64, client HttpClient) *DirectoryFSM {
	return &DirectoryFSM{
		directory: NewDirectory(replicationFactor, maxLVSize, client),
	}
}

func (fsm *DirectoryFSM) Apply(entry *raft.Log) any {
	var cmd Cmd
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		log.Printf("error unmarshaling command: %v", err)
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Type {
	case CmdRegisterStore:
		return fsm.applyRegisterStore(cmd.Data)
	case CmdAssignWriteLocations:
		return fsm.applyAssignWriteLocation(cmd.Data)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (fsm *DirectoryFSM) applyRegisterStore(data any) any {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal register store data: %w", err)
	}

	var regData RegisterStoreData
	if err := json.Unmarshal(dataBytes, &regData); err != nil {
		return fmt.Errorf("failed to unmarshal register store data: %w", err)
	}

	err = fsm.directory.RegisterStore(regData.StoreID, regData.Address)
	if err != nil {
		return fmt.Errorf("failed to register store: %w", err)
	}

	return fmt.Sprintf("Store %s registered successfully", regData.StoreID)
}

func (fsm *DirectoryFSM) applyAssignWriteLocation(data any) any {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal assign write location data: %w", err)
	}

	var assignData AssignWriteLocationData
	if err := json.Unmarshal(dataBytes, &assignData); err != nil {
		return fmt.Errorf("failed to unmarshal assign write location data: %w", err)
	}

	lvID, locations, err := fsm.directory.AssignWriteLocations(assignData.FileID, assignData.FileSize)
	if err != nil {
		return fmt.Errorf("failed to assign write locations: %w", err)
	}

	return map[string]any{
		"logical_volume_id": lvID,
		"locations":         locations,
	}
}

type SerializableStoreInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type SerializableLogicalVolume struct {
	ID           string      `json:"id"`
	Placements   []Placement `json:"placements"`
	IsWritable   bool        `json:"is_writable"`
	MaxTotalSize int64       `json:"max_total_size"`
	CurrentSize  int64       `json:"current_size"`
}

type SerializableDirectoryState struct {
	Stores            map[string]*SerializableStoreInfo     `json:"stores"`
	LogicalVolumes    map[string]*SerializableLogicalVolume `json:"logical_volumes"`
	FileIndex         map[string]*FileMapping               `json:"file_index"`
	WritableLVs       []string                              `json:"writable_lv_ids"`
	ReplicationFactor int                                   `json:"replication_factor"`
	MaxLVSize         int64                                 `json:"max_lv_size"`
	LVCounter         uint64                                `json:"lv_counter"`
}

type DirectorySnapshot struct {
	state *SerializableDirectoryState
}

func (fsm *DirectoryFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.directory.mu.RLock()
	defer fsm.directory.mu.RUnlock()

	serializableStores := make(map[string]*SerializableStoreInfo, len(fsm.directory.stores))
	for id, store := range fsm.directory.stores {
		serializableStores[id] = &SerializableStoreInfo{
			ID:      store.ID,
			Address: store.Address,
		}
	}

	serializableLVs := make(map[string]*SerializableLogicalVolume, len(fsm.directory.logicalVolumes))
	for id, lv := range fsm.directory.logicalVolumes {
		lv.mu.Lock()
		serializableLVs[id] = &SerializableLogicalVolume{
			ID:           lv.ID,
			Placements:   lv.Placements,
			IsWritable:   lv.IsWritable,
			MaxTotalSize: lv.MaxTotalSize,
			CurrentSize:  lv.CurrentSize,
		}
		lv.mu.Unlock()
	}

	writableLVIDs := make([]string, 0, len(fsm.directory.writableLVs))
	for id := range fsm.directory.writableLVs {
		writableLVIDs = append(writableLVIDs, id)
	}

	state := &SerializableDirectoryState{
		Stores:            serializableStores,
		LogicalVolumes:    serializableLVs,
		FileIndex:         fsm.directory.fileIndex,
		WritableLVs:       writableLVIDs,
		ReplicationFactor: fsm.directory.replicationFactor,
		MaxLVSize:         fsm.directory.maxLVSize,
		LVCounter:         fsm.directory.lvCounter,
	}

	log.Printf("Creating snapshot with %d stores, %d logical volumes, %d files",
		len(state.Stores), len(state.LogicalVolumes), len(state.FileIndex))

	return &DirectorySnapshot{state: state}, nil
}

func (fsm *DirectoryFSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	var state SerializableDirectoryState
	decoder := json.NewDecoder(snapshot)
	if err := decoder.Decode(&state); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	log.Printf("Restoring snapshot with %d stores, %d logical volumes, %d files",
		len(state.Stores), len(state.LogicalVolumes), len(state.FileIndex))

	newDirectory := &Directory{
		stores:            make(map[string]*StoreInfo),
		logicalVolumes:    make(map[string]*LogicalVolume),
		fileIndex:         make(map[string]*FileMapping),
		writableLVs:       make(map[string]*LogicalVolume),
		replicationFactor: state.ReplicationFactor,
		maxLVSize:         state.MaxLVSize,
		lvCounter:         state.LVCounter,
	}

	for id, serStore := range state.Stores {
		newDirectory.stores[id] = &StoreInfo{
			ID:      serStore.ID,
			Address: serStore.Address,
		}
	}

	for id, serLV := range state.LogicalVolumes {
		lv := &LogicalVolume{
			ID:           serLV.ID,
			Placements:   serLV.Placements,
			IsWritable:   serLV.IsWritable,
			MaxTotalSize: serLV.MaxTotalSize,
			CurrentSize:  serLV.CurrentSize,
		}
		newDirectory.logicalVolumes[id] = lv
	}

	for _, lvID := range state.WritableLVs {
		if lv, exists := newDirectory.logicalVolumes[lvID]; exists {
			newDirectory.writableLVs[lvID] = lv
		}
	}

	maps.Copy(newDirectory.fileIndex, state.FileIndex)
	fsm.directory.Close()
	fsm.directory = newDirectory

	log.Printf("successfully restored directory state from snapshot")
	return nil
}

func (s *DirectorySnapshot) Persist(sink raft.SnapshotSink) error {
	encoder := json.NewEncoder(sink)
	if err := encoder.Encode(s.state); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}

	log.Printf("successfully persisted snapshot with %d stores, %d logical volumes", len(s.state.Stores), len(s.state.LogicalVolumes))
	return sink.Close()
}

func (s *DirectorySnapshot) Release() {
	s.state = nil
}

type RaftService struct {
	raft      *raft.Raft
	fsm       *DirectoryFSM
	transport raft.Transport
}

func NewRaftService(nodeID, bindAddr, dataDir string, bootstrap bool, replicationFactor int, maxLVSize int64, client HttpClient) (*RaftService, error) {
	fsm := &DirectoryFSM{NewDirectory(replicationFactor, maxLVSize, client)}
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve tcp address: %w", err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft instance: %w", err)
	}

	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}

		r.BootstrapCluster(configuration)
	}

	return &RaftService{
		raft:      r,
		fsm:       fsm,
		transport: transport,
	}, nil
}

func (rs *RaftService) RegisterStore(storeID, address string) error {
	if rs.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	cmd := Cmd{
		Type: CmdRegisterStore,
		Data: RegisterStoreData{
			StoreID: storeID,
			Address: address,
		},
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	future := rs.raft.Apply(cmdBytes, defaultTimeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}

	result := future.Response()
	if err, ok := result.(error); ok {
		return err
	}

	log.Printf("registered store: %s %s", storeID, address)
	return nil
}

func (rs *RaftService) AssignWriteLocations(fileID string, fileSize int64) (string, []WriteLocationInfo, error) {
	if rs.raft.State() != raft.Leader {
		return "", nil, fmt.Errorf("not the leader, cannot assign write locations")
	}

	cmd := Cmd{
		Type: CmdAssignWriteLocations,
		Data: AssignWriteLocationData{
			FileID:   fileID,
			FileSize: fileSize,
		},
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal command: %w", err)
	}

	future := rs.raft.Apply(cmdBytes, defaultTimeout)
	if err := future.Error(); err != nil {
		return "", nil, fmt.Errorf("failed to apply command: %w", err)
	}

	result := future.Response()
	if err, ok := result.(error); ok {
		return "", nil, err
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		return "", nil, fmt.Errorf("unexpected result type: %T", result)
	}

	lvID, ok := resultMap["logical_volume_id"].(string)
	if !ok {
		return "", nil, fmt.Errorf("failed to parse logical volume ID from result")
	}

	locationsInterface, ok := resultMap["locations"]
	if !ok {
		return "", nil, fmt.Errorf("failed to parse locations from result")
	}

	locationsBytes, err := json.Marshal(locationsInterface)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal locations: %w", err)
	}

	var locations []WriteLocationInfo
	if err := json.Unmarshal(locationsBytes, &locations); err != nil {
		return "", nil, fmt.Errorf("failed to unmarshal locations: %w", err)
	}

	return lvID, locations, nil
}

func (rs *RaftService) GetReadLocations(fileID string) ([]ReadLocationInfo, error) {
	return rs.fsm.directory.GetReadLocations(fileID)
}

func (rs *RaftService) Join(id, addr string) error {
	if !rs.IsLeader() {
		return ErrNotLeader
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	future := rs.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	for _, srv := range future.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}

			removeFuture := rs.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := rs.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		if err == raft.ErrNotLeader {
			return ErrNotLeader
		}

		return err
	}

	return nil
}

func (rs *RaftService) AddVoter(nodeID, address string) error {
	if !rs.IsLeader() {
		return ErrNotLeader
	}

	serverID := raft.ServerID(nodeID)
	serverAddr := raft.ServerAddress(address)

	future := rs.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	for _, srv := range future.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}

			removeFuture := rs.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := rs.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		if err == raft.ErrNotLeader {
			return ErrNotLeader
		}

		return err
	}

	return nil
}

func (rs *RaftService) RemoveServer(nodeID string) error {
	if rs.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader, cannot remove server")
	}

	future := rs.raft.RemoveServer(raft.ServerID(nodeID), 0, 10*time.Second)
	return future.Error()
}

func (rs *RaftService) IsLeader() bool {
	return rs.raft.State() == raft.Leader
}

func (rs *RaftService) LeaderAddr() string {
	id := rs.raft.Leader()
	return string(id)
}

func (rs *RaftService) Stats() map[string]string {
	return rs.raft.Stats()
}

func (rs *RaftService) Shutdown() error {
	log.Println("Shutting down Raft directory service...")

	rs.fsm.directory.Close()

	future := rs.raft.Shutdown()
	if err := future.Error(); err != nil {
		log.Printf("Error shutting down Raft: %v", err)
		return err
	}

	log.Println("Raft directory service shutdown complete")
	return nil
}

func (rs *RaftService) GetCluster() ([]ClusterNode, error) {
	if !rs.IsLeader() {
		return nil, ErrNotLeader
	}

	fut := rs.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return nil, err
	}

	clusterNodes := make([]ClusterNode, 0, len(fut.Configuration().Servers))
	for _, node := range fut.Configuration().Servers {
		clusterNodes = append(clusterNodes, ClusterNode{
			RaftAddr: string(node.Address),
			IsLeader: node.Address == rs.raft.Leader(),
		})
	}

	return clusterNodes, nil
}
