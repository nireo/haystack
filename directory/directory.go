package directory

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"sync"
)

const (
	defaultReplicationFactor       = 3
	defaultMaxLVSize         int64 = 1 << 30
)

// StoreInfo contains information about a given store server
type StoreInfo struct {
	ID      string
	Address string
	Client  *rpc.Client
}

// Placement relates a given logical volume to a store server
type Placement struct {
	StoreID         string
	LogicalVolumeID string
}

// LogicalVolume contains metadata about a store server
type LogicalVolume struct {
	ID           string
	Placements   []Placement
	IsWritable   bool
	MaxTotalSize int64
	CurrentSize  int64
	mu           sync.Mutex
}

// FileMapping relates a file to a logical volume
type FileMapping struct {
	FileID          string
	LogicalVolumeID string
	FileSize        int64
}

// WriteLocationInfo contains information about where a file should be written
type WriteLocationInfo struct {
	StoreID         string `json:"store_id"`
	StoreAddress    string `json:"store_address"`
	LogicalVolumeID string `json:"logical_volume_id"`
}

// ReadLocationInfo contains information about where a file can be read from
type ReadLocationInfo struct {
	StoreID         string
	StoreAddress    string
	LogicalVolumeID string
}

// Directory handles choosing replication locations for files
type Directory struct {
	mu             sync.RWMutex
	stores         map[string]*StoreInfo
	logicalVolumes map[string]*LogicalVolume
	fileIndex      map[string]*FileMapping

	// Simplified: just track writable LVs without complex ordering
	writableLVs       map[string]*LogicalVolume
	replicationFactor int
	maxLVSize         int64

	// we need a counter since the FSM needs to be deterministic i.e. when the construct the
	// state from the log, it should be the same. If we generate uuids that is not the case.
	// as there would be new logical volumes with different ids.
	lvCounter uint64
}

func (d *Directory) getNextLVIDs() string {
	d.lvCounter++
	lvidOnNew := fmt.Sprintf("lvid-%d", d.lvCounter)
	return lvidOnNew
}

// NewDirectory creates a new directory instance
func NewDirectory(replicationFactor int, maxLVSize int64) *Directory {
	if replicationFactor <= 0 {
		log.Printf("WARNING: Invalid replicationFactor %d, using default %d", replicationFactor, defaultReplicationFactor)
		replicationFactor = defaultReplicationFactor
	}
	if maxLVSize <= 0 {
		log.Printf("WARNING: Invalid maxLVSize %d, using default %d", maxLVSize, defaultMaxLVSize)
		maxLVSize = defaultMaxLVSize
	}
	return &Directory{
		stores:            make(map[string]*StoreInfo),
		logicalVolumes:    make(map[string]*LogicalVolume),
		fileIndex:         make(map[string]*FileMapping),
		writableLVs:       make(map[string]*LogicalVolume),
		replicationFactor: replicationFactor,
		maxLVSize:         maxLVSize,
		lvCounter:         0,
	}
}

// RegisterStore registers a new store with the directory
func (d *Directory) RegisterStore(storeID, address string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if storeID == "" || address == "" {
		return errors.New("store id and address cannot be empty")
	}

	if existingStore, exists := d.stores[storeID]; exists {
		log.Printf("store %s re-registering. old address: %s, new address: %s", storeID, existingStore.Address, address)
		if existingStore.Client != nil {
			existingStore.Client.Close()
		}
	}

	d.stores[storeID] = &StoreInfo{
		ID:      storeID,
		Address: address,
		Client:  nil,
	}

	log.Printf("store %s registered with address %s", storeID, address)
	return nil
}

func (d *Directory) getOrEstablishStoreClient(storeID string) (*rpc.Client, error) {
	storeInfo, ok := d.stores[storeID]
	if !ok {
		return nil, fmt.Errorf("store %s not found", storeID)
	}

	if storeInfo.Client != nil {
		return storeInfo.Client, nil
	}

	client, err := rpc.Dial("tcp", storeInfo.Address)
	if err != nil {
		log.Printf("failed to connect to store %s at %s: %v", storeID, storeInfo.Address, err)
		return nil, err
	}

	storeInfo.Client = client
	return client, nil
}

// Simplified LV selection: find any writable LV with enough space
func (d *Directory) findWritableLV(fileSize int64) *LogicalVolume {
	for _, lv := range d.writableLVs {
		lv.mu.Lock()
		canFit := lv.IsWritable && (lv.CurrentSize+fileSize <= lv.MaxTotalSize)
		lv.mu.Unlock()

		if canFit {
			return lv
		}
	}
	return nil
}

// Allocate space in an LV and update its writability
func (d *Directory) allocateSpace(lv *LogicalVolume, fileSize int64) {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	lv.CurrentSize += fileSize

	// If LV becomes full, remove from writable set
	if lv.CurrentSize >= lv.MaxTotalSize {
		lv.IsWritable = false
		delete(d.writableLVs, lv.ID)
		log.Printf("LogicalVolume %s is now full. Current: %d, Max: %d", lv.ID, lv.CurrentSize, lv.MaxTotalSize)
	}
}

// CreateLogicalVolumeArgs represents the arguments for creating a logical volume on a store
type CreateLogicalVolumeArgs struct {
	ID string
}

// CreateLogicalVolumeReply represents the reply for creating a logical volume on a store
type CreateLogicalVolumeReply struct{}

func (d *Directory) createNewLogicalVolume() (*LogicalVolume, error) {
	if len(d.stores) < d.replicationFactor {
		return nil, fmt.Errorf("not enough registered stores (%d) to meet replication factor (%d)", len(d.stores), d.replicationFactor)
	}

	// Simple store selection - just take first N available stores
	selectedStoreIDs := make([]string, 0, d.replicationFactor)
	for storeID := range d.stores {
		selectedStoreIDs = append(selectedStoreIDs, storeID)
		if len(selectedStoreIDs) >= d.replicationFactor {
			break
		}
	}

	lvidOnNew := d.getNextLVIDs()
	placements := make([]Placement, 0, d.replicationFactor)
	successfulPlacements := 0

	for _, storeID := range selectedStoreIDs {
		client, err := d.getOrEstablishStoreClient(storeID)
		if err != nil {
			log.Printf("Skipping store %s for due to connection error: %v", storeID, err)
			continue
		}

		createLVArgs := CreateLogicalVolumeArgs{ID: lvidOnNew}
		var createLVReply CreateLogicalVolumeReply
		err = client.Call("Store.CreateLogicalVolume", createLVArgs, &createLVReply)
		if err != nil {
			log.Printf("Failed to create logical volume %s on store %s: %v. Closing client.", lvidOnNew, storeID, err)
			d.stores[storeID].Client.Close()
			d.stores[storeID].Client = nil
			continue
		}

		placements = append(placements, Placement{
			StoreID:         storeID,
			LogicalVolumeID: lvidOnNew,
		})
		successfulPlacements++
		log.Printf("Successfully created logical volume %s on store %s", lvidOnNew, storeID)
	}

	if successfulPlacements < d.replicationFactor {
		log.Printf("Failed to create ALV %s with enough replicas. Required: %d, Succeeded: %d", lvidOnNew, d.replicationFactor, successfulPlacements)
		return nil, fmt.Errorf("failed to create logical volume %s with sufficient replicas (%d/%d)", lvidOnNew, successfulPlacements, d.replicationFactor)
	}

	alv := &LogicalVolume{
		ID:           lvidOnNew,
		Placements:   placements,
		IsWritable:   true,
		CurrentSize:  0,
		MaxTotalSize: d.maxLVSize,
	}

	d.logicalVolumes[lvidOnNew] = alv
	d.writableLVs[lvidOnNew] = alv

	log.Printf("Successfully created LogicalVolume %s with %d placements", lvidOnNew, len(placements))
	return alv, nil
}

func (d *Directory) validateAssignWriteLocationsArgs(fileID string, fileSize int64) error {
	if fileID == "" {
		return errors.New("FileID cannot be empty")
	}
	if fileSize <= 0 {
		return errors.New("FileSize must be positive")
	}
	if fileSize > d.maxLVSize {
		return fmt.Errorf("FileSize %d exceeds max LV size %d", fileSize, d.maxLVSize)
	}
	return nil
}

// AssignWriteLocations assigns write locations for a file
func (d *Directory) AssignWriteLocations(fileID string, fileSize int64) (string, []WriteLocationInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.validateAssignWriteLocationsArgs(fileID, fileSize); err != nil {
		return "", nil, err
	}

	if _, exists := d.fileIndex[fileID]; exists {
		return "", nil, fmt.Errorf("file %s already exists", fileID)
	}

	var selectedLV *LogicalVolume
	selectedLV = d.findWritableLV(fileSize)

	if selectedLV == nil {
		log.Printf("No suitable existing LogicalVolume for FileID %s (size %d). Creating a new one.", fileID, fileSize)
		newLV, err := d.createNewLogicalVolume()
		if err != nil {
			return "", nil, fmt.Errorf("could not create new logical volume: %w", err)
		}
		selectedLV = newLV
	}

	d.allocateSpace(selectedLV, fileSize)
	d.fileIndex[fileID] = &FileMapping{
		FileID:          fileID,
		LogicalVolumeID: selectedLV.ID,
		FileSize:        fileSize,
	}

	locations := make([]WriteLocationInfo, 0, len(selectedLV.Placements))
	for _, p := range selectedLV.Placements {
		storeInfo, ok := d.stores[p.StoreID]
		if !ok {
			log.Printf("WARNING: Store %s for placement of LV %s (FileID %s) not found in registry", p.StoreID, selectedLV.ID, fileID)
			continue
		}

		locations = append(locations, WriteLocationInfo{
			StoreID:         p.StoreID,
			StoreAddress:    storeInfo.Address,
			LogicalVolumeID: p.LogicalVolumeID,
		})
	}

	if len(locations) == 0 {
		delete(d.fileIndex, fileID)
		selectedLV.mu.Lock()
		selectedLV.CurrentSize -= fileSize
		if !selectedLV.IsWritable && selectedLV.CurrentSize < selectedLV.MaxTotalSize {
			selectedLV.IsWritable = true
			d.writableLVs[selectedLV.ID] = selectedLV
		}
		selectedLV.mu.Unlock()
		return "", nil, fmt.Errorf("no valid store locations available for file %s", fileID)
	}

	log.Printf("Successfully assigned FileID %s (size %d) to LV %s with %d locations", fileID, fileSize, selectedLV.ID, len(locations))
	return selectedLV.ID, locations, nil
}

// GetReadLocations gets read locations for a file
func (d *Directory) GetReadLocations(fileID string) ([]ReadLocationInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if fileID == "" {
		return nil, errors.New("FileID cannot be empty")
	}

	fileMapping, ok := d.fileIndex[fileID]
	if !ok {
		return nil, fmt.Errorf("file ID %s not found", fileID)
	}

	lv, ok := d.logicalVolumes[fileMapping.LogicalVolumeID]
	if !ok {
		return nil, fmt.Errorf("logical volume %s for file %s not found", fileMapping.LogicalVolumeID, fileID)
	}

	locations := make([]ReadLocationInfo, 0, len(lv.Placements))

	for _, p := range lv.Placements {
		storeInfo, ok := d.stores[p.StoreID]
		if !ok {
			log.Printf("WARNING: store %s for LV %s (file %s) not found in registry", p.StoreID, lv.ID, fileID)
			continue
		}

		locations = append(locations, ReadLocationInfo{
			StoreID:         p.StoreID,
			StoreAddress:    storeInfo.Address,
			LogicalVolumeID: p.LogicalVolumeID,
		})
	}

	if len(locations) == 0 {
		return nil, fmt.Errorf("no available read locations for file %s", fileID)
	}

	return locations, nil
}

// Close closes all RPC client connections
func (d *Directory) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	log.Println("Closing RPC client connections to all registered stores...")
	for storeID, storeInfo := range d.stores {
		if storeInfo.Client != nil {
			err := storeInfo.Client.Close()
			if err != nil {
				log.Printf("Error closing RPC client for store %s (%s): %v", storeID, storeInfo.Address, err)
			} else {
				log.Printf("Closed RPC client for store %s (%s)", storeID, storeInfo.Address)
			}
			storeInfo.Client = nil
		}
	}
	log.Println("Finished closing store clients.")
}
