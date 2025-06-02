package directory

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/nireo/haystack/store"
)

const (
	defaultReplicationFactor       = 3
	defaultMaxLVSize         int64 = 1 << 30
)

// StoreInfo contains information about a given store server
type StoreInfo struct {
	ID      string
	Address string
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

// HttpClient provides a common interface to execute HTTP requests. This mainly exists such
// that we can assert that certain requests have been made in tests.
type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Directory handles choosing replication locations for files
type Directory struct {
	mu             sync.RWMutex
	client         HttpClient
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
func NewDirectory(replicationFactor int, maxLVSize int64, client HttpClient) *Directory {
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
		client:            client,
	}
}

// RegisterStore registers a new store with the directory
func (d *Directory) RegisterStore(storeID, address string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if storeID == "" || address == "" {
		return errors.New("store id and address cannot be empty")
	}

	d.stores[storeID] = &StoreInfo{
		ID:      storeID,
		Address: address,
	}

	log.Printf("store %s registered with address %s", storeID, address)
	return nil
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

	selectedStores := make(map[string]string)
	for storeID, info := range d.stores {
		selectedStores[storeID] = info.Address
		if len(selectedStores) >= d.replicationFactor {
			break
		}
	}

	lvidOnNew := d.getNextLVIDs()
	placements := make([]Placement, 0, d.replicationFactor)
	successfulPlacements := 0

	for storeID, addr := range selectedStores {
		err := d.sendCreateLV(addr, lvidOnNew)
		if err != nil {
			log.Printf("skipping store %s due to error: %s", storeID, err)
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

func (d *Directory) Close() {
	return
}

func (d *Directory) sendCreateLV(storeAddress, id string) error {
	req := store.CreateLogicalVolumeRequest{
		LogicalVolumeID: id,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(storeAddress, "http://") {
		storeAddress = "http://" + storeAddress
	}

	httpReq, err := http.NewRequest(http.MethodPost, storeAddress+"/api/v1/create_logical", bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	res, err := d.client.Do(httpReq)
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("error sending request to server %s got status: %d", storeAddress, err)
	}

	return nil
}
