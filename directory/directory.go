package directory

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/google/uuid"
)

const (
	defaultReplicationFactor       = 3
	defaultMaxLVSize         int64 = 1 << 30
)

// StoreInfo contains information about a given store server. We store the address explicitly since the
// directory gives the users store server addresses where they can read the files.
type StoreInfo struct {
	ID      string
	Address string
	Client  *rpc.Client
}

// Placement relates a given logical volume to a store server.
type Placement struct {
	StoreID         string
	LogicalVolumeID string
}

// LogicalVolume contains metadata about a store server.
type LogicalVolume struct {
	ID           string
	Placements   []Placement
	IsWritable   bool
	MaxTotalSize int64
	CurrentSize  int64
	mu           sync.Mutex
}

// FileMapping relates a file to a logical volume. The file size isn't technically needed but the client
// can use that information when allocating space etc.
type FileMapping struct {
	FileID          string
	LogicalVolumeID string
	FileSize        int64
}

// Directory handles choosing replication locations for files and returning store server information about
// logical volumes where files are stored.
type Directory struct {
	mu sync.RWMutex

	stores         map[string]*StoreInfo
	logicalVolumes map[string]*LogicalVolume
	fileIndex      map[string]*FileMapping

	activeWritableLVOrder []string
	currentLVIndex        int

	replicationFactor int
	maxLVSize         int64
}

// NewDirectory creates a new directory instance and it validates the configuration values or sets them to the default values
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
		stores:                make(map[string]*StoreInfo),
		logicalVolumes:        make(map[string]*LogicalVolume),
		fileIndex:             make(map[string]*FileMapping),
		activeWritableLVOrder: make([]string, 0),
		currentLVIndex:        0,
		replicationFactor:     replicationFactor,
		maxLVSize:             maxLVSize,
	}
}

// RegisterStoreArgs registers another store server such that files can be replicated to it.
type RegisterStoreArgs struct {
	StoreID string
	Address string
}

type RegisterStoreReply struct{}

// AssignWriteLocationsArgs gets a file ID and chooses the logical volumes where the file
// should be stored.
type AssignWriteLocationsArgs struct {
	FileID   string
	FileSize int64
}

// AssignWriteLocationInfo maps a file to a certain store address and a logical volume stored in that
// store address.
type AssignWriteLocationInfo struct {
	StoreID         string
	StoreAddress    string
	LogicalVolumeID string
}

type AssignWriteLocationsReply struct {
	LogicalVolumeID string
	Locations       []AssignWriteLocationInfo
}

type GetReadLocationsArgs struct {
	FileID string
}

type GetReadLocationInfo struct {
	StoreID         string
	StoreAddress    string
	LogicalVolumeID string
}

type GetReadLocationsReply struct {
	Locations []GetReadLocationInfo
}

type CreateLogicalVolumeStoreArgs struct {
	ID string
}
type CreateLogicalVolumeStoreReply struct{}

func (d *Directory) RegisterStore(args RegisterStoreArgs, reply *RegisterStoreReply) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if args.StoreID == "" || args.Address == "" {
		return errors.New("store id andAddress address be empty")
	}

	if existingStore, exists := d.stores[args.StoreID]; exists {
		log.Printf("store %s re-registering. old address: %s, new address: %s", args.StoreID, existingStore.Address, args.Address)
		if existingStore.Client != nil {
			existingStore.Client.Close()
		}
	}

	d.stores[args.StoreID] = &StoreInfo{
		ID:      args.StoreID,
		Address: args.Address,
		Client:  nil,
	}

	log.Printf("store %s registered with address %s", args.StoreID, args.Address)
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

func (d *Directory) createNewLogicalVolume() (*LogicalVolume, error) {
	if len(d.stores) < d.replicationFactor {
		return nil, fmt.Errorf("not enough registered stores (%d) to meet replication factor (%d)", len(d.stores), d.replicationFactor)
	}

	selectedStoreIDs := make([]string, 0, d.replicationFactor)
	availableStoreInfos := make([]*StoreInfo, 0, len(d.stores))
	for _, storeInfo := range d.stores {
		availableStoreInfos = append(availableStoreInfos, storeInfo)
	}

	for i := 0; i < d.replicationFactor && i < len(availableStoreInfos); i++ {
		selectedStoreIDs = append(selectedStoreIDs, availableStoreInfos[i].ID)
	}
	if len(selectedStoreIDs) < d.replicationFactor {
		return nil, fmt.Errorf("could not select enough stores (%d) for replication factor (%d)", len(selectedStoreIDs), d.replicationFactor)
	}

	alvID := uuid.New().String()
	lvidOnNew := uuid.New().String()

	placements := make([]Placement, 0, d.replicationFactor)
	successfulPlacements := 0

	for _, storeID := range selectedStoreIDs {
		client, err := d.getOrEstablishStoreClient(storeID)
		if err != nil {
			log.Printf("Skipping store %s for ALV %s due to connection error: %v", storeID, alvID, err)
			continue
		}

		createLVArgs := CreateLogicalVolumeStoreArgs{ID: lvidOnNew}
		var createLVReply CreateLogicalVolumeStoreReply

		err = client.Call("Store.CreateLogicalVolume", createLVArgs, &createLVReply)
		if err != nil {
			log.Printf("Failed to create  logical volume %s on store %s: %v. Closing client.", lvidOnNew, storeID, err)
			d.stores[storeID].Client.Close()
			d.stores[storeID].Client = nil
			continue
		}

		placements = append(placements, Placement{
			StoreID:         storeID,
			LogicalVolumeID: lvidOnNew,
		})
		successfulPlacements++
		log.Printf("Successfully created  logical volume %s on store %s for ALV %s", lvidOnNew, storeID, alvID)
	}

	if successfulPlacements < d.replicationFactor {
		log.Printf("Failed to create ALV %s with enough replicas. Required: %d, Succeeded: %d", alvID, d.replicationFactor, successfulPlacements)
		return nil, fmt.Errorf("failed to create  logical volume %s with sufficient replicas (%d/%d)", alvID, successfulPlacements, d.replicationFactor)
	}

	alv := &LogicalVolume{
		ID:           alvID,
		Placements:   placements,
		IsWritable:   true,
		CurrentSize:  0,
		MaxTotalSize: d.maxLVSize,
	}
	d.logicalVolumes[alvID] = alv
	d.activeWritableLVOrder = append(d.activeWritableLVOrder, alvID)
	log.Printf("Successfully created LogicalVolume %s with %d placements. LV ID on stores: %s", alvID, len(placements), lvidOnNew)

	return alv, nil
}

// validateAssignWriteLocationsArgs performs basic validation on the arguments.
func (d *Directory) validateAssignWriteLocationsArgs(args AssignWriteLocationsArgs) error {
	if args.FileID == "" {
		return errors.New("FileID cannot be empty")
	}
	if args.FileSize <= 0 {
		return errors.New("FileSize must be positive")
	}
	if args.FileSize > d.maxLVSize {
		return fmt.Errorf("FileSize %d exceeds max LV size %d", args.FileSize, d.maxLVSize)
	}

	return nil
}

// tryUseExistingLV attempts to find a writable LV with enough space and allocates the file size.
// It updates the LV's current size and writable status.
// It also advances d.currentLVIndex for the next allocation attempt.
// Returns the selected LV and true if successful, otherwise nil and false.
func (d *Directory) tryUseExistingLV(fileSize int64) (*LogicalVolume, bool) {
	if len(d.activeWritableLVOrder) == 0 {
		return nil, false
	}

	numLVs := len(d.activeWritableLVOrder)
	for i := range numLVs {
		idxToCheck := (d.currentLVIndex + i) % numLVs
		lvID := d.activeWritableLVOrder[idxToCheck]
		lv, ok := d.logicalVolumes[lvID]

		if !ok {
			log.Printf("Warning: LV ID %s from activeWritableLVOrder not found in logicalVolumes map. Will be pruned.", lvID)
			continue
		}

		lv.mu.Lock()
		if lv.IsWritable && (lv.CurrentSize+fileSize <= lv.MaxTotalSize) {
			lv.CurrentSize += fileSize
			if lv.CurrentSize >= lv.MaxTotalSize {
				lv.IsWritable = false
				log.Printf("LogicalVolume %s (%s) is now full. Current: %d, Max: %d", lv.ID, lvID, lv.CurrentSize, lv.MaxTotalSize)
			}
			lv.mu.Unlock()
			d.currentLVIndex = (idxToCheck + 1) % numLVs
			return lv, true
		}
		lv.mu.Unlock()
	}
	return nil, false
}

// createNewLogicalVolumeAndAllocate creates a new LV and allocates the file size to it.
// It updates the new LV's current size and writable status.
func (d *Directory) createNewLogicalVolumeAndAllocate(fileSize int64) (*LogicalVolume, error) {
	newLV, err := d.createNewLogicalVolume() // This method already logs its progress/errors
	if err != nil {
		return nil, fmt.Errorf("could not create new logical volume: %w", err)
	}

	newLV.mu.Lock()
	defer newLV.mu.Unlock()

	if newLV.CurrentSize+fileSize > newLV.MaxTotalSize {
		return nil, fmt.Errorf("internal error: new LV %s (max: %d) cannot fit file (size: %d), though file size was validated against d.maxLVSize (%d)",
			newLV.ID, newLV.MaxTotalSize, fileSize, d.maxLVSize)
	}

	newLV.CurrentSize += fileSize
	if newLV.CurrentSize >= newLV.MaxTotalSize {
		newLV.IsWritable = false
		log.Printf("Newly created LogicalVolume %s is immediately full after file of size %d. Current: %d, Max: %d", newLV.ID, fileSize, newLV.CurrentSize, newLV.MaxTotalSize)
	}
	return newLV, nil
}

// pruneAndUpdateActiveLVList removes non-writable LVs from activeWritableLVOrder
// and updates d.currentLVIndex to maintain round-robin behavior.
func (d *Directory) pruneAndUpdateActiveLVList() {
	if len(d.activeWritableLVOrder) == 0 {
		d.currentLVIndex = 0
		return
	}

	var idAtOriginalCurrentIndex string
	if d.currentLVIndex < len(d.activeWritableLVOrder) {
		idAtOriginalCurrentIndex = d.activeWritableLVOrder[d.currentLVIndex]
	} else if len(d.activeWritableLVOrder) > 0 {
		d.currentLVIndex = 0
		idAtOriginalCurrentIndex = d.activeWritableLVOrder[0]
		log.Printf("Warning: currentLVIndex was out of bounds before pruning. Reset to 0.")
	}

	newActiveOrder := make([]string, 0, len(d.activeWritableLVOrder))
	newIndexForOriginalCurrentLV := -1

	for _, lvID := range d.activeWritableLVOrder {
		lv, ok := d.logicalVolumes[lvID]
		if !ok {
			log.Printf("Warning: Pruning missing LV ID %s from active list (data inconsistency).", lvID)
			continue
		}

		lv.mu.Lock()
		isWritable := lv.IsWritable
		lv.mu.Unlock()

		if !isWritable {
			log.Printf("Pruning non-writable LV %s from active list.", lvID)
			continue
		}

		newActiveOrder = append(newActiveOrder, lvID)
		if lvID == idAtOriginalCurrentIndex {
			newIndexForOriginalCurrentLV = len(newActiveOrder) - 1
		}
	}

	d.activeWritableLVOrder = newActiveOrder

	if len(d.activeWritableLVOrder) == 0 {
		d.currentLVIndex = 0
	} else {
		if newIndexForOriginalCurrentLV != -1 {
			d.currentLVIndex = newIndexForOriginalCurrentLV
		} else {
			d.currentLVIndex = 0
		}

		if d.currentLVIndex >= len(d.activeWritableLVOrder) {
			d.currentLVIndex = 0
		}
	}
}

// rollbackAllocation reverts the capacity change on an LV and removes the file index entry.
func (d *Directory) rollbackAllocation(lv *LogicalVolume, fileID string, fileSize int64) {
	log.Printf("Rolling back allocation for FileID %s from LV %s (size %d)", fileID, lv.ID, fileSize)

	lv.mu.Lock()
	lv.CurrentSize -= fileSize
	if !lv.IsWritable && lv.CurrentSize < lv.MaxTotalSize {
		lv.IsWritable = true
		log.Printf("LV %s (%s) is now writable again after rollback.", lv.ID, fileID)
	}
	lv.mu.Unlock()

	delete(d.fileIndex, fileID)
}

// AssignWriteLocations assigns a file to a logical volume, handling replication choices.
// It is an RPC method.
func (d *Directory) AssignWriteLocations(args AssignWriteLocationsArgs, reply *AssignWriteLocationsReply) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.validateAssignWriteLocationsArgs(args); err != nil {
		return err
	}

	var selectedLV *LogicalVolume
	var LVCreated bool = false

	lv, found := d.tryUseExistingLV(args.FileSize)
	if found {
		selectedLV = lv
	} else {
		log.Printf("No suitable existing LogicalVolume for FileID %s (size %d). Creating a new one.", args.FileID, args.FileSize)
		newLV, err := d.createNewLogicalVolumeAndAllocate(args.FileSize)
		if err != nil {
			return err
		}
		selectedLV = newLV
		LVCreated = true
	}

	d.pruneAndUpdateActiveLVList()

	d.fileIndex[args.FileID] = &FileMapping{
		FileID:          args.FileID,
		LogicalVolumeID: selectedLV.ID,
		FileSize:        args.FileSize,
	}

	reply.LogicalVolumeID = selectedLV.ID
	reply.Locations = make([]AssignWriteLocationInfo, 0, len(selectedLV.Placements))
	for _, p := range selectedLV.Placements {
		storeInfo, ok := d.stores[p.StoreID]
		if !ok {
			log.Printf("CRITICAL INCONSISTENCY: Store %s for placement of LV %s (FileID %s) not found in registry! Skipping this location.",
				p.StoreID, selectedLV.ID, args.FileID)
			continue
		}
		reply.Locations = append(reply.Locations, AssignWriteLocationInfo{
			StoreID:         p.StoreID,
			StoreAddress:    storeInfo.Address,
			LogicalVolumeID: p.LogicalVolumeID,
		})
	}

	if len(reply.Locations) < d.replicationFactor {
		log.Printf("Warning: For FileID %s, LV %s has only %d/%d available placements due to missing store registrations for its placements.",
			args.FileID, selectedLV.ID, len(reply.Locations), d.replicationFactor)

		if len(reply.Locations) == 0 {
			d.rollbackAllocation(selectedLV, args.FileID, args.FileSize)
			return fmt.Errorf("no valid store locations could be determined for LV %s (FileID %s); allocation rolled back. Required %d, found 0",
				selectedLV.ID, args.FileID, d.replicationFactor)
		}
	}

	log.Printf("Successfully assigned FileID %s (size %d) to LV %s. Locations: %d. LV CurrentSize after alloc: %d. New LV created: %t.",
		args.FileID, args.FileSize, selectedLV.ID, len(reply.Locations), selectedLV.CurrentSize, LVCreated)
	return nil
}

// GetReadLocations takes in a file id and returns the logical volumes in which the file is stored.
func (d *Directory) GetReadLocations(args GetReadLocationsArgs, reply *GetReadLocationsReply) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if args.FileID == "" {
		return errors.New("FileID cannot be empty")
	}

	// each file is stored in a single logical volume but that logical volume itself exists in multiple different
	// stores.
	fileMapping, ok := d.fileIndex[args.FileID]
	if !ok {
		return fmt.Errorf("file ID %s not found in directory index", args.FileID)
	}

	alv, ok := d.logicalVolumes[fileMapping.LogicalVolumeID]
	if !ok {
		log.Printf("CRITICAL INCONSISTENCY: no logical volume for file %s | lv id: %s", args.FileID, fileMapping.LogicalVolumeID)
		return fmt.Errorf("internal error:  logical volume %s for file %s not found", fileMapping.LogicalVolumeID, args.FileID)
	}

	reply.Locations = make([]GetReadLocationInfo, 0, len(alv.Placements))
	for _, p := range alv.Placements {
		storeInfo, storeExists := d.stores[p.StoreID]
		if !storeExists {
			log.Printf("WARNING: store %s for ALV %s (file %s) placement not found in registry. Skipping this location for read.", p.StoreID, alv.ID, args.FileID)
			continue
		}

		reply.Locations = append(reply.Locations, GetReadLocationInfo{
			StoreID:         p.StoreID,
			StoreAddress:    storeInfo.Address,
			LogicalVolumeID: p.LogicalVolumeID,
		})
	}

	if len(reply.Locations) == 0 {
		return fmt.Errorf("no available read locations for file %s", args.FileID)
	}

	log.Printf("providing %d read locations for FileID %s from LV %s.", len(reply.Locations), args.FileID, alv.ID)
	return nil
}

// StartDirectoryServer starts up a rpc service and starts handling connections.
func StartDirectoryServer(dir *Directory, port int) error {
	rpc.RegisterName("Directory", dir)

	address := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", address, err)
	}
	defer listener.Close()
	log.Printf("Directory RPC server listening on %s", address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("Directory RPC listener closed.")
				return nil
			}
			log.Printf("Directory RPC server accept error: %v. Continuing...", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

// Close cleans up connections and any other resources that should be explicitly closed.
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
