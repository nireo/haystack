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
	defaultReplicationFactor = 3
	defaultMaxLVSize         = 1 << 30
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
		log.Printf("Warning: Invalid replicationFactor %d, using default %d", replicationFactor, defaultReplicationFactor)
		replicationFactor = defaultReplicationFactor
	}
	if maxLVSize <= 0 {
		log.Printf("Warning: Invalid maxLVSize %d, using default %d", maxLVSize, defaultMaxLVSize)
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
	Error           string
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

// AssignWriteLocations takes in a file's metadata and assigns it to some logical volumes. It handles choosing the replication
// it informs the caller about the logical volumes and how to call the stores. An important detail of this design is that
// the directory doesn't interact with the stores themselves when it comes to writing. Only when creating more logical
// volumes and getting information about the existing ones.
func (d *Directory) AssignWriteLocations(args AssignWriteLocationsArgs, reply *AssignWriteLocationsReply) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if args.FileID == "" {
		reply.Error = "FileID cannot be empty"
		return errors.New(reply.Error)
	}
	if args.FileSize <= 0 {
		reply.Error = "FileSize must be positive"
		return errors.New(reply.Error)
	}
	if args.FileSize > d.maxLVSize {
		reply.Error = fmt.Sprintf("FileSize %d exceeds max  LV size %d", args.FileSize, d.maxLVSize)
		return errors.New(reply.Error)
	}

	var selectedALV *LogicalVolume

	if len(d.activeWritableLVOrder) > 0 {
		initialIndex := d.currentLVIndex
		for range d.activeWritableLVOrder {
			lvID := d.activeWritableLVOrder[d.currentLVIndex]
			alv, ok := d.logicalVolumes[lvID]

			if ok {
				alv.mu.Lock()
				if alv.IsWritable && (alv.CurrentSize+args.FileSize <= alv.MaxTotalSize) {
					selectedALV = alv
					alv.CurrentSize += args.FileSize
					if alv.CurrentSize >= alv.MaxTotalSize {
						alv.IsWritable = false
						log.Printf("LogicalVolume %s (%s) is now full. Current: %d, Max: %d", alv.ID, lvID, alv.CurrentSize, alv.MaxTotalSize)
					}
					alv.mu.Unlock()
					d.currentLVIndex = (d.currentLVIndex + 1) % len(d.activeWritableLVOrder)
					break
				}
				alv.mu.Unlock()
			}
			d.currentLVIndex = (d.currentLVIndex + 1) % len(d.activeWritableLVOrder)
			if d.currentLVIndex == initialIndex && selectedALV == nil {
				break
			}
		}
	}

	newActiveOrder := make([]string, 0, len(d.activeWritableLVOrder))
	updatedCurrentIndex := -1
	currentIndexPassed := false

	for _, lvID := range d.activeWritableLVOrder {
		alv, ok := d.logicalVolumes[lvID]
		isStillWritable := false
		if ok {
			alv.mu.Lock()
			isStillWritable = alv.IsWritable
			alv.mu.Unlock()
		}
		if isStillWritable {
			newActiveOrder = append(newActiveOrder, lvID)
			if lvID == d.activeWritableLVOrder[d.currentLVIndex] && !currentIndexPassed {
				updatedCurrentIndex = len(newActiveOrder) - 1
				currentIndexPassed = true
			}
		} else {
			log.Printf("Removing ALV %s from active writable list.", lvID)
		}
	}
	d.activeWritableLVOrder = newActiveOrder
	if len(d.activeWritableLVOrder) > 0 {
		if updatedCurrentIndex != -1 {
			d.currentLVIndex = updatedCurrentIndex
		} else {
			d.currentLVIndex = 0
		}
		if d.currentLVIndex >= len(d.activeWritableLVOrder) {
			d.currentLVIndex = 0
		}
	} else {
		d.currentLVIndex = 0
	}

	if selectedALV == nil {
		log.Println("No suitable existing LogicalVolume found or all are full, creating a new one.")
		alv, err := d.createNewLogicalVolume()
		if err != nil {
			reply.Error = fmt.Sprintf("failed to create new  logical volume: %v", err)
			return errors.New(reply.Error)
		}
		selectedALV = alv
		selectedALV.mu.Lock()
		selectedALV.CurrentSize += args.FileSize
		if selectedALV.CurrentSize >= selectedALV.MaxTotalSize {
			selectedALV.IsWritable = false
			log.Printf("Newly created LogicalVolume %s is immediately full after file of size %d. Current: %d, Max: %d", selectedALV.ID, args.FileSize, selectedALV.CurrentSize, selectedALV.MaxTotalSize)
		}
		selectedALV.mu.Unlock()
	}

	d.fileIndex[args.FileID] = &FileMapping{
		FileID:          args.FileID,
		LogicalVolumeID: selectedALV.ID,
		FileSize:        args.FileSize,
	}

	reply.LogicalVolumeID = selectedALV.ID
	reply.Locations = make([]AssignWriteLocationInfo, 0, len(selectedALV.Placements))
	for _, p := range selectedALV.Placements {
		storeInfo, ok := d.stores[p.StoreID]
		if !ok {
			log.Printf("CRITICAL INCONSISTENCY: Store %s for placement of ALV %s not found in registry!", p.StoreID, selectedALV.ID)
			continue
		}
		reply.Locations = append(reply.Locations, AssignWriteLocationInfo{
			StoreID:         p.StoreID,
			StoreAddress:    storeInfo.Address,
			LogicalVolumeID: p.LogicalVolumeID,
		})
	}

	if len(reply.Locations) < d.replicationFactor {
		log.Printf("Warning: For FileID %s, ALV %s has only %d/%d available placements.", args.FileID, selectedALV.ID, len(reply.Locations), d.replicationFactor)
		if len(reply.Locations) == 0 {
			selectedALV.mu.Lock()
			selectedALV.CurrentSize -= args.FileSize
			if !selectedALV.IsWritable && selectedALV.CurrentSize < selectedALV.MaxTotalSize {
				selectedALV.IsWritable = true
			}
			selectedALV.mu.Unlock()
			delete(d.fileIndex, args.FileID)
			reply.Error = fmt.Sprintf("no valid store locations could be determined for ALV %s for file %s", selectedALV.ID, args.FileID)
			return errors.New(reply.Error)
		}
	}

	log.Printf("Assigned FileID %s (size %d) to LV %s. Locations: %d. ALV CurrentSize: %d", args.FileID, args.FileSize, selectedALV.ID, len(reply.Locations), selectedALV.CurrentSize)
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
