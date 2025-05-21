package directory

type LogicalVolumeInfo struct {
	ID               string
	ReplicaServerIDs []string
}

type StoreInfo struct {
	ID      string
	Address string
}

type Directory struct {
	storeServers   map[string]*StoreInfo
	logicalVolumes map[string]LogicalVolumeInfo
}
