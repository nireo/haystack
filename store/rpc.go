package store

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
)

// StoreServer provides a way to interact with the store using RPC.
type StoreServer struct {
	store *Store
	port  int
}

// NewStoreServer creates a new store server
func NewStoreServer(store *Store, port int) *StoreServer {
	return &StoreServer{
		store: store,
	}
}

type CreateFileArgs struct {
	LogicalID string // which logical volume the data should be written to
	FileID    string // the identifier so the file can be accessed in the future
	Data      []byte // the actual content of the file
}

type CreateFileReply struct{}

func (s *StoreServer) CreateFile(args CreateFileArgs, _ *CreateFileReply) error {
	return s.store.CreateFile(args.FileID, args.LogicalID, args.Data)
}

type ReadFileArgs struct {
	FileID        string // the actual identifier for the file
	LogicalVolume string // the logical volume in which the file resides
}

type ReadFileReply struct {
	Data []byte // the content of the file that was read.
}

func (s *StoreServer) ReadFile(args ReadFileArgs, reply *ReadFileReply) error {
	data, err := s.store.ReadFile(args.FileID, args.LogicalVolume)
	if err != nil {
		return err
	}

	reply.Data = data
	return nil
}

func (s *StoreServer) Start() error {
	err := rpc.RegisterName("Store", s)
	if err != nil {
		return fmt.Errorf("failed to register rpc service: %s", err)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to create a listener: %s", err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("rpc listener closed")
				return nil
			}
			log.Printf("error accepting connection: %v. skipping...", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

type CreateLogicalVolumeArgs struct {
	ID string // ID of the logical volume
}

type CreateLogicalVolumeReply struct{}

func (s *StoreServer) CreateLogicalVolume(args CreateLogicalVolumeArgs, reply *CreateLogicalVolumeReply) error {
	return s.store.NewLogical(args.ID)
}

type LogicalVolumeInfo struct {
	ID   string
	Size int64
}

type GetLogicalVolumesArgs struct{}

type GetLogicalVolumesReply struct {
	Volumes []LogicalVolumeInfo
}

func (s *StoreServer) GetLogicalVolumes(args GetLogicalVolumesArgs, reply *GetLogicalVolumesReply) error {
	volumes := make([]LogicalVolumeInfo, 0, len(s.store.logicals))
	for id, file := range s.store.logicals {
		size, err := file.Size()
		if err != nil {
			return err
		}

		volumes = append(volumes, LogicalVolumeInfo{
			ID:   id,
			Size: size,
		})
	}

	return nil
}
