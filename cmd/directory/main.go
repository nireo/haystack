package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nireo/haystack/directory"
)

func main() {
	var (
		nodeID       = flag.String("node-id", "", "Node ID")
		raftAddr     = flag.String("raft-addr", "127.0.0.1:7000", "Raft bind address")
		httpAddr     = flag.String("http-addr", "127.0.0.1:8000", "HTTP bind address")
		existingAddr = flag.String("existingAddr", "", "Leader HTTP address for discovery")
		dataDir      = flag.String("data-dir", "./data", "Data directory")
		bootstrap    = flag.Bool("bootstrap", false, "Start as bootstrap node")
		repFactor    = flag.Int("replication-factor", 3, "Replication factor")
		maxLVSize    = flag.Int64("max-lv-size", 1024*1024*1024, "Maximum logical volume size")
	)
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("node-id is required")
	}

	raftService, err := directory.NewRaftService(
		*nodeID,
		*raftAddr,
		*dataDir,
		*bootstrap,
		*repFactor,
		*maxLVSize,
	)
	if err != nil {
		log.Fatalf("Failed to create Raft service: %v", err)
	}

	httpService := directory.NewHTTPService(raftService, raftService, *httpAddr)
	go func() {
		log.Printf("Starting HTTP service on %s", *httpAddr)
		if err := httpService.Start(); err != nil {
			log.Fatalf("HTTP service failed: %v", err)
		}
	}()

	if !*bootstrap && *existingAddr != "" {
		go func() {
			time.Sleep(2 * time.Second)
			nodes, err := getClusterNodes(*existingAddr)
			if err != nil {
				log.Printf("Warning: Failed to discover cluster: %v", err)
				return
			}

			var leaderAddr string
			for _, node := range nodes {
				if node.RaftAddr != *raftAddr {
					httpService.AddNodeHTTPAddr(node.RaftAddr, node.HTTPAddr)
				}

				if node.IsLeader {
					leaderAddr = node.RaftAddr
				}
			}

			if leaderAddr == "" {
				// this shouldn't be happening but just in case
				leaderAddr = *existingAddr
			}

			if err := joinViaHTTP(leaderAddr, *nodeID, *raftAddr); err != nil {
				log.Printf("Warning: Failed to join cluster: %v", err)
			}
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal %v, shutting down...", sig)

	if err := raftService.Shutdown(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	log.Println("Shutdown complete")
}

func getClusterNodes(existingAddr string) ([]directory.ClusterNode, error) {
	if strings.HasPrefix(existingAddr, "http://") {
		existingAddr = strings.TrimPrefix(existingAddr, "http://")
	}

	url := fmt.Sprintf("http://%s/api/v1/cluster/nodes", existingAddr)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("failed to join cluster: %s", string(body))
	}

	var nodes []directory.ClusterNode
	err = json.NewDecoder(resp.Body).Decode(&nodes)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func joinViaHTTP(leaderHTTPAddr, nodeID, raftAddr string) error {
	if strings.HasPrefix(leaderHTTPAddr, "http://") {
		leaderHTTPAddr = strings.TrimPrefix(leaderHTTPAddr, "http://")
	}

	url := fmt.Sprintf("http://%s/api/v1/cluster/add-voter", leaderHTTPAddr)
	payload := map[string]string{
		"node_id": nodeID,
		"address": raftAddr,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", strings.NewReader(string(jsonData)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to join cluster: %s", string(body))
	}

	return nil
}
