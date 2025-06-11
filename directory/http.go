package directory

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/nireo/haystack/logging"
)

// DirectoryService defines what the HTPT service needs to manage the directory state.
type DirectoryService interface {
	RegisterStore(storeID, address string) error
	AssignWriteLocations(fileID string, fileSize int64) (string, []WriteLocationInfo, error)
	GetReadLocations(fileID string) ([]ReadLocationInfo, error)
}

// ClusterService defines what the HTTP service needs to manage the cluster.
type ClusterService interface {
	AddVoter(nodeID, address string) error
	RemoveServer(nodeID string) error
	IsLeader() bool
	LeaderAddr() string
	Stats() map[string]string
	Shutdown() error
	GetCluster() ([]ClusterNode, error)
}

// HTTPService implements a HTTP interface to interact with the raft cluster. It takes care of redirecting
// write requests automatically to the leader node. So the experience for the client is seamless.
type HTTPService struct {
	dir           DirectoryService
	cluster       ClusterService
	httpAddr      string
	nodeHTTPAddrs map[string]string
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func NewHTTPService(clusterService ClusterService, dirService DirectoryService, httpAddr string) *HTTPService {
	return &HTTPService{
		cluster:       clusterService,
		dir:           dirService,
		httpAddr:      httpAddr,
		nodeHTTPAddrs: make(map[string]string),
	}
}

func (h *HTTPService) AddNodeHTTPAddr(raftAddr, httpAddr string) {
	h.nodeHTTPAddrs[raftAddr] = httpAddr
}

func (h *HTTPService) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (h *HTTPService) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/stores", h.handleRegisterStore)
	mux.HandleFunc("POST /api/v1/files/write-locations", h.handleAssignWriteLocations)
	mux.HandleFunc("GET /api/v1/files/{fileID}/read-locations", h.handleGetReadLocations)
	mux.HandleFunc("POST /api/v1/cluster/add-voter", h.handleAddVoter)
	mux.HandleFunc("DELETE /api/v1/cluster/nodes/{nodeID}", h.handleRemoveServer)
	mux.HandleFunc("GET /api/v1/status", h.handleStatus)
	mux.HandleFunc("GET /api/v1/cluster/nodes", h.handleGetCluster)

	log.Printf("[INFO] directory: starting HTTP API server on %s", h.httpAddr)
	logger := logging.NewLogger()

	return http.ListenAndServe(h.httpAddr, logging.LoggingMiddleware(logger)(h.corsMiddleware(h.leaderRedirectMiddleware(mux))))
}

func (h *HTTPService) writeError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func (h *HTTPService) leaderRedirectMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// requets to get the node configuration should be directed to the leader
		if (r.Method == "GET" && !strings.HasSuffix(r.URL.Path, "/nodes")) || strings.HasSuffix(r.URL.Path, "/status") {
			next.ServeHTTP(w, r)
			return
		}

		if !h.cluster.IsLeader() {
			leaderRaftAddr := h.cluster.LeaderAddr()
			if leaderRaftAddr == "" {
				h.writeError(w, http.StatusServiceUnavailable, "no leader available")
				return
			}

			leaderHTTPAddr, exists := h.nodeHTTPAddrs[leaderRaftAddr]
			if !exists {
				h.writeError(w, http.StatusServiceUnavailable, "leader HTTP address not known")
				return
			}

			// Redirect to leader
			redirectURL := fmt.Sprintf("http://%s%s", leaderHTTPAddr, r.URL.Path)
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (h *HTTPService) handleRegisterStore(w http.ResponseWriter, r *http.Request) {
	var req RegisterStoreData
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	if req.StoreID == "" || req.Address == "" {
		h.writeError(w, http.StatusBadRequest, "store_id and address are required")
		return
	}

	if err := h.dir.RegisterStore(req.StoreID, req.Address); err != nil {
		if err == ErrNotLeader {
			h.writeError(w, http.StatusServiceUnavailable, "not the leader")
			return
		}

		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"message": fmt.Sprintf("store %s registered successfully", req.StoreID),
	})
}

type AssignWriteLocationsResponse struct {
	LogicalVolumeID string              `json:"logical_volume_id"`
	Locations       []WriteLocationInfo `json:"locations"`
}

func (h *HTTPService) handleAssignWriteLocations(w http.ResponseWriter, r *http.Request) {
	var req AssignWriteLocationData
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	if req.FileID == "" || req.FileSize <= 0 {
		h.writeError(w, http.StatusBadRequest, "file_id and valid file_size are required")
		return
	}

	lvID, locations, err := h.dir.AssignWriteLocations(req.FileID, req.FileSize)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := AssignWriteLocationsResponse{
		LogicalVolumeID: lvID,
		Locations:       locations,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

type GetReadLocationsResponse struct {
	Locations []ReadLocationInfo `json:"locations"`
}

func (h *HTTPService) handleGetReadLocations(w http.ResponseWriter, r *http.Request) {
	fileID := r.PathValue("fileID")
	if fileID == "" {
		h.writeError(w, http.StatusBadRequest, "file_id is required")
		return
	}

	locations, err := h.dir.GetReadLocations(fileID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := GetReadLocationsResponse{
		Locations: locations,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

type AddVoterRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

func (h *HTTPService) handleAddVoter(w http.ResponseWriter, r *http.Request) {
	var req AddVoterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	if req.NodeID == "" || req.Address == "" {
		h.writeError(w, http.StatusBadRequest, "node_id and address are required")
		return
	}

	if err := h.cluster.AddVoter(req.NodeID, req.Address); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": fmt.Sprintf("Voter %s added successfully", req.NodeID),
	})
}

func (h *HTTPService) handleRemoveServer(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("nodeID")
	if nodeID == "" {
		h.writeError(w, http.StatusBadRequest, "node_id is required")
		return
	}

	if err := h.cluster.RemoveServer(nodeID); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message": fmt.Sprintf("Server %s removed successfully", nodeID),
	})
}

func (h *HTTPService) handleGetCluster(w http.ResponseWriter, _ *http.Request) {
	nodes, err := h.cluster.GetCluster()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	for i, node := range nodes {
		nodes[i].HTTPAddr = h.nodeHTTPAddrs[node.RaftAddr]
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

type StatusResponse struct {
	IsLeader   bool              `json:"is_leader"`
	LeaderAddr string            `json:"leader_addr"`
	Stats      map[string]string `json:"stats"`
}

func (h *HTTPService) handleStatus(w http.ResponseWriter, r *http.Request) {
	response := StatusResponse{
		IsLeader:   h.cluster.IsLeader(),
		LeaderAddr: h.cluster.LeaderAddr(),
		Stats:      h.cluster.Stats(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
