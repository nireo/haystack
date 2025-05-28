package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

type HTTPService struct {
	httpAddr string
	store    *Store
}

type ErrorResponse struct {
	Error string `json:"error,omitempty"`
}

func NewHTTPService(httpAddr string, store *Store) *HTTPService {
	return &HTTPService{
		httpAddr: httpAddr,
		store:    store,
	}
}

func (h *HTTPService) writeError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func (h *HTTPService) writeMsg(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message": message,
	})
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

	mux.HandleFunc("POST /api/v1/create_logical", h.handleCreateLogicalVolume)

	log.Printf("Starting HTTP API server on %s", h.httpAddr)
	return http.ListenAndServe(h.httpAddr, h.corsMiddleware(mux))
}

type CreateLogicalVolumeRequest struct {
	LogicalVolumeID string `json:"logical_volume_id"`
}

func (h *HTTPService) handleCreateLogicalVolume(w http.ResponseWriter, r *http.Request) {
	var req CreateLogicalVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid json payload")
		return
	}

	err := h.store.NewLogical(req.LogicalVolumeID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "error creating logical volume")
		return
	}

	h.writeMsg(w, fmt.Sprintf("successfully created logical volume: %s", req.LogicalVolumeID))
}

func (h *HTTPService) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	fileID := r.PathValue("file_id")
	if fileID == "" {
		h.writeError(w, http.StatusBadRequest, "file_id is not provided in path")
		return
	}

	logicalID := r.PathValue("logical_id")
	if fileID == "" {
		h.writeError(w, http.StatusBadRequest, "file_id is not provided in path")
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "cannot read request body")
		return
	}
	r.Body.Close()

	err = h.store.CreateFile(fileID, logicalID, b)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("error writing file: %s", err))
		return
	}

	h.writeMsg(w, "successfully wrote file")
}
