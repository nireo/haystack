package store

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type HTTPService struct {
	httpAddr    string
	store       *Store
	tlsConfig   *tls.Config
	authService *AuthService
}

type ErrorResponse struct {
	Error string `json:"error,omitempty"`
}

func NewHTTPService(httpAddr string, store *Store, certPath, keyPath string, jwtSecret string) (*HTTPService, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %s", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	authService, err := NewAuthService(jwtSecret)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth service: %w", err)
	}

	return &HTTPService{
		httpAddr:    httpAddr,
		store:       store,
		tlsConfig:   tlsConfig,
		authService: authService,
	}, nil
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

	// Add login endpoint
	mux.HandleFunc("POST /api/v1/login", h.handleLogin)

	// Add auth middleware to protected endpoints
	mux.HandleFunc("POST /api/v1/create_logical", h.authMiddleware(h.handleCreateLogicalVolume))
	mux.HandleFunc("POST /api/v1/create_file", h.authMiddleware(h.handleCreateFile))
	mux.HandleFunc("GET /api/v1/read_file", h.authMiddleware(h.handleReadFile))
	mux.HandleFunc("GET /api/v1/logicals", h.authMiddleware(h.handleGetLogicals))

	log.Printf("[INFO] store: starting HTTP API server on %s", h.httpAddr)

	server := &http.Server{
		Addr:      h.httpAddr,
		Handler:   h.corsMiddleware(mux),
		TLSConfig: h.tlsConfig,
	}

	return server.ListenAndServeTLS("", "")
}

// authMiddleware wraps an HTTP handler with JWT authentication
func (h *HTTPService) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			h.writeError(w, http.StatusUnauthorized, "missing authorization header")
			return
		}

		// Remove "Bearer " prefix
		tokenString := strings.TrimPrefix(authHeader, "Bearer ")

		claims, err := h.authService.ValidateToken(tokenString)
		if err != nil {
			h.writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}

		// Add claims to request context
		ctx := r.Context()
		ctx = context.WithValue(ctx, "claims", claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	}
}

// handleLogin handles user login and returns a JWT token
func (h *HTTPService) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	resp, err := h.authService.Login(req.Username, req.Password)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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

func (h *HTTPService) handleReadFile(w http.ResponseWriter, r *http.Request) {
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

	data, err := h.store.ReadFile(fileID, logicalID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("error reading file: %s", err))
		return
	}

	w.Write(data)
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPService) handleGetLogicals(w http.ResponseWriter, _ *http.Request) {
	volumes := make([]LogicalVolumeInfo, 0, len(h.store.logicals))
	for id, file := range h.store.logicals {
		size, err := file.Size()
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("error reading file length: %s", err))
			return
		}

		volumes = append(volumes, LogicalVolumeInfo{
			ID:   id,
			Size: size,
		})
	}

	json.NewEncoder(w).Encode(&volumes)
}
