package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/nireo/haystack/directory"
)

type Client struct {
	directoryAddr string
}

func NewClient(directoryAddr string) *Client {
	return &Client{
		directoryAddr: directoryAddr,
	}
}

// CreateFile creates a file by first getting write locations from the directory,
// then writing the file content to each assigned store location
func (c *Client) CreateFile(fileID string, content []byte) error {
	reqBody := directory.AssignWriteLocationData{
		FileID:   fileID,
		FileSize: int64(len(content)),
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.directoryAddr+"/api/v1/files/write-locations", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to directory: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("directory returned status %d", res.StatusCode)
	}

	var writeLocations directory.AssignWriteLocationsResponse
	if err := json.NewDecoder(res.Body).Decode(&writeLocations); err != nil {
		return fmt.Errorf("failed to decode response body: %w", err)
	}

	for _, location := range writeLocations.Locations {
		if err := c.writeToStore(location, fileID, writeLocations.LogicalVolumeID, content); err != nil {
			return fmt.Errorf("failed to write to store %s: %w", location.StoreID, err)
		}
	}

	return nil
}

// writeToStore writes file content to a specific store
func (c *Client) writeToStore(location directory.WriteLocationInfo, fileID, logicalVolumeID string, content []byte) error {
	url := fmt.Sprintf("http://%s/api/v1/%s/%s/create_file", location.Address, logicalVolumeID, fileID)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(content))
	if err != nil {
		return fmt.Errorf("failed to create store request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to store: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("store returned status %d", res.StatusCode)
	}

	return nil
}

// ReadFile reads a file by first getting read locations from the directory,
// then reading the file content from one of the available store locations
func (c *Client) ReadFile(fileID string) ([]byte, error) {
	// Step 1: Get read locations from directory
	url := fmt.Sprintf("%s/api/v1/files/%s/read-locations", c.directoryAddr, fileID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to directory: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("directory returned status %d", res.StatusCode)
	}

	var readLocations directory.GetReadLocationsResponse
	if err := json.NewDecoder(res.Body).Decode(&readLocations); err != nil {
		return nil, fmt.Errorf("failed to decode response body: %w", err)
	}

	if len(readLocations.Locations) == 0 {
		return nil, fmt.Errorf("no read locations available for file %s", fileID)
	}

	for _, location := range readLocations.Locations {
		content, err := c.readFromStore(location, fileID)
		if err != nil {
			fmt.Printf("Failed to read from store %s: %v\n", location.StoreID, err)
			continue
		}
		return content, nil
	}

	return nil, fmt.Errorf("failed to read file from any available location")
}

// readFromStore reads file content from a specific store
func (c *Client) readFromStore(location directory.ReadLocationInfo, fileID string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/api/v1/%s/%s/read_file", location.StoreAddress, location.LogicalVolumeID, fileID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create store request: %w", err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to store: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("store returned status %d", res.StatusCode)
	}

	content, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return content, nil
}
