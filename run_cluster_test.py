#!/usr/bin/env python3

import requests
import json
import time
import random
import string
import sys
import os
import uuid
from typing import Dict, List, Optional


class HaystackClient:
    def __init__(self, directory_urls: List[str]):
        self.directory_urls = directory_urls
        self.session = requests.Session()
        self.session.timeout = 30

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make request to directory cluster with failover"""
        last_exception = None

        for url in self.directory_urls:
            try:
                full_url = f"{url}{endpoint}"
                response = self.session.request(method, full_url, **kwargs)
                return response
            except requests.exceptions.RequestException as e:
                last_exception = e
                continue

        raise last_exception or Exception("All directory nodes failed")

    def get_cluster_status(self) -> Dict:
        """Get cluster status"""
        response = self._make_request("GET", "/api/v1/status")
        response.raise_for_status()
        return response.json()

    def get_cluster_nodes(self) -> List[Dict]:
        """Get cluster nodes"""
        response = self._make_request("GET", "/api/v1/cluster/nodes")
        response.raise_for_status()
        return response.json()

    def assign_write_locations(self, file_id: str, file_size: int) -> Dict:
        """Assign write locations for a file"""
        data = {"file_id": file_id, "file_size": file_size}
        response = self._make_request(
            "POST", "/api/v1/files/write-locations", json=data
        )
        response.raise_for_status()
        return response.json()

    def get_read_locations(self, file_id: str) -> Dict:
        """Get read locations for a file"""
        response = self._make_request("GET", f"/api/v1/files/{file_id}/read-locations")
        response.raise_for_status()
        return response.json()

    def write_file_to_store(
        self, store_address: str, logical_volume_id: str, file_id: str, data: bytes
    ) -> bool:
        """Write file to a specific store"""
        try:
            file_response = requests.post(
                f"http://{store_address}/api/v1/{logical_volume_id}/{file_id}/create_file",
                data=data,
                headers={"Content-Type": "application/octet-stream"},
                timeout=30,
            )

            if file_response.status_code == 200:
                return True
            else:
                print(
                    f"Failed to write to store {store_address}: {file_response.status_code} - {file_response.text}"
                )
                return False

        except Exception as e:
            print(f"Error writing to store {store_address}: {e}")
            return False

    def read_file_from_store(
        self, store_address: str, logical_volume_id: str, file_id: str
    ) -> Optional[bytes]:
        """Read file from a specific store"""
        try:
            response = requests.get(
                f"http://{store_address}/api/v1/{logical_volume_id}/{file_id}/read_file",
                timeout=30,
            )

            if response.status_code == 200:
                return response.content
            else:
                print(
                    f"Failed to read from store {store_address}: {response.status_code}"
                )
                return None

        except Exception as e:
            print(f"Error reading from store {store_address}: {e}")
            return None


def generate_random_data(size: int) -> bytes:
    """Generate random data of specified size"""
    return "".join(
        random.choices(string.ascii_letters + string.digits, k=size)
    ).encode()


def test_cluster_status(client: HaystackClient):
    """Test cluster status and nodes"""
    print("=== Testing Cluster Status ===")

    try:
        status = client.get_cluster_status()
        print(f"Cluster Status: {json.dumps(status, indent=2)}")

        nodes = client.get_cluster_nodes()
        print(f"Cluster Nodes: {json.dumps(nodes, indent=2)}")

        print("✓ Cluster status test passed")
        return True
    except Exception as e:
        print(f"✗ Cluster status test failed: {e}")
        return False


def test_write_locations(client: HaystackClient):
    """Test write location assignment"""
    print("\n=== Testing Write Location Assignment ===")

    try:
        file_id = str(uuid.uuid4())
        file_size = 1024

        print(
            f"Requesting write locations for file: {file_id} (size: {file_size} bytes)"
        )

        locations = client.assign_write_locations(file_id, file_size)
        print(f"Directory response: {json.dumps(locations, indent=2)}")

        logical_volume_id = locations.get("logical_volume_id")
        location_list = locations.get("locations", [])

        if logical_volume_id and len(location_list) > 0:
            print(f"✓ Directory assigned logical volume: {logical_volume_id}")
            print(f"✓ Directory provided {len(location_list)} write locations")
            print("✓ Write location assignment test passed")
            return True, file_id, locations
        else:
            print("✗ Invalid response from directory service")
            return False, None, None

    except Exception as e:
        print(f"✗ Write location assignment test failed: {e}")
        return False, None, None


def test_read_locations(client: HaystackClient, file_id: str):
    """Test read location retrieval"""
    print("\n=== Testing Read Location Retrieval ===")

    try:
        locations = client.get_read_locations(file_id)
        print(f"Read locations for {file_id}: {json.dumps(locations, indent=2)}")

        if "locations" in locations:
            print("✓ Read location retrieval test passed")
            return True, locations
        else:
            print("✗ No read locations returned")
            return False, None

    except Exception as e:
        print(f"✗ Read location retrieval test failed: {e}")
        return False, None


def test_file_operations(client: HaystackClient):
    """Test end-to-end file operations"""
    print("\n=== Testing End-to-End File Operations ===")

    try:
        # Generate test data
        test_data = generate_random_data(1024)
        file_id = str(uuid.uuid4())

        print(f"Generated test data of size {len(test_data)} bytes for file: {file_id}")

        print(f"Requesting write locations for file {file_id}...")

        write_locations = client.assign_write_locations(file_id, len(test_data))
        print(
            f"Directory assigned write locations: {json.dumps(write_locations, indent=2)}"
        )

        logical_volume_id = write_locations.get("logical_volume_id")
        locations = write_locations.get("locations", [])

        if not logical_volume_id:
            print("✗ No logical volume ID returned from directory")
            return False

        if not locations:
            print("✗ No write locations available")
            return False

        print(f"Directory assigned logical volume: {logical_volume_id}")
        print(f"Write locations count: {len(locations)}")

        write_success_count = 0
        for i, location in enumerate(locations):
            store_address = location.get("store_address")
            if store_address:
                print(f"Writing to store {i+1}/{len(locations)}: {store_address}")
                success = client.write_file_to_store(
                    store_address, logical_volume_id, file_id, test_data
                )
                if success:
                    write_success_count += 1
                    print(f"  ✓ Successfully wrote to {store_address}")
                else:
                    print(f"  ✗ Failed to write to {store_address}")

        if write_success_count == 0:
            print("✗ Failed to write to any stores")
            return False
        elif write_success_count < len(locations):
            print(f"⚠ Only wrote to {write_success_count}/{len(locations)} stores")
        else:
            print(f"✓ Successfully wrote to all {len(locations)} stores")

        # Step 3: Request read locations from directory service
        print(f"Requesting read locations for file {file_id}...")

        read_locations = client.get_read_locations(file_id)
        print(
            f"Directory returned read locations: {json.dumps(read_locations, indent=2)}"
        )

        # Step 4: Read from available locations and verify data
        read_success_count = 0
        verified_count = 0

        for i, location in enumerate(read_locations.get("locations", [])):
            store_address = location.get("store_address")
            if store_address:
                print(f"Reading from store {i+1}: {store_address}")
                read_data = client.read_file_from_store(
                    store_address, logical_volume_id, file_id
                )

                if read_data is not None:
                    read_success_count += 1
                    if read_data == test_data:
                        verified_count += 1
                        print(f"  ✓ Data verified from {store_address}")
                    else:
                        print(
                            f"  ✗ Data mismatch from {store_address} (expected {len(test_data)} bytes, got {len(read_data)} bytes)"
                        )
                else:
                    print(f"  ✗ Failed to read from {store_address}")

        # Results
        print(f"\nOperation Summary:")
        print(f"  Logical Volume ID: {logical_volume_id}")
        print(f"  Write Success: {write_success_count}/{len(locations)} stores")
        print(
            f"  Read Success: {read_success_count}/{len(read_locations.get('locations', []))} stores"
        )
        print(
            f"  Data Verified: {verified_count}/{read_success_count} successful reads"
        )

        if verified_count > 0:
            print("✓ End-to-end file operations test passed")
            return True
        else:
            print("✗ End-to-end file operations test failed - no verified reads")
            return False

    except Exception as e:
        print(f"✗ End-to-end file operations test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Main test function"""
    print("Haystack Distributed File System Test")
    print("=" * 50)

    # Configuration
    directory_urls = [
        "http://127.0.0.1:8001",
        "http://127.0.0.1:8002",
        "http://127.0.0.1:8003",
    ]

    # Initialize client
    client = HaystackClient(directory_urls)

    # Wait for cluster to be ready
    print("Waiting for cluster to be ready...")
    max_retries = 30
    for i in range(max_retries):
        try:
            status = client.get_cluster_status()
            if status.get("is_leader") or status.get("leader_addr"):
                print("✓ Cluster is ready")
                break
        except Exception as e:
            if i == max_retries - 1:
                print(f"✗ Cluster not ready after {max_retries} attempts: {e}")
                return 1
            time.sleep(2)

    # Run tests
    tests_passed = 0
    total_tests = 3

    if test_cluster_status(client):
        tests_passed += 1

    if test_write_locations(client)[0]:
        tests_passed += 1

    if test_file_operations(client):
        tests_passed += 1

    # Results
    print(f"\n=== Test Results ===")
    print(f"Tests passed: {tests_passed}/{total_tests}")

    if tests_passed == total_tests:
        print("✓ All tests passed!")
        return 0
    else:
        print("✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
