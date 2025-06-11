#!/bin/bash

set -e

echo "=== building filesystem ==="

echo "Building directory service..."
cd cmd/directory
go build -o ../../haystack-directory .
cd ../..

echo "Building store service..."
cd cmd/store
go build -o ../../haystack-store .
cd ../..

echo "✓ Build complete!"
echo "Executables created:"
echo "  - haystack-directory"
echo "  - haystack-store"

chmod +x setup_cluster.sh
chmod +x test_haystack.py

echo ""
echo "Usage:"
echo "1. Run './build.sh' (this script) to build the binaries"
echo "2. Run './setup_cluster.sh' to start the cluster"
echo "3. In another terminal, run './test_haystack.py' to test the system"
