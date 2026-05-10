#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/init-cuckoo-volume.sh [volume-name]
# If volume-name is not provided, tries to find a volume containing 'apidata'.

REPO_CUCKOO="./backend/data/cuckoo.dat"
if [ ! -f "$REPO_CUCKOO" ]; then
  echo "Error: repo cuckoo file not found at $REPO_CUCKOO"
  exit 1
fi

VOL=${1:-}
if [ -z "$VOL" ]; then
  VOL=$(docker volume ls --format '{{.Name}}' | grep -m1 'apidata' || true)
fi

if [ -z "$VOL" ]; then
  echo "No docker volume found (no argument supplied and no volume matching 'apidata')."
  echo "Run: $0 <volume-name>"
  exit 1
fi

echo "Using docker volume: $VOL"

# Check if cuckoo.dat already exists in the volume
EXISTS=$(docker run --rm -v "${VOL}":/data alpine sh -c '[ -f /data/cuckoo.dat ] && echo yes || echo no')
if [ "$EXISTS" = "yes" ]; then
  echo "cuckoo.dat already exists in volume $VOL — nothing to do."
  exit 0
fi

# Copy from repo path into the volume via a temporary container that mounts both host path and the volume
docker run --rm -v "$(pwd)/backend/data":/src -v "${VOL}":/dst alpine sh -c 'cp /src/cuckoo.dat /dst/cuckoo.dat && sync'

if [ $? -eq 0 ]; then
  echo "Copied $REPO_CUCKOO -> volume:$VOL/cuckoo.dat"
else
  echo "Failed to copy cuckoo.dat into volume $VOL"
  exit 1
fi
