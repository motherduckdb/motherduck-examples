#!/bin/bash
# get-starter.sh - Download a specific starter from motherduck-examples repository
# 
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/main/scripts/get-starter.sh | bash -s <starter-name>
#
# This script uses git sparse checkout to download only the selected starter folder
# without cloning the entire repository.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REPO="motherduckdb/motherduck-examples"
REPO_URL="https://github.com/${REPO}.git"
# Default to main, but can be overridden via BRANCH env var for PR testing
# Example: BRANCH=feat/reorg curl -fsSL ... | bash -s dbt-ai-prompt
BRANCH="${BRANCH:-main}"

# Available starters (folder names in the repo)
AVAILABLE_STARTERS=(
  "dbt-ai-prompt"
  "dbt-dual-execution"
  "dbt-ducklake"
  "dbt-ingestion-s3"
  "dbt-local-ducklake"
  "dbt-metricflow"
  "dlt-db-replication"
  "motherduck-grafana"
  "motherduck-ui"
  "postgres-demo"
  "python_ingestion"
  "sqlmesh-demo"
)

# Function to print available starters
list_starters() {
  echo -e "${BLUE}Available starters:${NC}"
  echo ""
  for starter in "${AVAILABLE_STARTERS[@]}"; do
    echo "  - ${starter}"
  done
  echo ""
}

# Function to check if a starter exists
starter_exists() {
  local starter=$1
  for available in "${AVAILABLE_STARTERS[@]}"; do
    if [ "$available" = "$starter" ]; then
      return 0
    fi
  done
  return 1
}

# Get starter name from argument
STARTER_NAME=${1:-}

# If no starter name provided, show usage
if [ -z "$STARTER_NAME" ]; then
  echo -e "${YELLOW}MotherDuck Starters - Get a starter by name${NC}"
  echo ""
  echo "Usage:"
  echo "  curl -fsSL https://raw.githubusercontent.com/${REPO}/${BRANCH}/scripts/get-starter.sh | bash -s <starter-name>"
  echo ""
  echo "Or download and run locally:"
  echo "  ./scripts/get-starter.sh <starter-name>"
  echo ""
  list_starters
  echo "Example:"
  echo "  curl -fsSL https://raw.githubusercontent.com/${REPO}/${BRANCH}/scripts/get-starter.sh | bash -s dbt-ai-prompt"
  exit 1
fi

# Check if starter exists
if ! starter_exists "$STARTER_NAME"; then
  echo -e "${RED}Error: Starter '${STARTER_NAME}' not found.${NC}"
  echo ""
  list_starters
  exit 1
fi

# Check if directory already exists
if [ -d "$STARTER_NAME" ]; then
  echo -e "${YELLOW}Warning: Directory '${STARTER_NAME}' already exists.${NC}"
  read -p "Do you want to remove it and continue? (y/N) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
  fi
  rm -rf "$STARTER_NAME"
fi

echo -e "${BLUE}Fetching starter: ${STARTER_NAME}${NC}"
echo ""

# Clone with minimal history and sparse checkout
# Note: --sparse flag enables sparse-checkout but we still need to configure it
git clone --depth 1 --filter=blob:none --sparse -b "${BRANCH}" "${REPO_URL}" "${STARTER_NAME}.tmp" || {
  echo -e "${RED}Error: Failed to clone repository.${NC}"
  echo "Make sure you have git installed and internet connectivity."
  echo "If using a PR branch, set BRANCH env var: BRANCH=your-branch-name curl ..."
  exit 1
}

cd "${STARTER_NAME}.tmp"
# Configure sparse checkout to only get the starter folder
git sparse-checkout set "${STARTER_NAME}"
# Checkout the files (sparse checkout doesn't auto-checkout)
git checkout "${BRANCH}" 2>/dev/null || git checkout HEAD

# Check if the starter directory exists after sparse checkout
if [ ! -d "$STARTER_NAME" ]; then
  echo -e "${RED}Error: Starter directory '${STARTER_NAME}' not found in repository.${NC}"
  echo "Available directories:"
  ls -la
  cd ..
  rm -rf "${STARTER_NAME}.tmp"
  exit 1
fi

# Create final directory in parent
mkdir -p "../${STARTER_NAME}"

# Move all contents (including hidden files) from starter folder to final location
if [ -d "$STARTER_NAME" ]; then
  # Use find to handle all files including hidden ones
  find "${STARTER_NAME}" -mindepth 1 -maxdepth 1 -exec mv {} "../${STARTER_NAME}/" \;
  rmdir "$STARTER_NAME" 2>/dev/null || true
fi

# Remove git history
rm -rf .git

# Move back to parent directory and clean up
cd ..
rm -rf "${STARTER_NAME}.tmp"

# Final check
if [ -d "$STARTER_NAME" ] && [ "$(ls -A $STARTER_NAME 2>/dev/null)" ]; then
  echo -e "${GREEN}✓ Starter '${STARTER_NAME}' ready!${NC}"
  echo ""
  echo "Next steps:"
  echo "  cd ${STARTER_NAME}"
  echo "  # Follow the README.md for setup instructions"
else
  echo -e "${RED}Error: Failed to extract starter '${STARTER_NAME}'.${NC}"
  exit 1
fi
