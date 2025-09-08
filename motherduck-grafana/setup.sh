#!/bin/bash

set -e
# Constants
PLUGIN_REPO="https://api.github.com/repos/motherduckdb/grafana-duckdb-datasource/releases/latest"
CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVISIONING_DIR="$CURRENT_DIR/provisioning"

# Detect OS and set plugin directory
case "$(uname -s)" in
Darwin)
  PLUGIN_DIR="$CURRENT_DIR/plugins/motherduck-duckdb-datasource"
  ;;
Linux)
  PLUGIN_DIR="$CURRENT_DIR/plugins/motherduck-duckdb-datasource"
  ;;
CYGWIN* | MINGW* | MSYS*)
  echo "⚠ Windows detected. Please manually unzip the plugin into your Grafana plugin folder."
  exit 1
  ;;
*)
  echo "❌ Unsupported OS. Please set PLUGIN_DIR manually."
  exit 1
  ;;
esac

mkdir -p "$PLUGIN_DIR"

# Fetch latest release info
echo "📡 Fetching latest release info..."
ZIP_URL=$(curl -s $PLUGIN_REPO | grep browser_download_url | grep motherduck-duckdb-datasource | grep ".zip" | cut -d '"' -f4 | head -n 1)

if [ -z "$ZIP_URL" ]; then
  echo "❌ Failed to fetch zip URL."
  exit 1
fi

# Extract filename from URL
ZIP_FILE=$(basename "$ZIP_URL")
echo "ZIP_FILE: $ZIP_FILE"

# Download plugin
echo "⬇ Downloading plugin from $ZIP_URL..."
curl -L -o "$ZIP_FILE" "$ZIP_URL"

unzip -o "$ZIP_FILE" -d "$PLUGIN_DIR"
rm -f "$ZIP_FILE"
echo "✅ Plugin extracted to $PLUGIN_DIR and zip removed"

#check if grafana is already running on port 3000 and stop it and remove it
CONTAINER_ID=$(docker ps --filter "publish=3000" --filter "name=grafana" --format "{{.ID}}")

if [ -n "$CONTAINER_ID" ]; then
  echo "🛑 Stopping Grafana Docker container running on port 3000..."
  docker stop "$CONTAINER_ID"
  docker rm "$CONTAINER_ID"
  echo "✅ Stopped and removed Grafana container running on port 3000."
else
  echo "ℹ No Grafana container running on port 3000."
fi

#check if grafana is stopped but not removed
if docker ps -a --filter "name=grafana" | grep -q grafana; then
  echo "🛑 Removing existing Grafana Docker container..."
  docker rm grafana
  echo "✅ Existing Grafana Docker container removed."
fi

# check to see if port 3000 is already
if lsof -Pi :3000 -sTCP:LISTEN -t >/dev/null; then
  echo "❌ Port 3000 is already in use. Please free up the port and try again."
  exit 1
fi

# Validate required environment variables
if ! printenv motherduck_token >/dev/null 2>&1; then
  echo "❌ Required environment variable 'motherduck_token' is not set." >&2
  exit 1
fi
MOTHERDUCK_TOKEN_VALUE="$(printenv motherduck_token)"

echo "🚀 Starting Grafana Docker container..."
docker run -d \
  --name=grafana \
  -p 3000:3000 \
  -v "$PLUGIN_DIR:/var/lib/grafana/plugins/motherduck-duckdb-datasource" \
  -v "$PROVISIONING_DIR:/etc/grafana/provisioning" \
  -e "GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=motherduck-duckdb-datasource" \
  -e "motherduck_token=${MOTHERDUCK_TOKEN_VALUE}" \
  grafana/grafana:latest-ubuntu

# check if docker container is not running. if not wait for 5 seconds, when 5 seconds go by and it is still not running, exit
for i in {1..5}; do
  if docker ps --filter "name=grafana" --filter "status=running" | grep -q grafana; then
    echo "✅ Grafana Docker container is running."
    break
  fi
  echo "⏳ Waiting for Grafana Docker container to start... ($i/5)"
  sleep 1
done

# If the container is still not running after 5 seconds, exit
if ! docker ps --filter "name=grafana" --filter "status=running" | grep -q grafana; then
  echo "❌ Grafana Docker container failed to start after 5 seconds."
  exit 1
fi

echo "🔗 Grafana is now running at http://localhost:3000"
