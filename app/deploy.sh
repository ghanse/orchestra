#!/usr/bin/env bash
# Deploy the orchestra MCP server as a Databricks App.
#
# Builds a self-contained source bundle (app entrypoint + vendored orchestra
# package), syncs it to the workspace, and creates/deploys the app.
#
# Requirements: Databricks CLI v0.230+ authenticated to the target workspace.
#
# Env overrides:
#   APP_NAME            App name (default: orchestra-mcp)
#   APP_SOURCE_PATH     Workspace source path (default: /Workspace/Users/<me>/<APP_NAME>)
#   DATABRICKS_PROFILE  CLI profile to use (default: env/DEFAULT auth)
set -euo pipefail

APP_NAME="${APP_NAME:-orchestra-mcp}"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$APP_DIR/.." && pwd)"
BUILD_DIR="$APP_DIR/.build"

PROFILE_FLAG=()
if [ -n "${DATABRICKS_PROFILE:-}" ]; then
  PROFILE_FLAG=(--profile "$DATABRICKS_PROFILE")
fi

echo "==> Staging self-contained app bundle in $BUILD_DIR"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cp "$APP_DIR/app.py" "$APP_DIR/app.yaml" "$APP_DIR/requirements.txt" "$BUILD_DIR/"
cp -R "$REPO_ROOT/src/orchestra" "$BUILD_DIR/orchestra"
find "$BUILD_DIR/orchestra" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true

ME="$(databricks "${PROFILE_FLAG[@]}" current-user me --output json \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["userName"])')"
SOURCE_PATH="${APP_SOURCE_PATH:-/Workspace/Users/$ME/$APP_NAME}"

echo "==> Ensuring app '$APP_NAME' exists"
if ! databricks "${PROFILE_FLAG[@]}" apps get "$APP_NAME" >/dev/null 2>&1; then
  databricks "${PROFILE_FLAG[@]}" apps create "$APP_NAME"
fi

echo "==> Syncing bundle to $SOURCE_PATH"
databricks "${PROFILE_FLAG[@]}" sync "$BUILD_DIR" "$SOURCE_PATH"

echo "==> Deploying app"
databricks "${PROFILE_FLAG[@]}" apps deploy "$APP_NAME" --source-code-path "$SOURCE_PATH"

echo "==> Deployed. App details:"
databricks "${PROFILE_FLAG[@]}" apps get "$APP_NAME" --output json \
  | python3 -c 'import sys, json; d = json.load(sys.stdin); print("  name:", d.get("name")); print("  url: ", d.get("url", "(pending)"))'
echo "==> MCP endpoint will be: <app-url>/mcp"
