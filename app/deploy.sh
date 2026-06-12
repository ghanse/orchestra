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

# The v0.298+ CLI makes `sync` / `apps deploy` bundle-aware: if a databricks.yml
# is discoverable in the current directory or any parent (e.g. a generated
# orchestra_output/databricks.yml, or one in your workspace home), the command
# loads that bundle and fails with "Error: please specify target". Run every CLI
# call from a throwaway directory with no databricks.yml in its tree so the
# bundle loader can't attach. All paths passed to the CLI are absolute, so the
# working directory does not otherwise matter.
CLEAN_DIR="$(mktemp -d)"
trap 'rm -rf "$CLEAN_DIR"' EXIT
dbx() { (cd "$CLEAN_DIR" && databricks "${PROFILE_FLAG[@]}" "$@"); }

echo "==> Staging self-contained app bundle in $BUILD_DIR"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cp "$APP_DIR/app.py" "$APP_DIR/app.yaml" "$APP_DIR/requirements.txt" "$BUILD_DIR/"
cp -R "$REPO_ROOT/src/orchestra" "$BUILD_DIR/orchestra"
find "$BUILD_DIR/orchestra" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true

ME="$(dbx current-user me --output json \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["userName"])')"
SOURCE_PATH="${APP_SOURCE_PATH:-/Workspace/Users/$ME/$APP_NAME}"

echo "==> Ensuring app '$APP_NAME' exists"
if ! dbx apps get "$APP_NAME" >/dev/null 2>&1; then
  dbx apps create "$APP_NAME"
fi

echo "==> Syncing bundle to $SOURCE_PATH"
dbx sync "$BUILD_DIR" "$SOURCE_PATH"

echo "==> Deploying app"
dbx apps deploy "$APP_NAME" --source-code-path "$SOURCE_PATH"

echo "==> Deployed. App details:"
dbx apps get "$APP_NAME" --output json \
  | python3 -c 'import sys, json; d = json.load(sys.stdin); print("  name:", d.get("name")); print("  url: ", d.get("url", "(pending)"))'
echo "==> MCP endpoint will be: <app-url>/mcp"
