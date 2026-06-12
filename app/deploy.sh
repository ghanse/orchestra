#!/usr/bin/env bash
# Deploy the orchestra MCP server as a Databricks App.
#
# Builds a self-contained source bundle (app entrypoint + vendored orchestra
# package), syncs it to the workspace, and creates/deploys the app.
#
# Requirements: Databricks CLI v0.230+ authenticated to the target workspace.
#
# Env overrides:
#   APP_NAME            App name (default: mcp-orchestra). Keep the `mcp-` prefix —
#                       Databricks only surfaces apps named `mcp-*` under AI Gateway > MCPs.
#   APP_SOURCE_PATH     Workspace source path the app deploys from (default:
#                       /Workspace/Shared/<APP_NAME>). It MUST be readable by the app's
#                       service principal, so it defaults to /Workspace/Shared — NOT a user's
#                       private /Workspace/Users/<me> home, which the app SP cannot read.
#   DATABRICKS_PROFILE  CLI profile to use (default: env/DEFAULT auth)
set -euo pipefail

APP_NAME="${APP_NAME:-mcp-orchestra}"
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

# Deploy the source from a location the app's service principal can read. A user's
# private /Workspace/Users/<me> home is NOT readable by the app SP, so default to
# the shared workspace folder. Override with APP_SOURCE_PATH if you use a different
# all-users location.
SOURCE_PATH="${APP_SOURCE_PATH:-/Workspace/Shared/$APP_NAME}"

echo "==> Ensuring app '$APP_NAME' exists"
if ! dbx apps get "$APP_NAME" >/dev/null 2>&1; then
  dbx apps create "$APP_NAME"
fi

echo "==> Syncing bundle to $SOURCE_PATH"
dbx sync "$BUILD_DIR" "$SOURCE_PATH"

echo "==> Deploying app"
dbx apps deploy "$APP_NAME" --source-code-path "$SOURCE_PATH"

echo "==> Deployed. App details:"
APP_URL="$(dbx apps get "$APP_NAME" --output json \
  | python3 -c 'import sys, json; print(json.load(sys.stdin).get("url", ""))')"
echo "  name: $APP_NAME"
echo "  url:  ${APP_URL:-(pending — re-run 'databricks apps get $APP_NAME')}"
echo
echo "==> Next steps to use it in Genie Code:"
echo "  1. MCP endpoint:  ${APP_URL:-<app-url>}/mcp"
echo "  2. The app is named '$APP_NAME' (mcp- prefix), so it appears in the workspace"
echo "     under AI Gateway > MCPs."
echo "  3. Grant 'Can use' on the app to the users / service principals that will call it"
echo "     (Apps UI > Permissions, or: databricks apps set-permissions $APP_NAME ...)."
echo "  4. Grant that app's service principal access to the catalogs/schemas/volumes the"
echo "     migration touches (and any SQL warehouse used by the reporting tools)."
echo "  5. In Genie Code, select the orchestra MCP server and use its orchestra_* tools."
