#!/usr/bin/env bash
# install-local.sh — Build from source and install to ~/.local/bin (or $DEST).
#
# Resolves the correct Cargo target directory via `cargo metadata` so the
# installed binary always matches the freshly-built artifact, regardless of
# CARGO_TARGET_DIR overrides or workspace settings.
#
# Usage:
#   ./install-local.sh              # build release, install to ~/.local/bin
#   DEST=/usr/local/bin ./install-local.sh   # custom destination
#   ./install-local.sh --debug      # install debug build instead
#
# Fixes: https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/79

set -euo pipefail

DEST="${DEST:-$HOME/.local/bin}"
PROFILE="release"
PROFILE_FLAG="--release"

for arg in "$@"; do
  case "$arg" in
    --debug)
      PROFILE="debug"
      PROFILE_FLAG=""
      ;;
    --help|-h)
      echo "Usage: $0 [--debug]"
      echo ""
      echo "Build from source and install am + mcp-agent-mail to ~/.local/bin."
      echo "Set DEST to override the install directory."
      echo ""
      echo "Options:"
      echo "  --debug    Install debug build instead of release"
      exit 0
      ;;
  esac
done

BIN_CLI="am"
BIN_SERVER="mcp-agent-mail"

# Resolve the Cargo target directory the same way Cargo does.
# This respects CARGO_TARGET_DIR, .cargo/config.toml [build] target-dir, etc.
get_target_dir() {
  if command -v cargo >/dev/null 2>&1; then
    local meta
    meta=$(cargo metadata --no-deps --format-version 1 2>/dev/null)
    if command -v jq >/dev/null 2>&1; then
      echo "$meta" | jq -r '.target_directory'
    else
      # Fallback: sed-based JSON extraction (works when path has no quotes)
      echo "$meta" | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'
    fi
  fi
}

TARGET_DIR=$(get_target_dir)
if [ -z "$TARGET_DIR" ]; then
  # Fallback: check CARGO_TARGET_DIR, then default ./target
  TARGET_DIR="${CARGO_TARGET_DIR:-$(pwd)/target}"
fi

echo "==> Building $PROFILE artifacts..."
# shellcheck disable=SC2086
cargo build $PROFILE_FLAG -p mcp-agent-mail -p mcp-agent-mail-cli

CLI_SRC="$TARGET_DIR/$PROFILE/$BIN_CLI"
SERVER_SRC="$TARGET_DIR/$PROFILE/$BIN_SERVER"

# Verify the artifacts exist and are executable
for bin in "$CLI_SRC" "$SERVER_SRC"; do
  if [ ! -x "$bin" ]; then
    echo "ERROR: Expected binary not found: $bin" >&2
    echo "" >&2
    echo "The Cargo target directory resolved to: $TARGET_DIR" >&2
    echo "If CARGO_TARGET_DIR is set, ensure it matches the build." >&2
    exit 1
  fi
done

# Show what we built
CLI_VERSION=$("$CLI_SRC" --version 2>/dev/null || echo "unknown")
echo "==> Built: $CLI_SRC ($CLI_VERSION)"

# Create destination if needed
mkdir -p "$DEST"

# Atomic install: write to temp, then rename (prevents partial copies)
for pair in "$CLI_SRC:$DEST/$BIN_CLI" "$SERVER_SRC:$DEST/$BIN_SERVER"; do
  src="${pair%%:*}"
  dst="${pair##*:}"
  tmp="${dst}.tmp.$$"
  install -m 0755 "$src" "$tmp"
  mv -f "$tmp" "$dst"
done

INSTALLED_VERSION=$("$DEST/$BIN_CLI" --version 2>/dev/null || echo "unknown")
echo "==> Installed to $DEST"
echo "    $DEST/$BIN_CLI ($INSTALLED_VERSION)"
echo "    $DEST/$BIN_SERVER"

# Verify the installed binary matches what we just built
if [ "$CLI_VERSION" != "$INSTALLED_VERSION" ]; then
  echo "WARNING: Installed version ($INSTALLED_VERSION) differs from built version ($CLI_VERSION)." >&2
  echo "  This could indicate a PATH or symlink issue." >&2
fi

# Check PATH
case ":$PATH:" in
  *":$DEST:"*) ;;
  *)
    echo ""
    echo "NOTE: $DEST is not in your PATH."
    echo "  Add it:  export PATH=\"$DEST:\$PATH\""
    ;;
esac
