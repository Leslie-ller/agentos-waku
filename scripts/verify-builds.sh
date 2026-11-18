#!/usr/bin/env bash
#
# Cross-platform build & smoke verification pipeline for AgentOS Waku:
#   1. Static Mobile/Web SPA (`apps/web/dist-spa`)
#   2. Linux x86_64 headless daemon + GPUI desktop (`waku-daemon`, `waku`)
#   3. Windows x86_64 (`x86_64-pc-windows-msvc` via cargo-xwin or `-gnu` fallback)
#      + portable zip archive (`agentos-waku-<version>-<triple>.zip`)
#   4. macOS Apple Silicon (`aarch64-apple-darwin` via cargo-zigbuild on Linux or
#      `scripts/bundle.sh release` on macOS)
#
# Usage:
#   ./scripts/verify-builds.sh [--web] [--linux] [--windows] [--macos] [--all]
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export CARGO_INCREMENTAL=0
if command -v python3 >/dev/null 2>&1; then
  PY_SCRIPTS="$(python3 -c 'import sysconfig; print(sysconfig.get_path("scripts"))' 2>/dev/null || true)"
  ZIG_DIR="$(python3 -c 'import os, ziglang; print(os.path.dirname(ziglang.__file__))' 2>/dev/null || true)"
  export PATH="${PY_SCRIPTS:+$PY_SCRIPTS:}${ZIG_DIR:+$ZIG_DIR:}$HOME/.bun/bin:$PATH"
fi
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT_DIR/target}"
OUT_DIR="${VERIFY_OUT_DIR:-$TARGET_DIR/verify-artifacts}"
mkdir -p "$OUT_DIR"

VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)"

DO_WEB=0
DO_LINUX=0
DO_WINDOWS=0
DO_MACOS=0

if [ "$#" -eq 0 ]; then
  DO_WEB=1
  DO_LINUX=1
  DO_WINDOWS=1
  DO_MACOS=1
fi

for arg in "$@"; do
  case "$arg" in
    --web) DO_WEB=1 ;;
    --linux) DO_LINUX=1 ;;
    --windows) DO_WINDOWS=1 ;;
    --macos) DO_MACOS=1 ;;
    --all)
      DO_WEB=1
      DO_LINUX=1
      DO_WINDOWS=1
      DO_MACOS=1
      ;;
    *)
      echo "Unknown flag: $arg" >&2
      exit 2
      ;;
  esac
done

if [ "$DO_WEB" -eq 1 ]; then
  echo "==> [1/4] Building @waku/web static SPA bundle..."
  bun install --frozen-lockfile 2>/dev/null || bun install
  bun --filter @waku/web test
  (cd apps/web && bun run build:spa)
  test -f apps/web/dist-spa/index.html
  echo "    OK: apps/web/dist-spa/index.html ($(wc -c < apps/web/dist-spa/index.html) bytes)"
fi

if [ "$DO_LINUX" -eq 1 ]; then
  echo "==> [2/4] Building & testing Linux x86_64 workspace..."
  cargo test --target-dir "$TARGET_DIR" -p waku-protocol -p waku-client -p waku-core -p waku-daemon
  cargo build --release --target-dir "$TARGET_DIR" --package waku --bin waku --bin waku_js_repl --package waku-daemon --bin waku-daemon
  "$TARGET_DIR/release/waku-daemon" --help >/dev/null
  echo "    OK: Linux x86_64 binaries built & smoke-checked"
fi

if [ "$DO_WINDOWS" -eq 1 ]; then
  echo "==> [3/4] Cross-building Windows x86_64 release artifacts..."
  if command -v cargo-xwin >/dev/null 2>&1; then
    WIN_TRIPLE="x86_64-pc-windows-msvc"
    rustup target add "$WIN_TRIPLE"
    if command -v llvm-rc >/dev/null 2>&1; then
      REAL_LLVM_RC="$(command -v llvm-rc)"
      RC_WRAP_DIR="$(mktemp -d)"
      GPUI_DIR="$(find "${CARGO_HOME:-$HOME/.cargo}/git/checkouts" -path '*/crates/gpui' -type d 2>/dev/null | head -n 1 || true)"
      cat > "$RC_WRAP_DIR/llvm-rc" <<EOF
#!/bin/sh
exec "$REAL_LLVM_RC" ${GPUI_DIR:+-I "$GPUI_DIR" -I "$GPUI_DIR/resources/windows"} -I "$ROOT_DIR" -I "$ROOT_DIR/resources/windows" "\$@"
EOF
      chmod +x "$RC_WRAP_DIR/llvm-rc"
      export PATH="$RC_WRAP_DIR:$PATH"
    fi
    # On non-Windows cross hosts `gpui_windows` skips offline `fxc.exe` shader
    # compilation and compiles HLSL shaders via `D3DCompile` when debug_assertions is on.
    cargo-xwin xwin build --release --config "profile.release.debug-assertions=true" \
      --target "$WIN_TRIPLE" --target-dir "$TARGET_DIR" \
      --package waku --bin waku --bin waku_js_repl \
      --package waku-daemon --bin waku-daemon \
      --package waku-computer-use --bin waku_computer_use
  else
    WIN_TRIPLE="x86_64-pc-windows-gnu"
    rustup target add "$WIN_TRIPLE"
    cargo build --release --target "$WIN_TRIPLE" --target-dir "$TARGET_DIR" \
      --package waku-daemon --bin waku-daemon
    cargo check --target "$WIN_TRIPLE" --target-dir "$TARGET_DIR" \
      -p waku-protocol -p waku-client -p waku-core
  fi

  WIN_STAGE_ROOT="$(mktemp -d)"
  WIN_PKG_NAME="agentos-waku-${VERSION}-${WIN_TRIPLE}"
  WIN_STAGE="$WIN_STAGE_ROOT/$WIN_PKG_NAME"
  mkdir -p "$WIN_STAGE/resources"
  for exe in waku.exe waku-daemon.exe waku_js_repl.exe waku_computer_use.exe; do
    if [ -f "$TARGET_DIR/$WIN_TRIPLE/release/$exe" ]; then
      cp "$TARGET_DIR/$WIN_TRIPLE/release/$exe" "$WIN_STAGE/$exe"
    fi
  done
  cp LICENSE "$WIN_STAGE/LICENSE"
  if [ -f apps/web/dist-spa/index.html ]; then
    cp -R apps/web/dist-spa "$WIN_STAGE/resources/web-dist"
  fi
  WIN_ZIP="$OUT_DIR/${WIN_PKG_NAME}.zip"
  rm -f "$WIN_ZIP"
  (cd "$WIN_STAGE_ROOT" && zip -qr "$WIN_ZIP" "$WIN_PKG_NAME")
  rm -rf "$WIN_STAGE_ROOT"
  echo "    OK: Windows package $WIN_ZIP"
fi

if [ "$DO_MACOS" -eq 1 ]; then
  echo "==> [4/4] Building / cross-building macOS aarch64-apple-darwin..."
  MAC_TRIPLE="aarch64-apple-darwin"
  if [ "$(uname -s)" = "Darwin" ]; then
    CARGO_TARGET_DIR="$TARGET_DIR" ./scripts/bundle.sh release
  elif command -v cargo-zigbuild >/dev/null 2>&1; then
    rustup target add "$MAC_TRIPLE"
    cargo zigbuild --release --target "$MAC_TRIPLE" --target-dir "$TARGET_DIR" \
      --package waku-daemon --bin waku-daemon
    cp "$TARGET_DIR/$MAC_TRIPLE/release/waku-daemon" "$OUT_DIR/waku-daemon-${MAC_TRIPLE}"
  fi
fi

echo "==> Build verification artifacts in $OUT_DIR:"
ls -lh "$OUT_DIR" || true
