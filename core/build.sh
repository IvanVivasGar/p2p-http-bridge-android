#!/usr/bin/env bash
# Cross-compiles the Go daemon for Android (Fire TV / Android TV) and drops the binaries where the
# APK packs them: android/app/src/main/jniLibs/<abi>/libbridge.so
#
# The ".so" name is a trick: Android only extracts (and marks executable) files matching lib*.so
# from the APK into the app's nativeLibraryDir. That directory is the only place an app can exec
# binaries from on Android 10+ (W^X). The file is a regular executable, not a shared library.
#
# Modes (MODE=...):
#   ndk    (default) GOOS=android + cgo, linked with the NDK clang against bionic. This is the
#                    officially supported way to run Go on Android.
#   static           GOOS=linux, CGO_ENABLED=0: a fully static binary that needs no NDK at all.
#                    DNS is handled by the daemon itself (it installs public resolvers when
#                    /etc/resolv.conf is missing).
#
# Usage:
#   ./build.sh                 # both ABIs, ndk mode
#   ABIS="arm64" ./build.sh    # only arm64-v8a
#   MODE=static ./build.sh
#   ANDROID_NDK_HOME=/path/to/ndk ./build.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_BASE="${OUT_BASE:-$ROOT/../android/app/src/main/jniLibs}"
MODE="${MODE:-ndk}"
API="${API:-24}"                    # minSdk of the APK (Fire OS 5+ is API 22; 24 = Fire OS 6/7)
ABIS="${ABIS:-armeabi-v7a arm64-v8a}"
VERSION="${VERSION:-$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)}"

find_ndk() {
  if [[ -n "${ANDROID_NDK_HOME:-}" && -d "$ANDROID_NDK_HOME" ]]; then echo "$ANDROID_NDK_HOME"; return; fi
  local roots=(
    "${ANDROID_SDK_ROOT:-}" "${ANDROID_HOME:-}"
    "$HOME/Library/Android/sdk" "$HOME/Android/Sdk"
    /opt/homebrew/share/android-commandlinetools /usr/local/share/android-commandlinetools
  )
  for r in "${roots[@]}"; do
    [[ -n "$r" && -d "$r/ndk" ]] || continue
    local latest
    latest="$(ls -1 "$r/ndk" | sort -V | tail -n1)"
    [[ -n "$latest" ]] && { echo "$r/ndk/$latest"; return; }
  done
  return 1
}

HOST_TAG=""
if [[ "$MODE" == "ndk" ]]; then
  NDK="$(find_ndk)" || { echo "Android NDK not found. Set ANDROID_NDK_HOME (or use MODE=static)." >&2; exit 1; }
  case "$(uname -s)-$(uname -m)" in
    Darwin-*)       HOST_TAG="darwin-x86_64" ;;  # the NDK ships a universal x86_64 toolchain on macOS
    Linux-x86_64)   HOST_TAG="linux-x86_64" ;;
    *) echo "Unsupported build host $(uname -s)-$(uname -m)" >&2; exit 1 ;;
  esac
  TOOLCHAIN="$NDK/toolchains/llvm/prebuilt/$HOST_TAG/bin"
  [[ -d "$TOOLCHAIN" ]] || { echo "Toolchain not found: $TOOLCHAIN" >&2; exit 1; }
  echo "NDK: $NDK"
fi

build_abi() {
  local abi="$1" goarch cc cxx extra_env=()
  case "$abi" in
    armeabi-v7a)
      goarch=arm
      extra_env+=(GOARM=7)
      cc="armv7a-linux-androideabi${API}-clang"
      cxx="armv7a-linux-androideabi${API}-clang++"
      ;;
    arm64-v8a)
      goarch=arm64
      cc="aarch64-linux-android${API}-clang"
      cxx="aarch64-linux-android${API}-clang++"
      ;;
    *) echo "Unknown ABI $abi" >&2; return 1 ;;
  esac

  local out_dir="$OUT_BASE/$abi"
  mkdir -p "$out_dir"
  local out="$out_dir/libbridge.so"

  # -s -w strips symbols; -checklinkname=0 allows wlynxg/anet (used by pion on GOOS=android) on Go 1.23+.
  local ldflags="-s -w -checklinkname=0 -X main.version=$VERSION"

  echo ">> $abi ($MODE) -> $out"
  if [[ "$MODE" == "ndk" ]]; then
    (cd "$ROOT" && env \
      CGO_ENABLED=1 GOOS=android GOARCH="$goarch" ${extra_env[@]+"${extra_env[@]}"} \
      CC="$TOOLCHAIN/$cc" CXX="$TOOLCHAIN/$cxx" \
      go build -trimpath -buildvcs=false -ldflags "$ldflags" -o "$out" ./cmd/daemon)
  else
    (cd "$ROOT" && env \
      CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" ${extra_env[@]+"${extra_env[@]}"} \
      go build -trimpath -buildvcs=false -ldflags "$ldflags" -o "$out" ./cmd/daemon)
  fi
  chmod 755 "$out"
  ls -lh "$out" | awk '{print "   size:", $5}'
}

for abi in $ABIS; do build_abi "$abi"; done
echo "Done."
