# P2P HTTP Bridge - top level orchestration (Go daemon -> jniLibs -> APK)
#
#   make daemon        cross-compile the Go daemon for armeabi-v7a + arm64-v8a (NDK)
#   make daemon-static same, fully static (CGO_ENABLED=0), no NDK required
#   make test          Go tests (native)
#   make run           run the daemon on this machine (127.0.0.1:8080)
#   make apk           daemon + debug APK
#   make release       daemon + release APK (needs signing config)
#   make install       adb install the debug APK
#   make clean

CORE    := core
ANDROID := android
GRADLE  ?= $(shell test -x $(ANDROID)/gradlew && echo ./gradlew || echo gradle)

.PHONY: all daemon build-android daemon-static daemon-arm daemon-arm64 test vet run apk release install clean

all: apk

daemon build-android:
	cd $(CORE) && ./build.sh

daemon-static:
	cd $(CORE) && MODE=static ./build.sh

# 32-bit ARM: Fire TV Stick Lite / Basic
daemon-arm:
	cd $(CORE) && ABIS="armeabi-v7a" ./build.sh

# 64-bit ARM: Fire TV Stick 4K / 4K Max / Cube
daemon-arm64:
	cd $(CORE) && ABIS="arm64-v8a" ./build.sh

vet:
	cd $(CORE) && go vet ./...

test:
	cd $(CORE) && go test ./... -timeout 180s

run:
	cd $(CORE) && go run ./cmd/daemon -port 8080 -cache-mb 100

apk: daemon
	cd $(ANDROID) && $(GRADLE) :app:assembleDebug
	@echo "APK: $(ANDROID)/app/build/outputs/apk/debug/app-debug.apk"

release: daemon
	cd $(ANDROID) && $(GRADLE) :app:assembleRelease

install:
	adb install -r $(ANDROID)/app/build/outputs/apk/debug/app-debug.apk

clean:
	rm -rf $(ANDROID)/app/src/main/jniLibs/*/libbridge.so $(ANDROID)/app/build $(ANDROID)/build
