# p2p-http-bridge-android

[![Build and Release](https://github.com/ivanvivas/p2p-http-bridge-android/actions/workflows/build.yml/badge.svg)](https://github.com/ivanvivas/p2p-http-bridge-android/actions/workflows/build.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ivanvivas/p2p-http-bridge-android)](https://goreportcard.com/report/github.com/ivanvivas/p2p-http-bridge-android)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**P2P-to-HTTP streaming bridge** optimized for **Fire OS / Android TV** devices with constrained RAM (~1–1.5 GB) as well as desktop media centers (macOS / Linux / Windows).

Many media center frontends and aggregators (e.g. **AIOStreams**, **Omni**, web players) require direct HTTP streams and reject `magnet:` / `infoHash` responses. This project runs a lightweight Go daemon *on the device itself* that:

1. Exposes a **Stremio-compatible addon** on `127.0.0.1:8080`.
2. Queries **Torrentio** with residential browser headers, filtering out incompatible/unplayable releases (CAM/TS, massive REMUXes, lossless TrueHD/DTS-HD audio, and non-HDR10 `DV.P5` files) and scoring candidates for Fire TV hardware decoding.
3. Exposes **two clean Auto-Best stream options** (`⚡ 4K Auto-Best` and `⚡ 1080p Auto-Best`) so aggregators like AIOStreams never bury the fastest swarm.
4. **Pre-warms** top candidates in the background as soon as the user opens a title's detail screen (`GET /stream/…`) and runs a real-time **Swarm Race** across the top 4 candidates when Play is pressed, locking onto whichever swarm delivers the initial video piece fastest.
5. Downloads sequentially in **RAM only** (`MemStorage` ring buffer capped at 64–100 MiB) and immediately drops non-selected torrents so **100% of RAM and network bandwidth** go to the active stream.
6. Serves video over HTTP with full **`Range: bytes=…`** (`206 Partial Content`) support for instant seeking in `mpv`, `ExoPlayer`, `VLC`, etc.

No external server, no Debrid subscription required, and **zero bytes are ever written to flash/eMMC**.

```text
 Player (Stremio / AIOStreams / Omni / mpv / ExoPlayer)
   │
   │ 1. GET /stream/movie/tt0111161.json
   │    ├─ Returns 2 clean options: [4K Auto-Best] & [1080p Auto-Best]
   │    └─ Pre-warms top 4K candidates in background (metadata + Piece 0)
   │
   │ 2. GET /play/adaptive?id=tt0111161&q=4k (Range: bytes=0-)
   │    ├─ Runs real-time Swarm Race across top 4 candidates (highest seeds + best HDR/DV score)
   │    ├─ First swarm to verify initial 512 KiB wins & pins for the session
   │    ├─ Losers are dropped immediately (100% RAM & bandwidth to winner)
   │    └─ Activates 48 MiB sequential readahead + pins MKV Cues / MP4 moov tail
   ▼
 ┌─────────────────────────────── APK / Daemon ───────────────────────────────┐
 │ Kotlin: BridgeService (foreground) ──ProcessBuilder──► libbridge.so        │
 │         BootReceiver · MainActivity (D-pad TV UI, live log, port selector) │
 │ Go Core: Scraper ──► Swarm Race ──► Engine (anacrolix) ──► MemStorage (RAM)│
 └────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Features & Streaming Optimizations

### 1. Clean 2-Option Mode (`4K Auto-Best` & `1080p Auto-Best`)
Instead of returning 100+ raw torrent links that aggregators like AIOStreams might deduplicate or reorder by fake tracker seed counts, `GET /stream/:type/:id.json` returns **two curated options**:
* **`P2P Auto-Adaptive (4K/1080p)`** (`/play/adaptive?id=<IMDB_ID>&q=4k`): Prioritizes 4K HDR10/DV/HEVC streams and automatically falls back to the fastest 1080p swarm only if all 4K swarms are genuinely dead.
* **`P2P Auto-Best (1080p)`** (`/play/adaptive?id=<IMDB_ID>&q=1080p`): Directly races and serves the fastest 1080p WEB-DL/BluRay stream.

Each option includes distinct `behaviorHints.filename` and `behaviorHints.bingeGroup` metadata so AIOStreams' deduplicator preserves both options cleanly.

### 2. Background Pre-Warming (`Engine.Warm`)
When a user opens a movie or episode detail page (`GET /stream/…`), the daemon typically has **3 to 10 seconds** while the user reads the synopsis and clicks Play. During this window, `Engine.Warm` fetches `.torrent` metadata (`GotInfo`) via DHT/UDP trackers and prioritizes **Piece 0** of the top 4K candidates in the background. When the user clicks Play, metadata is already in RAM (`0 ms` wait) and peers are already connected.

### 3. Real-Time Swarm Race & Diverse Lineup (`SelectRaceLineup`)
Tracker seed counts are frequently inflated or stale. `SelectRaceLineup` builds a 4-candidate race lineup interleaving:
* The **highest-seeded** 4K releases (e.g. high-availability `WEB-DL H.265` scene releases), and
* The **highest-scored** 4K `HDR10` / `Dolby Vision` / `DDP 5.1` releases.

All 4 candidates race in parallel in **Fast-Start Probe Mode** (requesting *only* Piece 0 so bandwidth is never wasted on future/tail pieces during the race). The first candidate to deliver the initial `512 KiB` wins immediately.

### 4. Two-Phase Readahead & Resource Isolation (`DropOtherTorrents` + `promoteToFullStream`)
The instant a candidate wins the Swarm Race:
* **`DropOtherTorrents`** cancels and removes all losing torrents once their readers close, freeing 100% of the RAM cache and peer sockets for the winner.
* **`promoteToFullStream`** expands the winning reader's sequential readahead window to **75% of the RAM budget (48 MiB on a 64 MiB cache)** and pins the container index at EOF (**2 MiB** for `.mkv` `Cues` or **12 MiB** for `.mp4` `moov` atoms) so player seeks to the end of the file resolve in milliseconds.
* **Per-Session Pinning & Race Serialization (`getRaceLock`)**: Concurrent HTTP probes from Stremio/`mpv`/`ExoPlayer` share the same race via a per-key mutex, and all subsequent `Range: bytes=…` requests stay locked to the winning torrent for the entire playback session.

### 5. Fire TV Hardware Compatibility, Language & Elastic Filtering (`Scraper`)
* **Excludes** `CAM`/`TS`/`Screener` junk, `REMUX`/`BDREMUX` bitrates (>40 Mbps), lossless `TrueHD`/`DTS-HD MA` audio (which causes audio dropouts or software transcoding stalls on Fire Stick), and files over size caps (`<= 22 GiB` for adaptive 4K movies, `<= 15 GiB` for 1080p movies, `<= 4 GiB` for series episodes).
* **Penalizes** Dolby Vision Profile 5 (`DV.P5`) streams lacking an `HDR10` fallback layer and `.mp4` containers with end-of-file `moov` atoms in favor of fast-start `.mkv` `WEB-DL`/`WEBRip` `HEVC` streams with `DDP 5.1` / `EAC3` / `AAC` audio.
* **Audio Language Priority & Foreign Dub Protection (`IsForeignDubOnly`)**: Rewards `MULTi` / `DUAL` / `LATINO` / `SPA` / `ENG` releases (`+75` score bonus) and demotes exclusive foreign dubs (`RUS`, `TRUEFRENCH`/`VFF` without `MULTi`, `ITA`, `GERMAN`, `UKR`, `HINDI`) so a high-seed Russian/French-only dub never wins the Swarm Race over English/Spanish/Multi audio tracks.
* **Elastic Seed Threshold (`ElasticMinSeeds`)**: Enforces strict seed floors (`>= 30` for 4K, `>= 25` for 1080p, `>= 15` global) on blockbusters, and automatically relaxes down to `>= 5` seeds when a classic, indie, or niche title has no candidates meeting the strict floor.
* **Multi-Indexer Failover & 4K Enrichment (`DefaultFallbackEndpoints`)**: Rotates across Torrentio and backup Stremio indexers (`KnightCrawler`, `Comet`, `MediaFusion`) with a 3s timeout per endpoint, and merges 4K results from backup indexers when the primary indexer returns only 1080p.
* **Next-Episode Binge Auto-Prewarm (`maybePrewarmNextEpisode`)**: When streaming a series episode (`ttXXXX:S:E`) and playback reaches the 85%–97% window (end credits), automatically resolves and pre-warms `ttXXXX:S:(E+1)` in the background.
* **Device-Aware RAM Profile (`defaultCacheMB`)**: Defaults to `64 MiB` on Android/Fire OS (`48 MiB` forward readahead + `16 MiB` rewind cushion) and `100 MiB` on desktop macOS/Linux/Windows (`75 MiB` forward readahead + `25 MiB` instant-rewind cushion).

---

## Project Layout

| Path | Description |
|---|---|
| `core/cmd/daemon/main.go` | CLI entry point, flags, Android DNS fallback, quiet probe-cancel logger, graceful shutdown |
| `core/pkg/bridge/handlers.go` | HTTP endpoints (`/manifest.json`, `/stream/:type/:id.json`, `/play/adaptive`, `/play`, `/health`), Swarm Race & session pinning |
| `core/pkg/bridge/scraper.go` | Torrentio client, Fire Stick hardware scoring, `IsAdaptive4K`/`IsAdaptive1080p` classifiers, `SelectRaceLineup`, in-memory TTL cache |
| `core/pkg/torrent/engine.go` | `anacrolix/torrent` client tuned for low RAM, background `Warm`, safe reference-counted `DropOtherTorrents`/`DropHash`, multi-file episode picker |
| `core/pkg/torrent/memstorage.go` | Strict RAM-only `storage.ClientImpl` ring buffer with playhead-aware eviction (zero disk writes) |
| `core/pkg/torrent/stream_reader.go` | `PlayableStream` (`io.ReadSeekCloser`), `Prebuffer`, and `promoteToFullStream` |
| `core/build.sh` | Android NDK cross-compiler (`armeabi-v7a` & `arm64-v8a` -> `libbridge.so`) |
| `android/…` | Kotlin Android TV / Fire OS wrapper (`BridgeService`, `ProcessManager`, `BootReceiver`, `MainActivity`) |

---

## Build & Run

### Requirements
* **Go** ≥ 1.24
* **Android NDK** r26+ (for Android cross-compilation)
* **JDK 17** & **Android SDK 34** (for APK packaging)

### Makefile Targets
```bash
make run             # Run the Go daemon locally on 127.0.0.1:8080 (64 MiB RAM cache)
make test            # Run Go unit tests (bridge + torrent + MemStorage eviction)
make daemon          # Cross-compile libbridge.so for armeabi-v7a and arm64-v8a via Android NDK
make apk             # Build daemon + assemble Android debug APK
make install         # Install APK onto connected Fire TV / Android TV via adb
make daemon-static   # Alternative pure-Go static build (CGO_ENABLED=0)
```

### Local Testing
Start the daemon:
```bash
make run
```
Verify endpoints:
```bash
curl http://127.0.0.1:8080/manifest.json
curl http://127.0.0.1:8080/stream/movie/tt0111161.json
curl http://127.0.0.1:8080/health
```

---

## Daemon Flags

```text
-host 127.0.0.1        HTTP listen host (keep loopback)
-port 8080             HTTP listen port
-bt-port 42069         BitTorrent listen port (0 = random)
-cache-mb 64           RAM cache budget in MiB (clamped to 16..100)
-upload-kbps 256       Upload rate limit in KiB/s
-no-upload             Disable uploading to peers
-qualities ""          Optional comma-separated quality filter (e.g. 4k,1080p)
-torrentio-url "..."   Upstream Torrentio base URL (may include provider/filter config segment)
-dns auto              DNS resolver: 'auto' (public resolvers if /etc/resolv.conf is missing), 'system', or host:port list
-log-level info        Log level: debug | info | warn | error
-watch-stdin           Exit automatically when stdin closes (prevents orphan processes on Android)
```

---

## HTTP Endpoints

* `GET /manifest.json` — Stremio addon manifest (`id: p2p.http.bridge`, `resources: ["stream"]`, `types: ["movie", "series"]`, `idPrefixes: ["tt"]`).
* `GET /stream/:type/:id.json` — Resolves streams for a movie (`tt1757678`) or series episode (`tt0944947:1:1`), pre-warms the top 4K race lineup in the background, and returns the 2 clean Auto-Best options (`4K Auto-Best` and `1080p Auto-Best`).
* `GET /play/adaptive?id=<IMDB_ID>&q=4k|1080p` — Runs the real-time Swarm Race (or serves the already-pinned session winner), supporting `Range: bytes=…`, `206 Partial Content`, `416`, and `HEAD`. Sets `X-Adaptive-Selected: 4K|1080p`.
* `GET /play?hash=<INFOHASH>[&idx=N][&fb=<FALLBACK_HASH>]` — Direct stream endpoint by infohash with optional fallback hash.
* `GET /health` — JSON status with uptime, goroutines, Go heap metrics, RAM cache usage/evictions, and active torrent peer/seed counts.

---

## How RAM is Bounded (`MemStorage`)

* `MemStorage` implements `storage.ClientImpl` entirely in memory using recycled `[]byte` piece buffers. Nothing is ever written to disk.
* Advertises its capacity to `anacrolix/torrent` and allocates **75% of the budget (48 MiB at 64 MiB)** to the active stream's sequential readahead window once prebuffering succeeds.
* When resident bytes exceed the budget, verified pieces are evicted in strict priority order:
  1. Pieces far ahead of the readahead window (leftovers from seeks),
  2. Pieces behind the playhead (oldest/farthest first),
  3. Pieces inside the readahead window (last resort).
  In-flight pieces being hashed are never evicted.
* Only the winning torrent remains active during playback; losing race candidates are dropped immediately, and idle torrents are purged after 3 minutes of inactivity.
* Configured with `GOGC=50` and `debug.SetMemoryLimit((cacheMB + 160) << 20)` to keep the Go runtime footprint tight on 1–1.5 GB RAM devices.

---

## Legal & Educational Disclaimer

> [!WARNING]
> **Educational & Research Purpose Only**: This software is an experimental proof-of-concept demonstrating sequential BitTorrent data transfer directly in volatile memory (RAM) and exposing standard HTTP byte-range interfaces for media streaming.

1. **No Content Hosted or Distributed**: This project does not host, store, index, or distribute any media files or torrent content. It is a protocol translation utility (BitTorrent to HTTP).
2. **Third-Party Indexers & Scrapers**: Any scraping or metadata aggregation features connect to third-party endpoints configured by the user. The authors do not control, endorse, or promote any third-party indexing services or the material they index.
3. **P2P Nature & Intellectual Property**: The BitTorrent protocol operates via peer-to-peer data exchange (simultaneous downloading and uploading). Users are solely responsible for ensuring that their use of this software complies with all local, national, and international copyright laws and regulations.
4. **Limitation of Liability**: The authors and contributors assume no liability for any misuse, copyright infringement, or damages resulting from the use or distribution of this software.

---

## License

This project is licensed under the [MIT License](LICENSE).

