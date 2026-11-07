// Command daemon is the P2P-to-HTTP streaming bridge: a Stremio-compatible addon that resolves
// torrent streams through an upstream Stremio addon (configured via -addon-url or P2P_ADDON_URL)
// and streams them sequentially (RAM only) over HTTP with Range support, listening on 127.0.0.1.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ivanvivas/p2p-http-bridge-android/core/pkg/bridge"
	"github.com/ivanvivas/p2p-http-bridge-android/core/pkg/torrent"
)

// version is overridden at build time: -ldflags "-X main.version=1.0.0".
var version = "dev"

// quietCancelHandler suppresses noisy ERROR logs from anacrolix/torrent's internal reader when a
// video player (mpv/ExoPlayer/AIOStreams) closes an HTTP probe connection (`context canceled`).
type quietCancelHandler struct {
	slog.Handler
}

func (h quietCancelHandler) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "read failed") || strings.Contains(r.Message, "error reading") {
		canceled := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "err" {
				val := a.Value.String()
				if strings.Contains(val, "context canceled") ||
					strings.Contains(val, "torrent closed") ||
					strings.Contains(val, "closed torrent") {
					canceled = true
					return false
				}
			}
			return true
		})
		if canceled {
			return nil
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h quietCancelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return quietCancelHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h quietCancelHandler) WithGroup(name string) slog.Handler {
	return quietCancelHandler{Handler: h.Handler.WithGroup(name)}
}

func defaultCacheMB() int {
	if runtime.GOOS == "android" {
		return 64 // Fire Stick / Android TV (48 MiB readahead + 16 MiB rewind cushion)
	}
	return 100 // Desktop Mac/Linux/Windows (75 MiB readahead + 25 MiB rewind cushion)
}

func main() {
	var (
		host       = flag.String("host", "127.0.0.1", "HTTP listen host (keep loopback unless you know what you do)")
		port       = flag.Int("port", 8080, "HTTP listen port")
		btPort     = flag.Int("bt-port", 42069, "BitTorrent listen port (0 = random)")
		cacheMB    = flag.Int("cache-mb", defaultCacheMB(), "RAM cache for torrent data in MiB (clamped to 16..100)")
		uploadKBps = flag.Int("upload-kbps", 256, "upload limit in KiB/s")
		noUpload   = flag.Bool("no-upload", false, "never upload to peers")
		addonURL   = flag.String("addon-url", os.Getenv(bridge.EnvAddonURL), "Upstream Stremio stream addon URL (or set P2P_ADDON_URL)")
		qualities  = flag.String("qualities", "", "optional comma-separated quality filter (e.g. 4k,1080p,720p); empty keeps all non-CAM qualities")
		dns        = flag.String("dns", "auto", "DNS: 'auto' (public resolvers if /etc/resolv.conf is missing), 'system', or comma separated host:port list")
		logLevel   = flag.String("log-level", "info", "debug|info|warn|error")
		watchStdin = flag.Bool("watch-stdin", false, "exit when stdin is closed (the Android wrapper keeps a pipe open; if the app dies the daemon dies too)")
		showVer    = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()
	if *showVer {
		fmt.Println(version)
		return
	}

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		lvl = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	// The torrent library is very chatty; only surface warnings and silence normal probe cancellations.
	baseLibHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: max(lvl, slog.LevelWarn)})
	libLog := slog.New(quietCancelHandler{Handler: baseLibHandler}).With("component", "torrent-lib")

	setupDNS(log, *dns)

	// Low-RAM tuning: collect garbage more eagerly and keep the Go heap near the cache budget
	// plus a fixed overhead for the client, DHT and buffers.
	debug.SetGCPercent(50)
	debug.SetMemoryLimit(int64(*cacheMB+160) << 20)

	cfg := torrent.Config{
		CacheBytes: int64(*cacheMB) << 20,
		ListenPort: *btPort,
		UploadKBps: *uploadKBps,
		NoUpload:   *noUpload,
		Logger:     log,
		LibLogger:  libLog,
	}
	eng, err := torrent.NewEngine(cfg)
	if err != nil && *btPort != 0 {
		log.Warn("bt port unavailable, retrying with a random port", "port", *btPort, "err", err)
		cfg.ListenPort = 0
		eng, err = torrent.NewEngine(cfg)
	}
	if err != nil {
		log.Error("cannot start torrent engine", "err", err)
		os.Exit(1)
	}
	defer eng.Close()

	var quals []string
	for _, q := range strings.Split(*qualities, ",") {
		if q = strings.TrimSpace(q); q != "" {
			quals = append(quals, q)
		}
	}
	scraper := bridge.NewScraper(bridge.ScraperConfig{BaseURL: *addonURL, Qualities: quals})

	addr := net.JoinHostPort(*host, fmt.Sprint(*port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("cannot listen", "addr", addr, "err", err)
		os.Exit(1)
	}
	srv := &http.Server{
		Handler:           bridge.NewServer(eng, scraper, log, "http://"+addr, version).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// No Read/WriteTimeout: streams can last for hours.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *watchStdin {
		go func() {
			_, _ = io.Copy(io.Discard, os.Stdin) // returns on EOF: the parent process is gone
			log.Warn("stdin closed: parent process is gone, exiting")
			stop()
		}()
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	log.Info("listening", "addr", addr, "version", version, "cacheMB", eng.CacheBytes()>>20)

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		shCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := srv.Shutdown(shCtx); err != nil {
			_ = srv.Close() // active streams are cut after the grace period
		}
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}
}

// setupDNS fixes name resolution on Android, where there is no /etc/resolv.conf and Go would
// otherwise query 127.0.0.1:53 and fail for every host (upstreams, trackers, DHT bootstrap).
func setupDNS(log *slog.Logger, mode string) {
	var servers []string
	switch mode {
	case "system":
		return
	case "auto":
		if _, err := os.Stat("/etc/resolv.conf"); err == nil {
			return
		}
		servers = []string{"1.1.1.1:53", "8.8.8.8:53", "9.9.9.9:53"}
	default:
		for _, s := range strings.Split(mode, ",") {
			if s = strings.TrimSpace(s); s != "" {
				if !strings.Contains(s, ":") {
					s += ":53"
				}
				servers = append(servers, s)
			}
		}
	}
	if len(servers) == 0 {
		return
	}
	log.Info("using custom DNS servers", "servers", servers)
	var next atomic.Uint32
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			start := int(next.Add(1))
			var lastErr error
			for i := range servers {
				c, err := dialer.DialContext(ctx, network, servers[(start+i)%len(servers)])
				if err == nil {
					return c, nil
				}
				lastErr = err
			}
			return nil, lastErr
		},
	}
}
