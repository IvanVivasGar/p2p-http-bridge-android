package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ivanvivas/p2p-http-bridge-android/core/pkg/torrent"
)

// ManifestID is the Stremio addon id.
const ManifestID = "p2p.http.bridge"

// Adaptive Swarm-Race defaults: race the top 4 candidates (interleaving highest-seeded + best-scored)
// in parallel and pick whichever delivers the first 512 KiB of real video data fastest.
const (
	DefaultAdaptive4KWindow   = 16 * time.Second
	DefaultAdaptiveProbeBytes = 512 << 10       // 512 KiB initial race check
	DefaultAdaptivePinTTL     = 3 * time.Minute // matches torrent session idle retention
	maxRaceCandidates         = 4
)

// StreamOpener abstracts torrent.Engine.Open so adaptive fallback can be unit-tested deterministically.
type StreamOpener func(ctx context.Context, hash string, fileIdx int) (torrent.PlayableStream, error)

// pinnedSession locks the winning Candidate for an IMDb ID once the first HTTP 200/206 response
// is committed, preventing mid-playback codec/container desynchronization across Range requests.
type pinnedSession struct {
	cand    Candidate
	updated time.Time
}

// Server exposes the Stremio addon endpoints and the streaming endpoints.
type Server struct {
	eng     *torrent.Engine
	scraper *Scraper
	log     *slog.Logger
	baseURL string // public base of this daemon, e.g. http://127.0.0.1:8080
	version string
	started time.Time

	// Configurable hooks for adaptive streaming and testing.
	openStream         StreamOpener
	adaptive4KWindow   time.Duration
	adaptiveProbeBytes int64
	adaptivePinTTL     time.Duration

	pinMu       sync.Mutex
	pinned      map[string]pinnedSession
	raceLocks   map[string]*sync.Mutex
	bingeWarmed map[string]time.Time
}

// NewServer wires the HTTP handlers. baseURL is used to build the /play URLs handed to players.
func NewServer(eng *torrent.Engine, scraper *Scraper, log *slog.Logger, baseURL, version string) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		eng:                eng,
		scraper:            scraper,
		log:                log,
		baseURL:            strings.TrimRight(baseURL, "/"),
		version:            version,
		started:            time.Now(),
		adaptive4KWindow:   DefaultAdaptive4KWindow,
		adaptiveProbeBytes: DefaultAdaptiveProbeBytes,
		adaptivePinTTL:     DefaultAdaptivePinTTL,
		pinned:             make(map[string]pinnedSession),
		raceLocks:          make(map[string]*sync.Mutex),
		bingeWarmed:        make(map[string]time.Time),
	}
	if eng != nil {
		s.openStream = func(ctx context.Context, hash string, fileIdx int) (torrent.PlayableStream, error) {
			return eng.Open(ctx, hash, fileIdx)
		}
	}
	return s
}

// Handler returns the routed http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /manifest.json", s.handleManifest)
	mux.HandleFunc("GET /stream/{type}/{id}", s.handleStream)
	mux.HandleFunc("GET /play/adaptive", s.handlePlayAdaptive) // GET also routes HEAD
	mux.HandleFunc("GET /play", s.handlePlay)                  // GET also routes HEAD
	mux.HandleFunc("GET /health", s.handleHealth)
	return cors(mux)
}

// cors lets Stremio web / any web player talk to the local daemon.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Range, Content-Type, Accept")
		h.Set("Access-Control-Expose-Headers", "Content-Range, Accept-Ranges, Content-Length, X-Adaptive-Selected")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------- /manifest.json

func (s *Server) handleManifest(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          ManifestID,
		"version":     "1.0.0",
		"name":        "P2P HTTP Bridge",
		"description": "Local BitTorrent-to-HTTP streaming bridge (RAM-only, sequential, Range capable).",
		"resources":   []string{"stream"},
		"types":       []string{"movie", "series"},
		"idPrefixes":  []string{"tt"},
		"catalogs":    []any{},
	})
}

// ---------------------------------------------------------------- /stream/:type/:id.json

type stremioStream struct {
	Name          string         `json:"name"`
	Title         string         `json:"title"`
	URL           string         `json:"url"`
	BehaviorHints map[string]any `json:"behaviorHints,omitempty"`
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	typ := r.PathValue("type")
	id, ok := strings.CutSuffix(r.PathValue("id"), ".json")
	if !ok {
		http.NotFound(w, r)
		return
	}
	empty := map[string]any{"streams": []stremioStream{}}

	if err := ValidateRequest(typ, id); err != nil {
		s.log.Warn("bad stream request", "type", typ, "id", id, "err", err)
		w.Header().Set("X-Bridge-Error", err.Error())
		writeJSON(w, http.StatusBadRequest, empty)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	pool, err := s.scraper.ClassifyStreams(ctx, typ, id)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		s.log.Error("torrentio lookup failed", "type", typ, "id", id, "err", err)
		w.Header().Set("X-Bridge-Error", err.Error())
		writeJSON(w, http.StatusOK, empty)
		return
	}
	if len(pool.AllManual) == 0 {
		s.log.Info("no playable torrents", "type", typ, "id", id)
		writeJSON(w, http.StatusOK, empty)
		return
	}

	s.registerHints(typ, id, pool.AllManual)

	// Pre-warm the 4K race lineup in the background while the user is looking at the Stremio menu,
	// dropping any previous movie's torrents immediately so 100% of RAM & peer slots are free.
	if s.eng != nil && len(pool.Candidates4K) > 0 {
		lineup := SelectRaceLineup(pool.Candidates4K, maxRaceCandidates)
		keep := make([]string, 0, len(lineup))
		for _, c := range lineup {
			keep = append(keep, c.InfoHash)
		}
		s.eng.DropExceptHashes(keep)
		for _, c := range lineup {
			s.eng.Warm(c.InfoHash, c.FileIdx)
		}
	}

	// Return ONLY the clean Auto-Best options (1 for 4K + 1 for 1080p) so the user doesn't have to
	// guess among 100+ torrents and AIOStreams cannot reorder a slow swarm to the top.
	// Clicking 4K or 1080p runs a real-time Swarm Race across the top candidates of that tier.
	streams := make([]stremioStream, 0, 2)

	if _, has4K := pool.Best4K(); has4K {
		adaptiveURL := fmt.Sprintf("%s/play/adaptive?id=%s&q=4k", s.baseURL, url.QueryEscape(id))
		adapName, adapTitle, adapHints := pool.BuildAdaptiveStreamMeta()
		streams = append(streams, stremioStream{
			Name:          adapName,
			Title:         adapTitle,
			URL:           adaptiveURL,
			BehaviorHints: adapHints,
		})
	}

	if name1080, title1080, hints1080, has1080 := pool.Build1080pStreamMeta(); has1080 {
		url1080 := fmt.Sprintf("%s/play/adaptive?id=%s&q=1080p", s.baseURL, url.QueryEscape(id))
		streams = append(streams, stremioStream{
			Name:          name1080,
			Title:         title1080,
			URL:           url1080,
			BehaviorHints: hints1080,
		})
	}

	// Fallback if neither strict 4K nor 1080p pool matched (e.g. only 480p/unknown exists).
	if len(streams) == 0 {
		c := pool.AllManual[0]
		playURL := fmt.Sprintf("%s/play?hash=%s", s.baseURL, c.InfoHash)
		if c.FileIdx >= 0 {
			playURL += "&idx=" + strconv.Itoa(c.FileIdx)
		}
		streams = append(streams, stremioStream{
			Name:          c.Name,
			Title:         c.Title,
			URL:           playURL,
			BehaviorHints: c.BehaviorHints,
		})
	}

	s.log.Info("streams resolved (clean 4K/1080p auto-best mode)", "id", id, "optionsReturned", len(streams),
		"pool4K", len(pool.Candidates4K), "pool1080p", len(pool.Candidates1080p))
	writeJSON(w, http.StatusOK, map[string]any{"streams": streams})
}

func (s *Server) registerHints(typ, id string, cands []Candidate) {
	if s.eng == nil {
		return
	}
	var season, episode int
	if typ == "series" {
		parts := strings.Split(id, ":")
		season, _ = strconv.Atoi(parts[1])
		episode, _ = strconv.Atoi(parts[2])
	}
	for _, c := range cands {
		s.eng.SetHint(c.InfoHash, torrent.Hint{
			Trackers: c.Trackers,
			FileIdx:  c.FileIdx,
			Season:   season,
			Episode:  episode,
		})
	}
}

// ---------------------------------------------------------------- /play/adaptive?id=<IMDB_ID>[&q=4k|1080p]

func (s *Server) getPinnedCandidate(key string) (Candidate, bool) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	p, ok := s.pinned[key]
	if !ok {
		return Candidate{}, false
	}
	if time.Since(p.updated) > s.adaptivePinTTL {
		delete(s.pinned, key)
		return Candidate{}, false
	}
	p.updated = time.Now()
	s.pinned[key] = p
	return p.cand, true
}

func (s *Server) setPinnedCandidate(key string, c Candidate) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	if len(s.pinned) > 64 {
		s.pinned = make(map[string]pinnedSession)
	}
	s.pinned[key] = pinnedSession{cand: c, updated: time.Now()}
}

func (s *Server) getRaceLock(key string) *sync.Mutex {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	if len(s.raceLocks) > 64 {
		s.raceLocks = make(map[string]*sync.Mutex)
	}
	mu, ok := s.raceLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		s.raceLocks[key] = mu
	}
	return mu
}

func (s *Server) handlePlayAdaptive(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id := strings.TrimSpace(q.Get("id"))
	typ := strings.TrimSpace(q.Get("type"))
	preferQ := strings.ToLower(strings.TrimSpace(q.Get("q")))
	if typ == "" {
		typ = InferTypeFromID(id)
	}
	if err := ValidateRequest(typ, id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.openStream == nil {
		http.Error(w, "stream engine unavailable", http.StatusServiceUnavailable)
		return
	}

	pinKey := id
	if preferQ != "" {
		pinKey = id + "#" + preferQ
	}

	// 1. Fast path: if a stream was already selected and locked for this playback session, keep
	// serving the exact same candidate across all Range requests.
	if locked, ok := s.getPinnedCandidate(pinKey); ok {
		w.Header().Set("X-Adaptive-Selected", locked.Quality)
		s.serveCandidate(w, r, locked)
		return
	}

	// Serialize the initial Swarm Race per pinKey so that if Stremio/mpv fires 2 concurrent HTTP
	// probes 100ms apart, the second probe waits for the first race to pin the winner instead of
	// launching a duplicate race on the same torrents.
	keyMu := s.getRaceLock(pinKey)
	keyMu.Lock()
	if locked, ok := s.getPinnedCandidate(pinKey); ok {
		keyMu.Unlock()
		w.Header().Set("X-Adaptive-Selected", locked.Quality)
		s.serveCandidate(w, r, locked)
		return
	}

	lookupCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	pool, err := s.scraper.ClassifyStreams(lookupCtx, typ, id)
	cancel()
	if err != nil {
		keyMu.Unlock()
		if errors.Is(err, context.Canceled) {
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if len(pool.AllManual) == 0 {
		keyMu.Unlock()
		http.Error(w, "no playable streams found", http.StatusNotFound)
		return
	}
	s.registerHints(typ, id, pool.AllManual)

	// 2. Unless the user explicitly asked for 1080p (`q=1080p`), race the top 4K candidates
	// (interleaving highest-seeded + best-scored) in parallel and pick whichever delivers Piece 0 first.
	if preferQ != "1080p" && len(pool.Candidates4K) > 0 {
		top4K := SelectRaceLineup(pool.Candidates4K, maxRaceCandidates)
		s.log.Info("running 4K swarm race", "id", id, "candidates", len(top4K))
		if winStream, winCand, ok := s.raceCandidates(r.Context(), top4K, s.adaptive4KWindow); ok {
			defer winStream.Close()
			if s.eng != nil {
				s.eng.DropOtherTorrents(winCand.InfoHash)
			}
			s.setPinnedCandidate(pinKey, winCand)
			keyMu.Unlock()
			w.Header().Set("X-Adaptive-Selected", "4K")
			s.log.Info("swarm race selected fastest 4K stream",
				"id", id, "hash", winCand.InfoHash, "file", winStream.FileName(), "seeds", winCand.Seeds)
			s.writeStreamResponse(w, r, winStream)
			return
		}
		if r.Context().Err() != nil {
			keyMu.Unlock()
			return
		}
	}

	// 3. Race the top 1080p candidates (either because q=1080p was chosen or 4K timed out).
	cands1080 := pool.Candidates1080p
	if len(cands1080) == 0 {
		cands1080 = pool.AllManual
	}
	cands1080 = SelectRaceLineup(cands1080, maxRaceCandidates)

	s.log.Info("running 1080p swarm race", "id", id, "candidates", len(cands1080))
	if winStream, winCand, ok := s.raceCandidates(r.Context(), cands1080, 15*time.Second); ok {
		defer winStream.Close()
		if s.eng != nil {
			s.eng.DropOtherTorrents(winCand.InfoHash)
		}
		s.setPinnedCandidate(pinKey, winCand)
		keyMu.Unlock()
		w.Header().Set("X-Adaptive-Selected", winCand.Quality)
		s.log.Info("swarm race selected fastest 1080p stream",
			"id", id, "hash", winCand.InfoHash, "file", winStream.FileName(), "seeds", winCand.Seeds)
		s.writeStreamResponse(w, r, winStream)
		return
	}

	// Final fallback: single open on top candidate.
	chosen := cands1080[0]
	st, err := s.openAndPrebuffer(r, chosen)
	if err != nil {
		keyMu.Unlock()
		s.handleOpenError(w, r, err, chosen.InfoHash)
		return
	}
	defer st.Close()
	if s.eng != nil {
		s.eng.DropOtherTorrents(chosen.InfoHash)
	}
	s.setPinnedCandidate(pinKey, chosen)
	keyMu.Unlock()
	w.Header().Set("X-Adaptive-Selected", chosen.Quality)
	s.writeStreamResponse(w, r, st)
}

type raceResult struct {
	cand   Candidate
	stream torrent.PlayableStream
}

// raceCandidates launches up to 4 candidate torrents concurrently, waits for the first one to
// deliver the initial pre-buffer (512 KiB), and immediately cancels & drops all losing candidates
// so 100% of RAM and bandwidth go to the winning swarm.
func (s *Server) raceCandidates(reqCtx context.Context, cands []Candidate, window time.Duration) (torrent.PlayableStream, Candidate, bool) {
	if len(cands) == 0 {
		return nil, Candidate{}, false
	}
	if len(cands) == 1 {
		st, ok := s.trySingleCandidate(reqCtx, cands[0], window)
		return st, cands[0], ok
	}

	raceCtx, cancelRace := context.WithCancel(reqCtx)
	defer cancelRace()

	winCh := make(chan raceResult, len(cands))
	var wg sync.WaitGroup

	for _, c := range cands {
		cand := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, ok := s.trySingleCandidate(raceCtx, cand, window)
			if !ok {
				return
			}
			winCh <- raceResult{cand: cand, stream: st}
		}()
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case win := <-winCh:
		// Cancel the other contenders and clean up any late arrivals in the background.
		cancelRace()
		go func(winnerHash string) {
			<-doneCh
			close(winCh)
			for loser := range winCh {
				_ = loser.stream.Close()
			}
			for _, c := range cands {
				if c.InfoHash != winnerHash && s.eng != nil {
					s.eng.DropHash(c.InfoHash)
				}
			}
		}(win.cand.InfoHash)
		return win.stream, win.cand, true

	case <-doneCh:
		// Check if a winner arrived right as the last goroutine finished.
		select {
		case win := <-winCh:
			return win.stream, win.cand, true
		default:
			for _, c := range cands {
				if s.eng != nil {
					s.eng.Deprioritize(c.InfoHash)
				}
			}
			return nil, Candidate{}, false
		}
	}
}

func (s *Server) trySingleCandidate(ctx context.Context, cand Candidate, window time.Duration) (torrent.PlayableStream, bool) {
	probeCtx, cancel := context.WithTimeout(ctx, window)
	defer cancel()

	start := time.Now()
	st, err := s.openStream(probeCtx, cand.InfoHash, cand.FileIdx)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Info("adaptive candidate open exceeded window",
				"quality", cand.Quality, "hash", cand.InfoHash, "elapsedMs", time.Since(start).Milliseconds(), "err", err)
		}
		return nil, false
	}

	remainingWindow := window - time.Since(start)
	if remainingWindow <= 0 {
		_ = st.Close()
		return nil, false
	}

	if err := st.Prebuffer(probeCtx, 0, s.adaptiveProbeBytes, remainingWindow); err != nil {
		_ = st.Close()
		if ctx.Err() == nil {
			s.log.Info("adaptive candidate prebuffer below threshold",
				"quality", cand.Quality, "hash", cand.InfoHash, "elapsedMs", time.Since(start).Milliseconds(), "err", err)
		}
		return nil, false
	}
	return st, true
}

// ---------------------------------------------------------------- /play?hash=

var contentTypes = map[string]string{
	".mkv": "video/x-matroska", ".mp4": "video/mp4", ".m4v": "video/mp4", ".webm": "video/webm",
	".avi": "video/x-msvideo", ".mov": "video/quicktime", ".ts": "video/mp2t", ".m2ts": "video/mp2t",
	".wmv": "video/x-ms-wmv", ".mpg": "video/mpeg", ".mpeg": "video/mpeg", ".flv": "video/x-flv",
}

func (s *Server) handlePlay(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	hash := strings.ToLower(strings.TrimSpace(q.Get("hash")))
	idx := -1
	if v := q.Get("idx"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, "invalid idx", http.StatusBadRequest)
			return
		}
		idx = n
	}

	if locked, ok := s.getPinnedCandidate(hash); ok {
		s.serveCandidate(w, r, locked)
		return
	}

	cand := Candidate{
		InfoHash: hash,
		FileIdx:  idx,
		Quality:  "4K",
	}

	fbHash := strings.ToLower(strings.TrimSpace(q.Get("fb")))
	if isValidInfoHash(fbHash) && fbHash != hash {
		fbIdx := -1
		if v := q.Get("fbidx"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				fbIdx = n
			}
		}
		if st4K, ok := s.trySingleCandidate(r.Context(), cand, s.adaptive4KWindow); ok {
			defer st4K.Close()
			if s.eng != nil {
				s.eng.DropOtherTorrents(cand.InfoHash)
			}
			s.setPinnedCandidate(hash, cand)
			w.Header().Set("X-Adaptive-Selected", "4K")
			s.writeStreamResponse(w, r, st4K)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		if s.eng != nil {
			s.eng.Deprioritize(hash)
		}
		fbCand := Candidate{InfoHash: fbHash, FileIdx: fbIdx, Quality: "1080p"}
		s.log.Info("4K stream slow, falling back to 1080p backup", "from4K", hash, "to1080p", fbHash)
		st1080, err := s.openAndPrebuffer(r, fbCand)
		if err != nil {
			s.handleOpenError(w, r, err, fbHash)
			return
		}
		defer st1080.Close()
		if s.eng != nil {
			s.eng.DropOtherTorrents(fbCand.InfoHash)
		}
		s.setPinnedCandidate(hash, fbCand)
		w.Header().Set("X-Adaptive-Selected", "1080p")
		s.writeStreamResponse(w, r, st1080)
		return
	}

	if s.eng != nil {
		s.eng.DropOtherTorrents(cand.InfoHash)
	}
	s.serveCandidate(w, r, cand)
}

func (s *Server) serveCandidate(w http.ResponseWriter, r *http.Request, cand Candidate) {
	if s.openStream == nil {
		http.Error(w, "stream engine unavailable", http.StatusServiceUnavailable)
		return
	}
	st, err := s.openAndPrebuffer(r, cand)
	if err != nil {
		s.handleOpenError(w, r, err, cand.InfoHash)
		return
	}
	defer st.Close()
	s.writeStreamResponse(w, r, st)
}

func (s *Server) openAndPrebuffer(r *http.Request, cand Candidate) (torrent.PlayableStream, error) {
	st, err := s.openStream(r.Context(), cand.InfoHash, cand.FileIdx)
	if err != nil {
		return nil, err
	}
	if r.Method != http.MethodHead {
		prebufBytes := int64(1 << 20)
		prebufTimeout := 15 * time.Second
		if s.eng != nil {
			prebufBytes, prebufTimeout = s.eng.PrebufferConfig()
		}
		startOff, wantLen := parseRangeWindow(r.Header.Get("Range"), st.FileSize(), prebufBytes)
		if err := st.Prebuffer(r.Context(), startOff, wantLen, prebufTimeout); err != nil {
			_ = st.Close()
			return nil, err
		}
	}
	return st, nil
}

func (s *Server) handleOpenError(w http.ResponseWriter, _ *http.Request, err error, hash string) {
	switch {
	case errors.Is(err, context.Canceled):
		s.log.Debug("client closed connection during open/prebuffer", "hash", hash)
	case errors.Is(err, torrent.ErrBadHash):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, torrent.ErrMetadataTimeout), errors.Is(err, torrent.ErrPrebufferTimeout):
		s.log.Warn("stream timeout waiting for swarm data", "hash", hash, "err", err)
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	case errors.Is(err, torrent.ErrNoFile):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		s.log.Error("play failed", "hash", hash, "err", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
}

func (s *Server) writeStreamResponse(w http.ResponseWriter, r *http.Request, st torrent.PlayableStream) {
	name := st.FileName()
	rangeHdr := r.Header.Get("Range")
	s.log.Info("serving stream", "method", r.Method, "file", name, "range", rangeHdr)

	// Binge-Watching Auto-Prewarm: when streaming a series episode and the player reaches the
	// 85%–97% window (end credits, avoiding the >=98% MKV/MP4 EOF index probe at startup),
	// pre-resolve and pre-warm the next episode in the background.
	if id := strings.TrimSpace(r.URL.Query().Get("id")); id != "" && st.FileSize() > 0 && rangeHdr != "" {
		startOff, _ := parseRangeWindow(rangeHdr, st.FileSize(), 0)
		s.maybePrewarmNextEpisode(id, startOff, st.FileSize())
	}

	ct := contentTypes[strings.ToLower(path.Ext(name))]
	if ct == "" {
		ct = mime.TypeByExtension(path.Ext(name))
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "no-store")

	http.ServeContent(w, r, name, time.Time{}, st)
}

// NextEpisodeID returns the next episode identifier ("tt0944947:1:2") for a series ID ("tt0944947:1:1").
func NextEpisodeID(id string) (string, bool) {
	parts := strings.Split(strings.TrimSpace(id), ":")
	if len(parts) != 3 || !strings.HasPrefix(parts[0], "tt") {
		return "", false
	}
	season, err1 := strconv.Atoi(parts[1])
	episode, err2 := strconv.Atoi(parts[2])
	if err1 != nil || err2 != nil || season <= 0 || episode <= 0 {
		return "", false
	}
	return fmt.Sprintf("%s:%d:%d", parts[0], season, episode+1), true
}

func (s *Server) maybePrewarmNextEpisode(id string, startOff, fileSize int64) {
	if s.scraper == nil || fileSize <= 0 {
		return
	}
	// Trigger only in the 85%..97% playback window (avoids >=98% container EOF probe at t=0s).
	if startOff < (fileSize*85)/100 || startOff >= (fileSize*98)/100 {
		return
	}
	nextID, ok := NextEpisodeID(id)
	if !ok {
		return
	}

	s.pinMu.Lock()
	if last, exists := s.bingeWarmed[nextID]; exists && time.Since(last) < 15*time.Minute {
		s.pinMu.Unlock()
		return
	}
	if len(s.bingeWarmed) > 64 {
		s.bingeWarmed = make(map[string]time.Time)
	}
	s.bingeWarmed[nextID] = time.Now()
	s.pinMu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		pool, err := s.scraper.ClassifyStreams(ctx, "series", nextID)
		if err != nil || len(pool.AllManual) == 0 {
			return
		}
		s.registerHints("series", nextID, pool.AllManual)
		if s.eng != nil {
			if top4K, ok := pool.Best4K(); ok {
				s.eng.Warm(top4K.InfoHash, top4K.FileIdx)
			} else if top1080, ok := pool.Best1080p(); ok {
				s.eng.Warm(top1080.InfoHash, top1080.FileIdx)
			}
		}
		s.log.Info("binge pre-warmed next episode", "current", id, "next", nextID,
			"pool4K", len(pool.Candidates4K), "pool1080p", len(pool.Candidates1080p))
	}()
}

func parseRangeWindow(rangeHdr string, fileSize, defaultPrebuf int64) (start int64, length int64) {
	const minProbeWarm = 256 << 10 // 256 KiB
	if !strings.HasPrefix(rangeHdr, "bytes=") {
		return 0, defaultPrebuf
	}
	spec := strings.TrimPrefix(rangeHdr, "bytes=")
	if i := strings.IndexByte(spec, ','); i >= 0 {
		spec = spec[:i]
	}
	parts := strings.SplitN(strings.TrimSpace(spec), "-", 2)
	if len(parts) != 2 {
		return 0, defaultPrebuf
	}
	if parts[0] == "" {
		if suffix, err := strconv.ParseInt(parts[1], 10, 64); err == nil && suffix > 0 {
			if suffix > fileSize {
				suffix = fileSize
			}
			return fileSize - suffix, min(suffix, defaultPrebuf)
		}
		return 0, defaultPrebuf
	}
	s, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || s < 0 || s >= fileSize {
		return 0, defaultPrebuf
	}
	if parts[1] == "" {
		if s > 0 && s > fileSize-(2<<20) {
			return s, min(fileSize-s, minProbeWarm)
		}
		return s, defaultPrebuf
	}
	e, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || e < s {
		return s, defaultPrebuf
	}
	span := e - s + 1
	if span < minProbeWarm {
		span = minProbeWarm
	}
	if span > defaultPrebuf {
		span = defaultPrebuf
	}
	return s, span
}

// ---------------------------------------------------------------- /health

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	status := s.eng.Status()
	seeds := 0
	for _, t := range status.Torrents {
		seeds += t.Seeds
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"version":    s.version,
		"listen":     s.baseURL,
		"uptimeSec":  int64(time.Since(s.started).Seconds()),
		"goroutines": runtime.NumGoroutine(),
		"memory": map[string]any{
			"goAllocMB":   float64(m.Alloc) / (1 << 20),
			"goHeapInuse": float64(m.HeapInuse) / (1 << 20),
			"goSysMB":     float64(m.Sys) / (1 << 20),
			"numGC":       m.NumGC,
		},
		"cache":        map[string]any{"budgetMB": status.CacheBudgetMB, "usedMB": status.CacheUsedMB, "evictions": status.Evictions},
		"seedsTotal":   seeds,
		"torrents":     status.Torrents,
		"engineUptime": status.UptimeSec,
	})
}
