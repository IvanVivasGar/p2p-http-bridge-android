package torrent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	anacrolix "github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/time/rate"
)

var (
	// ErrBadHash is returned for a malformed info hash.
	ErrBadHash = errors.New("invalid info hash (expected 40 hex characters)")
	// ErrMetadataTimeout is returned when no peer delivered the torrent metadata in time.
	ErrMetadataTimeout = errors.New("timed out waiting for torrent metadata (no peers?)")
	// ErrPrebufferTimeout is returned when initial video chunks did not arrive within the prebuffer window.
	ErrPrebufferTimeout = errors.New("timed out pre-buffering initial video data")
	// ErrInsufficientSpeed is returned when a swarm fails the minimum sustained throughput check.
	ErrInsufficientSpeed = errors.New("swarm speed below minimum sustained threshold")
	// ErrNoFile is returned when the torrent has no file that can be streamed.
	ErrNoFile = errors.New("torrent has no streamable file")
)

// Hint carries optional information from the scraper that helps to pick the right file/peers.
type Hint struct {
	Trackers []string
	FileIdx  int // -1 when unknown
	Season   int // 0 when unknown
	Episode  int // 0 when unknown
}

// Config tunes the engine. Zero values get sane defaults for a ~1 GB RAM device.
type Config struct {
	// CacheBytes is the hard cap for torrent data kept in RAM (clamped to 16..100 MiB).
	CacheBytes int64
	// ListenPort is the BitTorrent listen port (TCP+uTP/DHT UDP).
	ListenPort int
	// UploadKBps limits upload speed. 0 uses 256 KB/s. Ignored if NoUpload.
	UploadKBps int
	// NoUpload disables uploading entirely (leech only).
	NoUpload bool
	// MetadataTimeout is how long /play waits for the torrent metadata.
	MetadataTimeout time.Duration
	// PrebufferTimeout is the max wait time to fill the initial prebuffer before serving HTTP bytes.
	PrebufferTimeout time.Duration
	// PrebufferBytes is the minimum contiguous bytes to buffer before replying to a GET /play request.
	PrebufferBytes int64
	// IdleTimeout drops a torrent that has had no open streams for this long (at least 3 minutes).
	IdleTimeout time.Duration
	// Logger is used by the engine. LibLogger receives the (noisy) library logs.
	Logger    *slog.Logger
	LibLogger *slog.Logger

	// tweak, if set, can adjust the low level client config (used by hermetic tests).
	tweak func(*anacrolix.ClientConfig)
}

const (
	minCacheBytes     = 16 << 20
	maxCacheBytes     = 100 << 20
	defCacheBytes     = 64 << 20
	defPrebufferBytes = 1 << 20 // 1 MiB initial chunk before HTTP status (while 75% readahead fills in background)
	minIdleTimeout    = 3 * time.Minute
)

func (c *Config) applyDefaults() {
	switch {
	case c.CacheBytes <= 0:
		c.CacheBytes = defCacheBytes
	case c.CacheBytes < minCacheBytes:
		c.CacheBytes = minCacheBytes
	case c.CacheBytes > maxCacheBytes:
		c.CacheBytes = maxCacheBytes
	}
	if c.UploadKBps <= 0 {
		c.UploadKBps = 256
	}
	if c.MetadataTimeout <= 0 {
		c.MetadataTimeout = 45 * time.Second
	}
	if c.PrebufferTimeout <= 0 {
		c.PrebufferTimeout = 15 * time.Second
	}
	if c.PrebufferBytes <= 0 {
		c.PrebufferBytes = defPrebufferBytes
	}
	if c.IdleTimeout < minIdleTimeout {
		c.IdleTimeout = 3 * time.Minute
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.LibLogger == nil {
		c.LibLogger = c.Logger
	}
}

// defaultTrackers are appended to every magnet to speed up peer discovery (DHT is also enabled).
var defaultTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://open.tracker.cl:1337/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://exodus.desync.com:6969/announce",
	"udp://open.stealth.si:80/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
}

// Engine wraps an anacrolix/torrent client configured for low-memory sequential streaming.
type Engine struct {
	cfg    Config
	log    *slog.Logger
	client *anacrolix.Client
	store  *MemStorage

	mu       sync.Mutex
	sessions map[metainfo.Hash]*session
	hints    map[metainfo.Hash]Hint
	started  time.Time

	stop chan struct{}
	done sync.WaitGroup
}

// session holds a torrent whose lifecycle is decoupled from ephemeral HTTP probe requests.
type session struct {
	t           *anacrolix.Torrent
	ctx         context.Context
	cancel      context.CancelFunc
	refs        int  // total active holders (Open streams + background Warm goroutines)
	streamRefs  int  // active Open() player streams
	dropPending bool // true when DropOtherTorrents/DropHash requested destruction once refs == 0
	destroyOnce sync.Once
	lastUse     time.Time
	resetPri    sync.Once
	warmMu      sync.Mutex
}

func (s *session) destroy() {
	s.destroyOnce.Do(func() {
		s.cancel()
		s.t.Drop()
	})
}

// pinHeadPieces keeps the first ~6 MiB and tail pieces (MP4 moov atom / MKV cues) of the selected
// file prioritized on the torrent itself, so that even between ephemeral HTTP connections the swarm
// continues downloading the header and container index needed by the player.
func (s *session) pinHeadPieces(f *anacrolix.File, pieceLen int64, headBytes int64) {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	if pieceLen <= 0 || s.ctx.Err() != nil {
		return
	}
	headPieces := int((headBytes + pieceLen - 1) / pieceLen)
	if headPieces < 2 {
		headPieces = 2
	}
	begin := f.BeginPieceIndex()
	end := f.EndPieceIndex()
	for i := begin; i < end && i < begin+headPieces; i++ {
		if i == begin {
			s.t.Piece(i).SetPriority(anacrolix.PiecePriorityNow)
		} else {
			s.t.Piece(i).SetPriority(anacrolix.PiecePriorityNext)
		}
	}

	ext := strings.ToLower(path.Ext(f.DisplayPath()))
	tailBytes := int64(2 << 20) // 2 MiB for MKV/default
	if ext == ".mp4" || ext == ".m4v" || ext == ".mov" {
		tailBytes = 12 << 20 // 12 MiB for MP4 moov atom
	}
	tailPieces := int((tailBytes + pieceLen - 1) / pieceLen)
	if tailPieces < 1 {
		tailPieces = 1
	}
	startTail := end - tailPieces
	if startTail < begin {
		startTail = begin
	}
	for i := startTail; i < end; i++ {
		s.t.Piece(i).SetPriority(anacrolix.PiecePriorityHigh)
	}
}

// unpinPieces resets piece priorities on a torrent.
func (s *session) unpinPieces() {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	if s.ctx.Err() != nil || s.t.Info() == nil {
		return
	}
	for _, f := range s.t.Files() {
		f.SetPriority(anacrolix.PiecePriorityNone)
	}
}

// NewEngine creates the BitTorrent client with an in-RAM storage.
func NewEngine(cfg Config) (*Engine, error) {
	cfg.applyDefaults()

	store := NewMemStorage(cfg.CacheBytes)

	cc := anacrolix.NewDefaultClientConfig()
	cc.DefaultStorage = store // RAM only: nothing is ever persisted to flash/eMMC
	cc.ListenPort = cfg.ListenPort
	cc.Seed = false
	cc.NoUpload = cfg.NoUpload
	cc.UploadRateLimiter = rate.NewLimiter(rate.Limit(cfg.UploadKBps*1024), 1<<20)
	cc.NoDefaultPortForwarding = true // no UPnP chatter; outgoing connections are enough
	cc.DisableWebtorrent = true       // drops the heavy pion/webrtc code paths at runtime
	cc.EstablishedConnsPerTorrent = 65
	cc.HalfOpenConnsPerTorrent = 30
	cc.TotalHalfOpenConns = 60
	cc.TorrentPeersHighWater = 250
	cc.MaxUnverifiedBytes = cfg.CacheBytes / 3
	cc.Slogger = cfg.LibLogger
	if cfg.tweak != nil {
		cfg.tweak(cc)
	}

	client, err := anacrolix.NewClient(cc)
	if err != nil {
		return nil, fmt.Errorf("creating torrent client: %w", err)
	}

	e := &Engine{
		cfg:      cfg,
		log:      cfg.Logger,
		client:   client,
		store:    store,
		sessions: make(map[metainfo.Hash]*session),
		hints:    make(map[metainfo.Hash]Hint),
		started:  time.Now(),
		stop:     make(chan struct{}),
	}

	// When a piece leaves RAM, make the client re-read its completion state so that it is
	// re-downloaded if a reader still needs it.
	store.SetOnEvict(func(ih metainfo.Hash, piece int) {
		e.mu.Lock()
		s := e.sessions[ih]
		e.mu.Unlock()
		if s != nil && s.ctx.Err() == nil && s.t.Info() != nil {
			s.t.Piece(piece).UpdateCompletion()
		}
	})

	e.done.Add(1)
	go e.janitor()
	return e, nil
}

// Close shuts the client down and releases all memory.
func (e *Engine) Close() {
	close(e.stop)
	e.done.Wait()
	e.mu.Lock()
	for ih, s := range e.sessions {
		s.destroy()
		delete(e.sessions, ih)
	}
	e.mu.Unlock()
	e.client.Close()
}

// CacheBytes returns the effective RAM cap.
func (e *Engine) CacheBytes() int64 { return e.store.Budget() }

// PrebufferConfig returns the configured prebuffer byte size and timeout.
func (e *Engine) PrebufferConfig() (int64, time.Duration) {
	return e.cfg.PrebufferBytes, e.cfg.PrebufferTimeout
}

// SetHint stores scraper information for a hash so that /play can pick the right file.
func (e *Engine) SetHint(hash string, h Hint) {
	var ih metainfo.Hash
	if err := ih.FromHexString(strings.ToLower(hash)); err != nil {
		return
	}
	e.mu.Lock()
	if len(e.hints) > 512 { // bounded memory
		e.hints = make(map[metainfo.Hash]Hint)
	}
	e.hints[ih] = h
	e.mu.Unlock()
}

// DropOtherTorrents immediately cancels any torrent session EXCEPT keepHash and schedules its
// removal once all open Readers have closed, freeing 100% of the RAM cache and network bandwidth
// for the winning stream without racing against anacrolix.Reader.Read().
func (e *Engine) DropOtherTorrents(keepHash string) {
	e.DropExceptHashes([]string{keepHash})
}

// DropExceptHashes immediately cancels and drops all torrent sessions not present in keepHashes.
// Called when the user opens a new movie/episode stream list or when a Swarm Race winner is chosen,
// ensuring previous movies never linger in RAM or block background pre-warming.
func (e *Engine) DropExceptHashes(keepHashes []string) {
	keepSet := make(map[metainfo.Hash]bool, len(keepHashes))
	for _, h := range keepHashes {
		var ih metainfo.Hash
		if err := ih.FromHexString(strings.ToLower(strings.TrimSpace(h))); err == nil {
			keepSet[ih] = true
		}
	}
	var toDestroy []*session
	e.mu.Lock()
	for other, s := range e.sessions {
		if !keepSet[other] {
			e.log.Info("dropping non-selected torrent to free RAM & bandwidth", "hash", other.HexString())
			delete(e.sessions, other)
			s.dropPending = true
			s.cancel() // immediately unblocks any waiting Reader or Warm goroutine from previous movie
			if s.refs <= 0 {
				toDestroy = append(toDestroy, s)
			}
		}
	}
	e.mu.Unlock()
	for _, s := range toDestroy {
		s.destroy()
	}
}

// DropHash stops and removes a specific torrent session once all open Readers on it have closed.
func (e *Engine) DropHash(hash string) {
	var ih metainfo.Hash
	if err := ih.FromHexString(strings.ToLower(strings.TrimSpace(hash))); err != nil {
		return
	}
	var destroyNow bool
	e.mu.Lock()
	s := e.sessions[ih]
	if s != nil {
		delete(e.sessions, ih)
		s.dropPending = true
		s.cancel()
		destroyNow = s.refs <= 0
	}
	e.mu.Unlock()
	if s != nil && destroyNow {
		s.destroy()
	}
}

// Deprioritize pauses piece requests for a torrent without destroying the BitTorrent client.
func (e *Engine) Deprioritize(hash string) {
	var ih metainfo.Hash
	if err := ih.FromHexString(strings.ToLower(strings.TrimSpace(hash))); err != nil {
		return
	}
	e.mu.Lock()
	s := e.sessions[ih]
	e.mu.Unlock()
	if s != nil {
		s.unpinPieces()
	}
}

// Warm starts fetching torrent metadata and pinning Piece 0 of the selected video file in the
// background as soon as the user opens the stream list (`GET /stream/...`). By the time the user
// clicks Play (3-10s later), metadata is already in RAM and peers are already connected.
func (e *Engine) Warm(hash string, fileIdx int) {
	var ih metainfo.Hash
	if err := ih.FromHexString(strings.ToLower(strings.TrimSpace(hash))); err != nil {
		return
	}
	e.mu.Lock()
	if existing := e.sessions[ih]; existing != nil && existing.streamRefs > 0 {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	go func() {
		s, err := e.acquireSession(ih, false)
		if err != nil {
			return
		}
		defer e.releaseSession(s, false)

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		select {
		case <-s.t.GotInfo():
		case <-s.ctx.Done():
			return
		case <-timer.C:
			return
		}

		if s.ctx.Err() != nil {
			return
		}

		s.resetPri.Do(func() {
			if s.ctx.Err() != nil {
				return
			}
			for _, f := range s.t.Files() {
				f.SetPriority(anacrolix.PiecePriorityNone)
			}
		})

		e.mu.Lock()
		hint := e.hints[ih]
		e.mu.Unlock()
		if fileIdx < 0 {
			fileIdx = hint.FileIdx
		}
		file, err := pickFile(s.t.Files(), fileIdx, hint)
		if err != nil || s.ctx.Err() != nil {
			return
		}
		// Pin only Piece 0 during background warm so peer connections focus 100% on the first piece.
		begin := file.BeginPieceIndex()
		end := file.EndPieceIndex()
		if begin < end && s.ctx.Err() == nil {
			s.t.Piece(begin).SetPriority(anacrolix.PiecePriorityNow)
		}
		e.log.Info("pre-warmed candidate torrent in background", "hash", ih.HexString(), "file", path.Base(file.DisplayPath()))
	}()
}

// acquire returns (creating if needed) the session for ih and takes a player stream reference on it.
func (e *Engine) acquire(ih metainfo.Hash) (*session, error) {
	return e.acquireSession(ih, true)
}

func (e *Engine) acquireSession(ih metainfo.Hash, isStream bool) (*session, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for other, s := range e.sessions {
		if other != ih && s.refs == 0 && time.Since(s.lastUse) > e.cfg.IdleTimeout {
			e.log.Info("dropping expired idle torrent", "hash", other.HexString())
			s.destroy()
			delete(e.sessions, other)
		}
	}

	s := e.sessions[ih]
	if s == nil {
		hint := e.hints[ih]
		t, err := e.client.AddMagnet(buildMagnet(ih, hint.Trackers))
		if err != nil {
			return nil, fmt.Errorf("adding torrent: %w", err)
		}
		sessCtx, cancel := context.WithCancel(context.Background())
		s = &session{
			t:      t,
			ctx:    sessCtx,
			cancel: cancel,
		}
		e.sessions[ih] = s
		e.log.Info("torrent session created", "hash", ih.HexString())
	} else if hint, ok := e.hints[ih]; ok && len(hint.Trackers) > 0 {
		s.t.AddTrackers([][]string{hint.Trackers})
	}
	s.refs++
	if isStream {
		s.streamRefs++
	}
	s.lastUse = time.Now()
	return s, nil
}

// release decrements the active stream reference count and destroys the session if a drop was pending.
func (e *Engine) release(s *session) {
	e.releaseSession(s, true)
}

func (e *Engine) releaseSession(s *session, isStream bool) {
	var shouldDestroy bool
	e.mu.Lock()
	if s.refs > 0 {
		s.refs--
	}
	if isStream && s.streamRefs > 0 {
		s.streamRefs--
	}
	s.lastUse = time.Now()
	if s.refs == 0 && s.dropPending {
		shouldDestroy = true
	}
	e.mu.Unlock()
	if shouldDestroy {
		s.destroy()
	}
}

func buildMagnet(ih metainfo.Hash, extra []string) string {
	var b strings.Builder
	b.WriteString("magnet:?xt=urn:btih:")
	b.WriteString(ih.HexString())
	seen := map[string]bool{}
	for _, tr := range append(append([]string{}, extra...), defaultTrackers...) {
		if tr == "" || seen[tr] {
			continue
		}
		seen[tr] = true
		b.WriteString("&tr=")
		b.WriteString(url.QueryEscape(tr))
	}
	return b.String()
}

// Open prepares a sequential, seekable stream for the selected video file of the torrent.
func (e *Engine) Open(reqCtx context.Context, hash string, fileIdx int) (*Stream, error) {
	var ih metainfo.Hash
	if err := ih.FromHexString(strings.ToLower(strings.TrimSpace(hash))); err != nil {
		return nil, ErrBadHash
	}
	s, err := e.acquire(ih)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			e.release(s)
		}
	}()

	timer := time.NewTimer(e.cfg.MetadataTimeout)
	defer timer.Stop()
	select {
	case <-s.t.GotInfo():
	case <-reqCtx.Done():
		return nil, reqCtx.Err()
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case <-timer.C:
		return nil, ErrMetadataTimeout
	}

	s.resetPri.Do(func() {
		for _, f := range s.t.Files() {
			f.SetPriority(anacrolix.PiecePriorityNone)
		}
	})

	e.mu.Lock()
	hint := e.hints[ih]
	e.mu.Unlock()
	if fileIdx < 0 {
		fileIdx = hint.FileIdx
	}
	file, err := pickFile(s.t.Files(), fileIdx, hint)
	if err != nil {
		return nil, err
	}

	pieceLen := s.t.Info().PieceLength

	// During initial prebuffering (and Swarm Race probing), focus 100% of peer bandwidth on Piece 0
	// rather than splitting bandwidth across 48 MiB readahead + 12 MiB tail pieces across multiple
	// racing torrents. As soon as Prebuffer completes on the winner, promoteToFullStream() activates
	// the full 48 MiB readahead and pins the tail index pieces (MP4 moov / MKV cues).
	begin := file.BeginPieceIndex()
	end := file.EndPieceIndex()
	if begin < end {
		s.t.Piece(begin).SetPriority(anacrolix.PiecePriorityNow)
	}

	// Full readahead (75% of RAM budget, e.g. 48 MiB on 64 MiB cache) activated on the winner.
	readahead := (e.store.Budget() * 3) / 4
	if readahead < 3*pieceLen {
		readahead = 3 * pieceLen
	}
	probeReadahead := max(e.cfg.PrebufferBytes*2, pieceLen)

	readerCtx, readerCancel := context.WithCancel(s.ctx)
	r := file.NewReader()
	r.SetContext(readerCtx)
	r.SetReadahead(probeReadahead)
	r.SetResponsive()

	st := &Stream{
		Name:      path.Base(file.DisplayPath()),
		Size:      file.Length(),
		eng:       e,
		sess:      s,
		file:      file,
		ih:        ih,
		r:         r,
		reqCtx:    reqCtx,
		rCtx:      readerCtx,
		rCancel:   readerCancel,
		base:      file.Offset(),
		pieceLen:  pieceLen,
		raBytes:   readahead,
		raPieces:  int(readahead / pieceLen),
		lastPiece: -1,
	}
	ok = true
	return st, nil
}

var videoExts = map[string]bool{
	".mkv": true, ".mp4": true, ".m4v": true, ".avi": true, ".mov": true, ".webm": true,
	".ts": true, ".m2ts": true, ".wmv": true, ".mpg": true, ".mpeg": true, ".flv": true,
}

func pickFile(files []*anacrolix.File, idx int, hint Hint) (*anacrolix.File, error) {
	if len(files) == 0 {
		return nil, ErrNoFile
	}
	if idx >= 0 && idx < len(files) {
		return files[idx], nil
	}
	var videos []*anacrolix.File
	for _, f := range files {
		if videoExts[strings.ToLower(path.Ext(f.DisplayPath()))] {
			videos = append(videos, f)
		}
	}
	if len(videos) == 0 {
		videos = files
	}
	if hint.Season > 0 && hint.Episode > 0 {
		re := regexp.MustCompile(fmt.Sprintf(`(?i)(?:s0*%d[ ._-]*e0*%d(?:\D|$)|(?:^|\D)0*%dx0*%d(?:\D|$))`,
			hint.Season, hint.Episode, hint.Season, hint.Episode))
		var best *anacrolix.File
		for _, f := range videos {
			if re.MatchString(f.DisplayPath()) && (best == nil || f.Length() > best.Length()) {
				best = f
			}
		}
		if best != nil {
			return best, nil
		}
	}
	best := videos[0]
	for _, f := range videos[1:] {
		if f.Length() > best.Length() {
			best = f
		}
	}
	return best, nil
}

// janitor drops torrents that have been idle for longer than IdleTimeout (default 3 min).
func (e *Engine) janitor() {
	defer e.done.Done()
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-tick.C:
			e.mu.Lock()
			for ih, s := range e.sessions {
				if s.refs == 0 && time.Since(s.lastUse) > e.cfg.IdleTimeout {
					e.log.Info("dropping idle torrent after inactivity timeout",
						"hash", ih.HexString(), "idleSec", int(time.Since(s.lastUse).Seconds()))
					s.destroy()
					delete(e.sessions, ih)
				}
			}
			e.mu.Unlock()
		}
	}
}

// TorrentStatus is a snapshot of an active torrent for /health.
type TorrentStatus struct {
	InfoHash       string `json:"infoHash"`
	Name           string `json:"name"`
	HasMetadata    bool   `json:"hasMetadata"`
	Streams        int    `json:"activeStreams"`
	PiecesTotal    int    `json:"piecesTotal"`
	PiecesComplete int    `json:"piecesResident"`
	PieceLengthKB  int64  `json:"pieceLengthKB"`
	Peers          int    `json:"activePeers"`
	Seeds          int    `json:"connectedSeeds"`
	PendingPeers   int    `json:"pendingPeers"`
	BytesDownload  int64  `json:"bytesDownloaded"`
}

// Status describes the engine state.
type Status struct {
	UptimeSec     int64           `json:"uptimeSec"`
	CacheBudgetMB int64           `json:"cacheBudgetMB"`
	CacheUsedMB   float64         `json:"cacheUsedMB"`
	Evictions     int64           `json:"evictions"`
	Torrents      []TorrentStatus `json:"torrents"`
}

// Status returns a snapshot of the engine.
func (e *Engine) Status() Status {
	st := Status{
		UptimeSec:     int64(time.Since(e.started).Seconds()),
		CacheBudgetMB: e.store.Budget() >> 20,
		CacheUsedMB:   float64(e.store.Used()) / (1 << 20),
		Evictions:     e.store.Evictions(),
		Torrents:      []TorrentStatus{},
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for ih, s := range e.sessions {
		ts := TorrentStatus{InfoHash: ih.HexString(), Streams: s.refs, Name: s.t.Name()}
		if info := s.t.Info(); info != nil {
			ts.HasMetadata = true
			ts.PiecesTotal = info.NumPieces()
			ts.PieceLengthKB = info.PieceLength >> 10
		}
		stats := s.t.Stats()
		ts.PiecesComplete = stats.PiecesComplete
		ts.Peers = stats.ActivePeers
		ts.Seeds = stats.ConnectedSeeders
		ts.PendingPeers = stats.PendingPeers
		ts.BytesDownload = stats.BytesReadUsefulData.Int64()
		st.Torrents = append(st.Torrents, ts)
	}
	return st
}

var _ = io.EOF
