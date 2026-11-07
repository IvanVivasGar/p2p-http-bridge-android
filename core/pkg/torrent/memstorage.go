package torrent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// ErrPieceEvicted is returned by ReadAt when the piece data is no longer in RAM. The torrent
// client reacts to it by re-syncing the piece completion (see MemStorage.OnEvict) and
// re-downloading the piece if a reader still needs it.
var ErrPieceEvicted = errors.New("piece evicted from memory cache")

// minCachePieces is the minimum number of pieces the cache must be able to hold, regardless of the
// configured budget. Without it a torrent with huge pieces could never make progress.
const minCachePieces = 4

// freeListMax bounds the number of recycled piece buffers kept around to relieve the GC.
const freeListMax = 3

// MemStorage is a storage.ClientImpl that keeps torrent data exclusively in RAM, with a hard cap
// on resident piece bytes. NOTHING is ever written to flash/eMMC.
//
// It behaves like a sliding ring buffer around the playhead:
//   - The torrent client only schedules downloads that fit inside Capacity (the library honours the
//     capacity we advertise, prioritising the Reader's readahead window).
//   - When the resident bytes exceed the budget, verified pieces are evicted: first those far
//     ahead of the readahead window (left over from a seek), then those behind the playhead
//     (farthest first), and only as a last resort pieces inside the readahead window.
//   - Pieces that are still being downloaded/hashed are never evicted.
type MemStorage struct {
	mu       sync.Mutex
	budget   int64 // effective cap in bytes; may be raised to fit minCachePieces pieces
	used     int64 // resident bytes (allocated piece buffers)
	torrents map[metainfo.Hash]*memTorrent
	free     [][]byte

	capFn  func() (int64, bool)
	capPtr storage.TorrentCapacity

	// onEvict is invoked (in its own goroutine, without locks held) after a piece has been
	// dropped from RAM so the client can refresh its view of that piece.
	onEvict func(ih metainfo.Hash, piece int)

	evictions atomic.Int64
}

var _ storage.ClientImpl = (*MemStorage)(nil)

// NewMemStorage creates an in-memory storage capped at budgetBytes.
func NewMemStorage(budgetBytes int64) *MemStorage {
	s := &MemStorage{
		budget:   budgetBytes,
		torrents: make(map[metainfo.Hash]*memTorrent),
	}
	s.capFn = func() (int64, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.budget, true
	}
	s.capPtr = &s.capFn
	return s
}

// SetOnEvict registers the eviction callback. Must be called before torrents are added.
func (s *MemStorage) SetOnEvict(fn func(ih metainfo.Hash, piece int)) {
	s.mu.Lock()
	s.onEvict = fn
	s.mu.Unlock()
}

// Budget returns the effective capacity in bytes.
func (s *MemStorage) Budget() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.budget
}

// Used returns the number of resident piece bytes.
func (s *MemStorage) Used() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// Evictions returns how many pieces have been evicted so far.
func (s *MemStorage) Evictions() int64 { return s.evictions.Load() }

// SetPlayhead tells the storage where playback currently is, and how many pieces ahead are part
// of the readahead window. It drives the eviction order.
func (s *MemStorage) SetPlayhead(ih metainfo.Hash, piece, readaheadPieces int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.torrents[ih]; t != nil {
		t.playhead = piece
		t.readaheadPieces = readaheadPieces
	}
}

// OpenTorrent implements storage.ClientImpl.
func (s *MemStorage) OpenTorrent(_ context.Context, info *metainfo.Info, ih metainfo.Hash) (storage.TorrentImpl, error) {
	if info.PieceLength <= 0 {
		return storage.TorrentImpl{}, fmt.Errorf("invalid piece length %d", info.PieceLength)
	}
	s.mu.Lock()
	// The cache must be able to hold a few pieces or the torrent would stall.
	if need := int64(minCachePieces) * info.PieceLength; s.budget < need {
		s.budget = need
	}
	t := &memTorrent{
		s:      s,
		ih:     ih,
		info:   info,
		pieces: make([]*memPiece, info.NumPieces()),
	}
	s.torrents[ih] = t
	s.mu.Unlock()

	return storage.TorrentImpl{
		Piece: func(p metainfo.Piece) storage.PieceImpl {
			return t.piece(p.Index(), int(p.Length()))
		},
		Close:    func() error { t.close(); return nil },
		Capacity: s.capPtr,
	}, nil
}

// allocLocked returns a zeroed-length-n buffer, recycling when possible. s.mu must be held.
func (s *MemStorage) allocLocked(n int) []byte {
	for i := len(s.free) - 1; i >= 0; i-- {
		if cap(s.free[i]) >= n {
			b := s.free[i][:n]
			s.free = append(s.free[:i], s.free[i+1:]...)
			return b
		}
	}
	return make([]byte, n)
}

// releaseLocked returns a buffer to the free list (or drops it for the GC). s.mu must be held.
func (s *MemStorage) releaseLocked(b []byte) {
	if len(s.free) < freeListMax {
		s.free = append(s.free, b[:cap(b)])
	}
}

type evicted struct {
	ih    metainfo.Hash
	piece int
}

// evictLocked frees pieces until used <= budget. keep is never evicted. s.mu must be held.
func (s *MemStorage) evictLocked(keep *memPiece) (out []evicted) {
	for s.used > s.budget {
		victim := s.pickVictimLocked(keep)
		if victim == nil {
			return // everything resident is in-flight; tolerate a temporary overshoot
		}
		s.dropLocked(victim)
		out = append(out, evicted{victim.t.ih, victim.index})
	}
	return out
}

func (s *MemStorage) dropLocked(p *memPiece) {
	s.used -= int64(len(p.data))
	s.releaseLocked(p.data)
	p.data = nil
	p.complete = false
	s.evictions.Add(1)
}

func (s *MemStorage) pickVictimLocked(keep *memPiece) (best *memPiece) {
	var bestScore int64 = -1
	for _, t := range s.torrents {
		for _, p := range t.pieces {
			if p == nil || p == keep || p.data == nil || !p.complete {
				continue
			}
			if sc := t.evictionScore(p.index); sc > bestScore {
				bestScore, best = sc, p
			}
		}
	}
	return best
}

func (s *MemStorage) notify(ev []evicted) {
	if len(ev) == 0 {
		return
	}
	s.mu.Lock()
	fn := s.onEvict
	s.mu.Unlock()
	if fn == nil {
		return
	}
	go func() {
		for _, e := range ev {
			fn(e.ih, e.piece)
		}
	}()
}

type memTorrent struct {
	s    *MemStorage
	ih   metainfo.Hash
	info *metainfo.Info

	pieces          []*memPiece
	playhead        int
	readaheadPieces int
	closed          bool
}

// evictionScore: higher means "evict first".
//
//	class A (ahead of the readahead window, e.g. leftovers from a seek): 2e12 + distance
//	class B (behind the playhead):                                       1e12 + distance behind
//	class C (inside the readahead window):                               distance (last resort)
func (t *memTorrent) evictionScore(idx int) int64 {
	d := int64(idx - t.playhead)
	if d >= 0 {
		if d > int64(t.readaheadPieces) {
			return 2e12 + d
		}
		return d
	}
	return 1e12 - d // d is negative: farther behind => larger score
}

func (t *memTorrent) piece(idx, length int) *memPiece {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	p := t.pieces[idx]
	if p == nil {
		p = &memPiece{t: t, index: idx, length: length}
		t.pieces[idx] = p
	}
	return p
}

func (t *memTorrent) close() {
	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()
	t.closed = true
	for _, p := range t.pieces {
		if p != nil && p.data != nil {
			s.used -= int64(len(p.data))
			s.releaseLocked(p.data)
			p.data = nil
			p.complete = false
		}
	}
	delete(s.torrents, t.ih)
}

type memPiece struct {
	t        *memTorrent
	index    int
	length   int
	data     []byte // nil until first write / after eviction
	complete bool
}

var _ storage.PieceImpl = (*memPiece)(nil)

func (p *memPiece) ReadAt(b []byte, off int64) (int, error) {
	s := p.t.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.t.closed {
		return 0, io.EOF
	}
	if p.data == nil {
		return 0, ErrPieceEvicted
	}
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(b, p.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

func (p *memPiece) WriteAt(b []byte, off int64) (int, error) {
	s := p.t.s
	s.mu.Lock()
	if p.t.closed {
		s.mu.Unlock()
		return 0, errors.New("storage closed")
	}
	if off < 0 || off+int64(len(b)) > int64(p.length) {
		s.mu.Unlock()
		return 0, fmt.Errorf("write out of piece bounds: off=%d len=%d piece=%d", off, len(b), p.length)
	}
	if p.data == nil {
		p.data = s.allocLocked(p.length)
		s.used += int64(p.length)
	}
	n := copy(p.data[off:], b)
	ev := s.evictLocked(p)
	s.mu.Unlock()
	s.notify(ev)
	return n, nil
}

func (p *memPiece) MarkComplete() error {
	s := p.t.s
	s.mu.Lock()
	p.complete = p.data != nil
	ev := s.evictLocked(p)
	s.mu.Unlock()
	s.notify(ev)
	return nil
}

func (p *memPiece) MarkNotComplete() error {
	s := p.t.s
	s.mu.Lock()
	p.complete = false
	s.mu.Unlock()
	return nil
}

// Completion is always "known" (Ok) and never touches disk.
func (p *memPiece) Completion() storage.Completion {
	s := p.t.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return storage.Completion{Ok: true, Complete: p.complete && p.data != nil}
}
