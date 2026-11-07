package torrent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	anacrolix "github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

// TestMemStorageEviction verifies MemStorage directly in memory (no sockets required):
//   - hard budget enforcement and automatic piece eviction,
//   - playhead-aware eviction order (pieces far ahead of readahead -> pieces behind -> readahead window),
//   - ErrPieceEvicted returned when reading evicted pieces,
//   - OnEvict callback invocation.
func TestMemStorageEviction(t *testing.T) {
	const (
		pieceLen  = 256 << 10 // 256 KiB
		numPieces = 20
		budget    = 4 * pieceLen // 1 MiB (4 pieces)
	)
	store := NewMemStorage(budget)
	var evictedPieces []int
	evictCh := make(chan int, 32)
	store.SetOnEvict(func(_ metainfo.Hash, piece int) {
		evictCh <- piece
	})

	info := &metainfo.Info{
		PieceLength: pieceLen,
		Pieces:      make([]byte, numPieces*20),
		Length:      int64(numPieces) * pieceLen,
	}
	var ih metainfo.Hash
	ih[0] = 0x42

	ti, err := store.OpenTorrent(context.Background(), info, ih)
	if err != nil {
		t.Fatal(err)
	}
	defer ti.Close()

	// Set playhead at piece 5 with 2-piece readahead [5..7].
	store.SetPlayhead(ih, 5, 2)

	writePiece := func(idx int, fill byte) {
		p := ti.Piece(info.Piece(idx))
		buf := bytes.Repeat([]byte{fill}, pieceLen)
		if _, err := p.WriteAt(buf, 0); err != nil {
			t.Fatalf("WriteAt(%d): %v", idx, err)
		}
		if err := p.MarkComplete(); err != nil {
			t.Fatalf("MarkComplete(%d): %v", idx, err)
		}
	}

	// Fill 4 pieces: 1 (behind), 5 (playhead), 6 (in readahead), 15 (far ahead of readahead).
	for _, idx := range []int{1, 5, 6, 15} {
		writePiece(idx, byte(idx+1))
	}
	if store.Used() != budget {
		t.Fatalf("used=%d want=%d", store.Used(), budget)
	}

	// Writing piece 7 (inside readahead) pushes resident memory over budget -> piece 15 (class A: far ahead) must be evicted first.
	writePiece(7, 8)
	if store.Used() > budget {
		t.Fatalf("cache exceeded budget: used=%d budget=%d", store.Used(), budget)
	}
	select {
	case got := <-evictCh:
		evictedPieces = append(evictedPieces, got)
		if got != 15 {
			t.Fatalf("first evicted piece=%d, want 15 (far ahead of readahead)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for eviction callback")
	}

	// Evicted piece 15 must return ErrPieceEvicted and report !Complete.
	p15 := ti.Piece(info.Piece(15))
	if c := p15.Completion(); c.Complete {
		t.Fatal("piece 15 should be marked incomplete after eviction")
	}
	if _, err := p15.ReadAt(make([]byte, 16), 0); err != ErrPieceEvicted {
		t.Fatalf("ReadAt on evicted piece = %v, want ErrPieceEvicted", err)
	}

	// Writing piece 8 evicts piece 1 (class B: behind playhead) before touching pieces 5, 6, 7.
	writePiece(8, 9)
	select {
	case got := <-evictCh:
		if got != 1 {
			t.Fatalf("second evicted piece=%d, want 1 (behind playhead)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second eviction callback")
	}
}

// TestStreamEvictionAndSeek runs a real swarm on loopback (one file-backed seeder, one Engine with
// a 16 MiB RAM cache) when loopback sockets are permitted by the OS/sandbox.
func TestStreamEvictionAndSeek(t *testing.T) {
	// Probe whether the execution environment permits loopback TCP connections.
	probeLn, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback listen not permitted in this environment: %v", err)
	}
	probeConn, err := net.DialTimeout("tcp4", probeLn.Addr().String(), time.Second)
	_ = probeLn.Close()
	if err != nil {
		t.Skipf("loopback dial not permitted in this sandbox (%v); run `go test` outside sandbox for full swarm test", err)
	}
	_ = probeConn.Close()

	const (
		fileSize = 40 << 20
		pieceLen = 256 << 10
	)
	dir := t.TempDir()
	data := make([]byte, fileSize)
	rand.New(rand.NewSource(1)).Read(data)
	filePath := filepath.Join(dir, "video.mkv")
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	info := metainfo.Info{PieceLength: pieceLen}
	if err := info.BuildFromFilePath(filePath); err != nil {
		t.Fatal(err)
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}

	// --- seeder
	scfg := anacrolix.NewDefaultClientConfig()
	scfg.DataDir = dir
	scfg.NoDHT = true
	scfg.DisableTrackers = true
	scfg.Seed = true
	scfg.ListenPort = 0
	scfg.DisableIPv6 = true
	scfg.DisableUTP = true
	scfg.NoDefaultPortForwarding = true
	scfg.DisableWebtorrent = true
	scfg.ListenHost = func(string) string { return "127.0.0.1" }
	seeder, err := anacrolix.NewClient(scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer seeder.Close()
	st, err := seeder.AddTorrent(mi)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for st.BytesCompleted() < fileSize {
		if time.Now().After(deadline) {
			t.Fatalf("seeder never completed hash check: %d/%d", st.BytesCompleted(), fileSize)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// --- engine under test
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	e, err := NewEngine(Config{
		CacheBytes: 16 << 20, Logger: quiet, LibLogger: quiet, MetadataTimeout: 30 * time.Second,
		tweak: func(c *anacrolix.ClientConfig) {
			c.NoDHT = true
			c.DisableTrackers = true
			c.DisableUTP = true
			c.DisableIPv6 = true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	ih := mi.HashInfoBytes()
	sess, err := e.acquire(ih)
	if err != nil {
		t.Fatal(err)
	}
	sess.t.AddPeers([]anacrolix.PeerInfo{{
		Addr:    &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: seeder.LocalPort()},
		Trusted: true,
	}})
	e.release(sess)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stream, err := e.Open(ctx, ih.HexString(), -1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if stream.Size != fileSize || stream.Name != "video.mkv" {
		t.Fatalf("unexpected stream %q size=%d", stream.Name, stream.Size)
	}

	// 1) full sequential read, checking RAM usage on the way.
	h := sha256.New()
	buf := make([]byte, 64<<10)
	var maxUsed int64
	for {
		n, err := stream.Read(buf)
		h.Write(buf[:n])
		if u := e.store.Used(); u > maxUsed {
			maxUsed = u
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if sum := sha256.Sum256(data); !bytes.Equal(h.Sum(nil), sum[:]) {
		t.Fatal("sequential content mismatch")
	}
	slack := int64(8 * pieceLen)
	if budget := e.store.Budget(); maxUsed > budget+slack {
		t.Fatalf("RAM cache overshoot: used=%d budget=%d", maxUsed, budget)
	}
	if e.store.Evictions() == 0 {
		t.Fatal("expected evictions when streaming 40 MiB through a 16 MiB cache")
	}

	// 2) seeks: backwards into evicted data, mid-file, and the tail.
	for _, off := range []int64{3 << 20, 20<<20 + 12345, fileSize - 512<<10, 0} {
		if _, err := stream.Seek(off, io.SeekStart); err != nil {
			t.Fatalf("seek %d: %v", off, err)
		}
		got := make([]byte, 1<<20)
		n, err := io.ReadFull(stream, got)
		if err != nil && err != io.ErrUnexpectedEOF {
			t.Fatalf("read after seek %d: %v", off, err)
		}
		if !bytes.Equal(got[:n], data[off:off+int64(n)]) {
			t.Fatalf("content mismatch after seek to %d", off)
		}
	}
	if end, err := stream.Seek(0, io.SeekEnd); err != nil || end != fileSize {
		t.Fatalf("SeekEnd = %d, %v", end, err)
	}
}
