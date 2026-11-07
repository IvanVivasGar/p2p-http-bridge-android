package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ivanvivas/p2p-http-bridge-android/core/pkg/torrent"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestManifestEndpoint(t *testing.T) {
	srv := NewServer(nil, nil, nil, "http://127.0.0.1:8080", "1.0.0")
	req := httptest.NewRequest(http.MethodGet, "/manifest.json", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var m map[string]any
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m["id"] != ManifestID {
		t.Fatalf("id=%v, want %s", m["id"], ManifestID)
	}
}

func TestStreamEndpointReturnsTwoCleanAutoBestOptions(t *testing.T) {
	mockJSON := `{
		"streams": [
			{
				"name": "Torrentio\n4k REMUX",
				"title": "Show.S01E01.2160p.BluRay.REMUX.HEVC\n👤 200 💾 18.0 GB",
				"infoHash": "0000000000000000000000000000000000000000",
				"fileIdx": 0
			},
			{
				"name": "Torrentio\n1080p",
				"title": "Show.S01E01.1080p.WEB-DL.DDP5.1\n👤 90 💾 1.5 GB",
				"infoHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"fileIdx": 0
			},
			{
				"name": "Torrentio\n4k HDR",
				"title": "Show.S01E01.2160p.WEB-DL.DDP5.1.DV.HDR.HEVC-FLUX\n👤 65 💾 2.8 GB",
				"infoHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"fileIdx": 3
			},
			{
				"name": "Torrentio\n1080p",
				"title": "Show.S01E01.1080p.BluRay.x264\n👤 250 💾 2.2 GB",
				"infoHash": "cccccccccccccccccccccccccccccccccccccccc"
			}
		]
	}`

	client := &http.Client{
		Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(mockJSON)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	sc := NewScraper(ScraperConfig{BaseURL: "https://torrentio.strem.fun", Client: client})
	srv := NewServer(nil, sc, nil, "http://127.0.0.1:8080", "1.0.0")

	req := httptest.NewRequest(http.MethodGet, "/stream/series/tt0944947:1:1.json", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Streams []stremioStream `json:"streams"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	// Only 2 clean options: #1 4K Auto-Best and #2 1080p Auto-Best.
	if len(resp.Streams) != 2 {
		t.Fatalf("got %d streams, want 2 clean options (4K + 1080p): %+v", len(resp.Streams), resp.Streams)
	}

	opt4K := resp.Streams[0]
	if opt4K.URL != "http://127.0.0.1:8080/play/adaptive?id=tt0944947%3A1%3A1&q=4k" {
		t.Fatalf("opt4K.URL = %q", opt4K.URL)
	}
	if !strings.Contains(opt4K.Title, "2160p.WEB-DL") {
		t.Fatalf("opt4K.Title missing 4K tags: %q", opt4K.Title)
	}

	opt1080 := resp.Streams[1]
	if opt1080.URL != "http://127.0.0.1:8080/play/adaptive?id=tt0944947%3A1%3A1&q=1080p" {
		t.Fatalf("opt1080.URL = %q", opt1080.URL)
	}
	if !strings.Contains(opt1080.Title, "1080p") {
		t.Fatalf("opt1080.Title missing 1080p tags: %q", opt1080.Title)
	}
}

// fakePlayableStream is an in-memory PlayableStream used to test Swarm Race and adaptive fallback.
type fakePlayableStream struct {
	*bytes.Reader
	name       string
	size       int64
	prebufWait time.Duration
	closed     *atomic.Bool
}

func (f *fakePlayableStream) FileName() string { return f.name }
func (f *fakePlayableStream) FileSize() int64  { return f.size }
func (f *fakePlayableStream) Close() error {
	if f.closed != nil {
		f.closed.Store(true)
	}
	return nil
}

func (f *fakePlayableStream) Prebuffer(ctx context.Context, _, _ int64, timeout time.Duration) error {
	if f.prebufWait > 0 {
		timer := time.NewTimer(f.prebufWait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		if f.prebufWait > timeout {
			return torrent.ErrPrebufferTimeout
		}
	}
	return nil
}

func TestSwarmRacePicksFastest4KCandidate(t *testing.T) {
	const (
		hashSlow4K = "1111111111111111111111111111111111111111"
		hashFast4K = "2222222222222222222222222222222222222222"
	)
	// hashSlow4K has more reported seeds (500 vs 80), so on paper it looks #1,
	// but in real time hashFast4K delivers data in 10ms while hashSlow4K stalls for 300ms.
	mockJSON := `{
		"streams": [
			{
				"name": "Torrentio\n4k HDR",
				"title": "Movie.2024.2160p.WEB-DL.DDP5.1.DV.HDR.HEVC\n👤 500 💾 14.0 GB",
				"infoHash": "` + hashSlow4K + `"
			},
			{
				"name": "Torrentio\n4k HDR",
				"title": "Movie.2024.2160p.WEB-DL.DDP5.1.HDR10.HEVC-PSA\n👤 80 💾 9.5 GB",
				"infoHash": "` + hashFast4K + `"
			}
		]
	}`

	client := &http.Client{
		Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(mockJSON)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	sc := NewScraper(ScraperConfig{BaseURL: "https://torrentio.strem.fun", Client: client})
	srv := NewServer(nil, sc, nil, "http://127.0.0.1:8080", "1.0.0")
	srv.adaptive4KWindow = 500 * time.Millisecond

	var closedSlow atomic.Bool
	srv.openStream = func(_ context.Context, hash string, _ int) (torrent.PlayableStream, error) {
		if hash == hashSlow4K {
			return &fakePlayableStream{
				Reader:     bytes.NewReader([]byte("SLOW-4K-DATA")),
				name:       "slow-4k.mkv",
				size:       12,
				prebufWait: 300 * time.Millisecond,
				closed:     &closedSlow,
			}, nil
		}
		return &fakePlayableStream{
			Reader:     bytes.NewReader([]byte("FAST-4K-WINNER")),
			name:       "fast-4k.mkv",
			size:       14,
			prebufWait: 10 * time.Millisecond,
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/play/adaptive?id=tt0111161&q=4k", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 OK", rec.Code)
	}
	if got := rec.Body.String(); got != "FAST-4K-WINNER" {
		t.Fatalf("body = %q, want FAST-4K-WINNER (fastest swarm should win the race)", got)
	}
}

func TestPlayAdaptiveFallbackFromSlow4KTo1080pAndSessionPinning(t *testing.T) {
	const (
		hash4K    = "4444444444444444444444444444444444444444"
		hash1080p = "1080108010801080108010801080108010801080"
	)
	mockJSON := `{
		"streams": [
			{
				"name": "Torrentio\n4k HDR",
				"title": "Movie.2024.2160p.WEB-DL.DDP5.1.DV.HDR.HEVC\n👤 80 💾 14.0 GB",
				"infoHash": "` + hash4K + `",
				"fileIdx": 0
			},
			{
				"name": "Torrentio\n1080p",
				"title": "Movie.2024.1080p.WEB-DL.DDP5.1.H.264\n👤 220 💾 4.5 GB",
				"infoHash": "` + hash1080p + `",
				"fileIdx": 1
			}
		]
	}`

	client := &http.Client{
		Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(mockJSON)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	sc := NewScraper(ScraperConfig{BaseURL: "https://torrentio.strem.fun", Client: client})
	srv := NewServer(nil, sc, nil, "http://127.0.0.1:8080", "1.0.0")
	srv.adaptive4KWindow = 60 * time.Millisecond

	data4K := []byte("4K-STREAM-DATA-PAYLOAD")
	data1080p := []byte("1080P-FALLBACK-STREAM-PAYLOAD")

	var closed4K atomic.Bool
	var attempts4K, attempts1080p atomic.Int32

	srv.openStream = func(_ context.Context, hash string, _ int) (torrent.PlayableStream, error) {
		if hash == hash4K {
			attempts4K.Add(1)
			return &fakePlayableStream{
				Reader:     bytes.NewReader(data4K),
				name:       "movie-4k.mkv",
				size:       int64(len(data4K)),
				prebufWait: 150 * time.Millisecond,
				closed:     &closed4K,
			}, nil
		}
		attempts1080p.Add(1)
		return &fakePlayableStream{
			Reader: bytes.NewReader(data1080p),
			name:   "movie-1080p.mkv",
			size:   int64(len(data1080p)),
		}, nil
	}

	req1 := httptest.NewRequest(http.MethodGet, "/play/adaptive?id=tt0111161&q=4k", nil)
	req1.Header.Set("Range", "bytes=0-13")
	rec1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusPartialContent {
		t.Fatalf("status=%d, want 206 Partial Content; body=%s", rec1.Code, rec1.Body.String())
	}
	if !closed4K.Load() {
		t.Fatal("expected slow 4K stream reader to be closed on fallback")
	}
	if got := rec1.Header().Get("X-Adaptive-Selected"); got != "1080p" {
		t.Fatalf("X-Adaptive-Selected = %q, want 1080p", got)
	}
	if got := rec1.Body.String(); got != "1080P-FALLBACK" {
		t.Fatalf("body = %q, want %q", got, "1080P-FALLBACK")
	}

	req2 := httptest.NewRequest(http.MethodGet, "/play/adaptive?id=tt0111161&q=4k", nil)
	req2.Header.Set("Range", "bytes=15-28")
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusPartialContent {
		t.Fatalf("second range status=%d, want 206", rec2.Code)
	}
	if got := rec2.Body.String(); got != "STREAM-PAYLOAD" {
		t.Fatalf("second range body = %q, want %q", got, "STREAM-PAYLOAD")
	}
	if attempts4K.Load() != 1 {
		t.Fatalf("expected 4K candidate to be probed only once due to session pinning, got %d", attempts4K.Load())
	}
	if attempts1080p.Load() != 2 {
		t.Fatalf("expected 1080p candidate to serve both range requests, got %d", attempts1080p.Load())
	}
}

func TestNextEpisodeBingePrewarm(t *testing.T) {
	if next, ok := NextEpisodeID("tt0944947:1:1"); !ok || next != "tt0944947:1:2" {
		t.Fatalf("NextEpisodeID(tt0944947:1:1) = %q, %v; want tt0944947:1:2, true", next, ok)
	}
	if _, ok := NextEpisodeID("tt0111161"); ok {
		t.Fatal("expected movie ID not to produce a NextEpisodeID")
	}

	ep2Requested := make(chan struct{}, 1)
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "tt0944947:1:2") {
				select {
				case ep2Requested <- struct{}{}:
				default:
				}
			}
			body := `{
				"streams": [
					{
						"name": "Torrentio\n4k HDR",
						"title": "Show.S01.2160p.WEB-DL.DDP5.1.HDR.HEVC\n👤 120 💾 2.5 GB",
						"infoHash": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
					}
				]
			}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	sc := NewScraper(ScraperConfig{
		BaseURL:      "https://torrentio.strem.fun",
		FallbackURLs: []string{},
		Client:       client,
	})
	srv := NewServer(nil, sc, nil, "http://127.0.0.1:8080", "1.0.0")

	payload := bytes.Repeat([]byte("A"), 1000)
	srv.openStream = func(_ context.Context, _ string, _ int) (torrent.PlayableStream, error) {
		return &fakePlayableStream{
			Reader: bytes.NewReader(payload),
			name:   "ep1.mkv",
			size:   int64(len(payload)),
		}, nil
	}

	// 1. Seek to 90% of episode 1 (bytes=900-949): should trigger background pre-warm of episode 2.
	req := httptest.NewRequest(http.MethodGet, "/play/adaptive?id=tt0944947:1:1&q=4k", nil)
	req.Header.Set("Range", "bytes=900-949")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status=%d, want 206", rec.Code)
	}

	select {
	case <-ep2Requested:
	case <-time.After(2 * time.Second):
		t.Fatal("expected episode 2 (tt0944947:1:2) to be pre-warmed in background at 90% playback")
	}
}
