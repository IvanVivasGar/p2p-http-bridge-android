package bridge

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestScraperFireStickFilteringAndScoring(t *testing.T) {
	mockJSON := `{
		"streams": [
			{
				"name": "Torrentio\n4k HDR",
				"title": "Movie.2024.2160p.UHD.BluRay.REMUX.HDR.HEVC.TrueHD.7.1\n👤 400 💾 62.5 GB ⚙️ RARBG",
				"infoHash": "0000000000000000000000000000000000000001"
			},
			{
				"name": "Torrentio\n4k",
				"title": "Movie.2024.2160p.WEB-DL.HEVC.Uncompressed\n👤 350 💾 29.0 GB ⚙️ 1337x",
				"infoHash": "0000000000000000000000000000000000000002"
			},
			{
				"name": "Torrentio\n4k HDR",
				"title": "Movie.2024.2160p.WEB-DL.DDP5.1.DV.HDR10.HEVC\n👤 8 💾 12.0 GB ⚙️ DeadSwarm",
				"infoHash": "0000000000000000000000000000000000000003"
			},
			{
				"name": "Torrentio\n1080p",
				"title": "Movie.2024.1080p.HDCAM.c1nem4\n👤 999 💾 1.8 GB",
				"infoHash": "0000000000000000000000000000000000000004"
			},
			{
				"name": "External Debrid",
				"title": "Missing InfoHash",
				"infoHash": ""
			},
			{
				"name": "Torrentio\n720p",
				"title": "Movie.2024.720p.WEB-DL.x264\n👤 500 💾 1.4 GB ⚙️ YTS",
				"infoHash": "1111111111111111111111111111111111111111"
			},
			{
				"name": "Torrentio\n1080p",
				"title": "Movie.2024.1080p.WEB-DL.DDP5.1.H.264\n👤 120 💾 4.2 GB ⚙️ EZTV",
				"infoHash": "2222222222222222222222222222222222222222",
				"fileIdx": 0
			},
			{
				"name": "Torrentio\n1080p",
				"title": "Movie.2024.1080p.BluRay.x264\n👤 450 💾 8.5 GB ⚙️ RARBG",
				"infoHash": "3333333333333333333333333333333333333333"
			},
			{
				"name": "Torrentio\n4k",
				"title": "Movie.2024.2160p.10bit.HDR.BluRay.HEVC.AAC-PSA\n👤 95 💾 6.8 GB ⚙️ PSA",
				"infoHash": "4444444444444444444444444444444444444444",
				"fileIdx": 2
			},
			{
				"name": "Torrentio\n4k HDR",
				"title": "Movie.2024.2160p.WEB-DL.DDP5.1.Atmos.DV.HDR10.H.265\n👤 180 💾 14.5 GB ⚙️ 1337x",
				"infoHash": "5555555555555555555555555555555555555555",
				"fileIdx": 1
			},
			{
				"name": "Torrentio\n4k DV",
				"title": "Movie.2024.2160p.WEBRip.EAC3.DoVi.HDR.HEVC\n👤 310 💾 12.2 GB ⚙️ TorrentGalaxy",
				"infoHash": "6666666666666666666666666666666666666666",
				"fileIdx": 0
			}
		]
	}`

	var gotUA string
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotUA = r.Header.Get("User-Agent")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(mockJSON)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	sc := NewScraper(ScraperConfig{
		BaseURL: "https://torrentio.strem.fun",
		Client:  client,
	})

	pool, err := sc.ClassifyStreams(t.Context(), "movie", "tt0111161")
	if err != nil {
		t.Fatalf("ClassifyStreams failed: %v", err)
	}
	if !strings.Contains(gotUA, "Mozilla/5.0") {
		t.Fatalf("expected residential browser User-Agent, got %q", gotUA)
	}

	streams := pool.AllManual
	wantOrder := []struct {
		hash    string
		quality string
		seeds   int
		fileIdx int
	}{
		{"6666666666666666666666666666666666666666", "4K", 310, 0},
		{"5555555555555555555555555555555555555555", "4K", 180, 1},
		{"4444444444444444444444444444444444444444", "4K", 95, 2},
		{"3333333333333333333333333333333333333333", "1080p", 450, -1},
		{"2222222222222222222222222222222222222222", "1080p", 120, 0},
		{"1111111111111111111111111111111111111111", "720p", 500, -1},
	}

	if len(streams) != len(wantOrder) {
		t.Fatalf("got %d streams, want %d: %+v", len(streams), len(wantOrder), streams)
	}

	for i, w := range wantOrder {
		got := streams[i]
		if got.InfoHash != w.hash {
			t.Errorf("[%d] hash = %s, want %s (score=%d)", i, got.InfoHash, w.hash, got.Score)
		}
		if got.Quality != w.quality {
			t.Errorf("[%d] quality = %s, want %s", i, got.Quality, w.quality)
		}
		if got.Seeds != w.seeds {
			t.Errorf("[%d] seeds = %d, want %d", i, got.Seeds, w.seeds)
		}
		if got.FileIdx != w.fileIdx {
			t.Errorf("[%d] fileIdx = %d, want %d", i, got.FileIdx, w.fileIdx)
		}
	}

	// Verify Adaptive classification pools:
	// 3 adaptive 4K candidates (all <= 22 GB, >= 30 seeds, no REMUX/TrueHD)
	// 2 adaptive 1080p candidates (>= 25 seeds)
	if len(pool.Candidates4K) != 3 {
		t.Fatalf("got %d adaptive 4K candidates, want 3", len(pool.Candidates4K))
	}
	if len(pool.Candidates1080p) != 2 {
		t.Fatalf("got %d adaptive 1080p candidates, want 2", len(pool.Candidates1080p))
	}
	if best4K, ok := pool.Best4K(); !ok || best4K.InfoHash != "6666666666666666666666666666666666666666" {
		t.Fatalf("unexpected best4K: %+v", best4K)
	}
	if best1080, ok := pool.Best1080p(); !ok || best1080.InfoHash != "3333333333333333333333333333333333333333" {
		t.Fatalf("unexpected best1080p: %+v", best1080)
	}
}

func TestAdaptive4KRejectsTrueHDAndOver22GBAndLowSeeds(t *testing.T) {
	cTrueHD := Candidate{
		InfoHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Name:     "Torrentio\n4k HDR",
		Title:    "Movie.2160p.WEB-DL.TrueHD.7.1.HEVC\n👤 100 💾 18.0 GB",
		Quality:  "4K",
		Seeds:    100,
		Size:     18 << 30,
	}
	if IsAdaptive4K(cTrueHD, "movie") {
		t.Fatal("expected 4K candidate with TrueHD to be excluded from Adaptive 4K pool")
	}

	c24GB := Candidate{
		InfoHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Name:     "Torrentio\n4k HDR",
		Title:    "Movie.2160p.WEB-DL.DDP5.1.HEVC\n👤 100 💾 24.0 GB",
		Quality:  "4K",
		Seeds:    100,
		Size:     24 << 30,
	}
	if IsAdaptive4K(c24GB, "movie") {
		t.Fatal("expected 24 GB 4K candidate (> 22 GB cap) to be excluded from Adaptive 4K pool")
	}

	c20Seeds := Candidate{
		InfoHash: "cccccccccccccccccccccccccccccccccccccccc",
		Name:     "Torrentio\n4k HDR",
		Title:    "Movie.2160p.WEB-DL.DDP5.1.HEVC\n👤 20 💾 12.0 GB",
		Quality:  "4K",
		Seeds:    20,
		Size:     12 << 30,
	}
	if IsAdaptive4K(c20Seeds, "movie") {
		t.Fatal("expected 20-seed 4K candidate (< 30 seeds) to be excluded from Adaptive 4K pool")
	}
}

func TestScraperFallbackToMirrorOn403AndTimeout(t *testing.T) {
	mockJSON := `{
		"streams": [
			{
				"name": "KnightCrawler\n4k HDR",
				"title": "Movie.2025.2160p.WEB-DL.DDP5.1.DV.HDR10.HEVC\n👤 420 💾 11.5 GB",
				"infoHash": "9999999999999999999999999999999999999999"
			}
		]
	}`

	var gotAcceptLang, gotSecFetchMode string
	var primaryHits, timeoutHits, mirrorHits atomic.Int32

	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotAcceptLang = r.Header.Get("Accept-Language")
			gotSecFetchMode = r.Header.Get("Sec-Fetch-Mode")
			host := r.URL.Host
			switch host {
			case "primary.blocked.test":
				primaryHits.Add(1)
				return &http.Response{
					StatusCode: http.StatusForbidden,
					Body:       io.NopCloser(strings.NewReader("cloudflare 403")),
					Header:     make(http.Header),
				}, nil
			case "slow.mirror.test":
				timeoutHits.Add(1)
				<-r.Context().Done()
				return nil, r.Context().Err()
			case "good.mirror.test":
				mirrorHits.Add(1)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(mockJSON)),
					Header:     make(http.Header),
				}, nil
			default:
				t.Fatalf("unexpected host %q", host)
				return nil, nil
			}
		}),
	}

	sc := NewScraper(ScraperConfig{
		BaseURL: "https://primary.blocked.test",
		FallbackURLs: []string{
			"https://slow.mirror.test",
			"https://good.mirror.test",
		},
		EndpointTimeout: 50 * time.Millisecond,
		Client:          client,
	})

	cands, err := sc.Streams(t.Context(), "movie", "tt15239678")
	if err != nil {
		t.Fatalf("expected fallback mirror to succeed, got error: %v", err)
	}
	if len(cands) != 1 || cands[0].InfoHash != "9999999999999999999999999999999999999999" {
		t.Fatalf("unexpected candidates from mirror: %+v", cands)
	}
	if primaryHits.Load() != 1 || timeoutHits.Load() != 1 || mirrorHits.Load() != 1 {
		t.Fatalf("expected 1 hit per endpoint in order, got primary=%d timeout=%d mirror=%d",
			primaryHits.Load(), timeoutHits.Load(), mirrorHits.Load())
	}
	if !strings.HasPrefix(gotAcceptLang, "es-MX") || gotSecFetchMode != "cors" {
		t.Fatalf("expected Android TV Chrome headers (es-MX, cors), got Accept-Language=%q Sec-Fetch-Mode=%q",
			gotAcceptLang, gotSecFetchMode)
	}
}

func TestScraperTTLCacheAndBoundedEviction(t *testing.T) {
	var upstreamHits atomic.Int32
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			upstreamHits.Add(1)
			body := `{
				"streams": [
					{
						"name": "Torrentio\n1080p",
						"title": "Movie.2025.1080p.WEB-DL.DDP5.1.H.265\n👤 200 💾 3.5 GB",
						"infoHash": "abababababababababababababababababababab"
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
		BaseURL:         "https://torrentio.strem.fun",
		FallbackURLs:    []string{},
		CacheTTL:        10 * time.Minute,
		MaxCacheEntries: 3,
		Client:          client,
	})

	// Two lookups for the same ID should only hit upstream once.
	for i := 0; i < 2; i++ {
		if _, err := sc.Streams(t.Context(), "movie", "tt0000001"); err != nil {
			t.Fatalf("lookup failed: %v", err)
		}
	}
	if upstreamHits.Load() != 1 {
		t.Fatalf("expected 1 upstream hit due to TTL cache, got %d", upstreamHits.Load())
	}

	// Insert 4 distinct movies when MaxCacheEntries = 3; cache size must remain bounded at 3.
	for i := 2; i <= 5; i++ {
		id := fmt.Sprintf("tt000000%d", i)
		if _, err := sc.Streams(t.Context(), "movie", id); err != nil {
			t.Fatalf("lookup %s failed: %v", id, err)
		}
	}
	if got := sc.CacheLen(); got > 3 {
		t.Fatalf("expected cache length bounded at 3, got %d", got)
	}
}

func TestForeignDubDemotionAndLanguagePriority(t *testing.T) {
	if !IsForeignDubOnly("Movie.2025.2160p.WEB-DL.RUS.HEVC.mkv") {
		t.Fatal("expected RUS-only release to be detected as foreign dub only")
	}
	if !IsForeignDubOnly("Movie.2025.2160p.WEB-DL.TRUEFRENCH.HEVC.mkv") {
		t.Fatal("expected TRUEFRENCH without MULTi to be detected as foreign dub only")
	}
	if IsForeignDubOnly("Movie.2025.2160p.WEB-DL.MULTi.TRUEFRENCH.ENG.HEVC.mkv") {
		t.Fatal("expected MULTi/ENG release not to be flagged as foreign dub only")
	}
	if IsForeignDubOnly("Movie.2025.2160p.WEB-DL.DUAL.LATINO.ENG.DDP5.1.H265.mkv") {
		t.Fatal("expected DUAL LATINO/ENG release not to be flagged as foreign dub only")
	}

	rus4K := Candidate{
		InfoHash: "1111111111111111111111111111111111111111",
		Name:     "Torrentio\n4k HDR",
		Title:    "Movie.2025.2160p.WEB-DL.DDP5.1.HDR.HEVC.RUS\n👤 900 💾 12.0 GB",
		Quality:  "4K",
		Seeds:    900,
		Size:     12 << 30,
	}
	rus4K.Score = ScoreCandidate(rus4K)

	latMulti4K := Candidate{
		InfoHash: "2222222222222222222222222222222222222222",
		Name:     "Torrentio\n4k HDR",
		Title:    "Movie.2025.2160p.WEB-DL.DDP5.1.HDR.HEVC.MULTi.LATINO.ENG\n👤 120 💾 12.5 GB",
		Quality:  "4K",
		Seeds:    120,
		Size:     12 << 30,
	}
	latMulti4K.Score = ScoreCandidate(latMulti4K)

	if IsAdaptive4K(rus4K, "movie") {
		t.Fatal("expected RUS-only 4K dub to be excluded from strict Adaptive 4K pool")
	}
	if !IsAdaptive4K(latMulti4K, "movie") {
		t.Fatal("expected MULTi/LATINO/ENG 4K stream to be accepted in Adaptive 4K pool")
	}
	if latMulti4K.Score <= rus4K.Score {
		t.Fatalf("expected MULTi/LATINO score (%d) > RUS-only score (%d)", latMulti4K.Score, rus4K.Score)
	}

	// SelectRaceLineup must put preferred-language candidates ahead of foreign-only dubs even if
	// the foreign dub has more seeds.
	cands := []Candidate{
		latMulti4K,
		{InfoHash: "3333333333333333333333333333333333333333", Name: "4k", Title: "Movie.2160p.WEB-DL.HEVC.ENG", Quality: "4K", Seeds: 110, Score: 4175},
		{InfoHash: "4444444444444444444444444444444444444444", Name: "4k", Title: "Movie.2160p.WEB-DL.HEVC.SPA", Quality: "4K", Seeds: 100, Score: 4175},
		{InfoHash: "5555555555555555555555555555555555555555", Name: "4k", Title: "Movie.2160p.WEB-DL.HEVC.DUAL", Quality: "4K", Seeds: 90, Score: 4175},
		rus4K,
	}
	lineup := SelectRaceLineup(cands, 4)
	for _, c := range lineup {
		if c.InfoHash == rus4K.InfoHash {
			t.Fatalf("expected RUS-only dub to be kept out of top-4 race lineup when 4 MULTi/ENG/SPA candidates exist: %+v", lineup)
		}
	}
}

func TestElasticSeedThresholdForClassicMovies(t *testing.T) {
	// Classic/indie film where the best 4K has 8 seeds and the best 1080p has 6 seeds
	// (below the strict 30/25/15 thresholds, but above the elastic >= 5 threshold).
	mockJSON := `{
		"streams": [
			{
				"name": "Torrentio\n4k HDR",
				"title": "Classic.Film.1975.2160p.WEB-DL.DDP5.1.HDR.HEVC\n👤 8 💾 11.0 GB",
				"infoHash": "8888888888888888888888888888888888888888"
			},
			{
				"name": "Torrentio\n1080p",
				"title": "Classic.Film.1975.1080p.BluRay.x264\n👤 6 💾 5.5 GB",
				"infoHash": "7777777777777777777777777777777777777777"
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

	sc := NewScraper(ScraperConfig{
		BaseURL:      "https://torrentio.strem.fun",
		FallbackURLs: []string{},
		Client:       client,
	})

	pool, err := sc.ClassifyStreams(t.Context(), "movie", "tt0073486")
	if err != nil {
		t.Fatalf("ClassifyStreams failed: %v", err)
	}
	if len(pool.Candidates4K) != 1 || pool.Candidates4K[0].InfoHash != "8888888888888888888888888888888888888888" {
		t.Fatalf("expected elastic 4K candidate (8 seeds) to be promoted, got %+v", pool.Candidates4K)
	}
	if len(pool.Candidates1080p) != 1 || pool.Candidates1080p[0].InfoHash != "7777777777777777777777777777777777777777" {
		t.Fatalf("expected elastic 1080p candidate (6 seeds) to be promoted, got %+v", pool.Candidates1080p)
	}
}

func TestMultiIndexer4KEnrichmentWhenPrimaryOnlyHas1080p(t *testing.T) {
	primaryJSON := `{
		"streams": [
			{
				"name": "Torrentio\n1080p",
				"title": "Movie.2025.1080p.WEB-DL.DDP5.1.H264\n👤 150 💾 4.0 GB",
				"infoHash": "1010101010101010101010101010101010101010"
			}
		]
	}`
	mirrorJSON := `{
		"streams": [
			{
				"name": "KnightCrawler\n4k HDR",
				"title": "Movie.2025.2160p.WEB-DL.DDP5.1.HDR.HEVC\n👤 90 💾 13.0 GB",
				"infoHash": "4040404040404040404040404040404040404040"
			}
		]
	}`

	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := primaryJSON
			if r.URL.Host == "mirror.enrichment.test" {
				body = mirrorJSON
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	sc := NewScraper(ScraperConfig{
		BaseURL:      "https://primary.1080only.test",
		FallbackURLs: []string{"https://mirror.enrichment.test"},
		Client:       client,
	})

	pool, err := sc.ClassifyStreams(t.Context(), "movie", "tt9999999")
	if err != nil {
		t.Fatalf("ClassifyStreams failed: %v", err)
	}
	if len(pool.Candidates4K) != 1 || pool.Candidates4K[0].InfoHash != "4040404040404040404040404040404040404040" {
		t.Fatalf("expected 4K candidate from backup indexer to be merged, got %+v", pool.Candidates4K)
	}
	if len(pool.Candidates1080p) != 1 || pool.Candidates1080p[0].InfoHash != "1010101010101010101010101010101010101010" {
		t.Fatalf("expected 1080p candidate from primary indexer to be preserved, got %+v", pool.Candidates1080p)
	}
}
