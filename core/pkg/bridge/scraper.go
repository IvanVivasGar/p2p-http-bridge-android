package bridge

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// browserUA emulates Chrome 124 on Fire OS / Android TV (AFTMM = Fire TV Stick 4K) so upstream
// CDNs treat requests as standard Android TV WebView/Chrome traffic.
const browserUA = "Mozilla/5.0 (Linux; Android 11; AFTMM Build/RS8132.2610N; wv) " +
	"AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/124.0.0.0 Mobile Safari/537.36"

// DefaultTorrentioURL is the primary upstream addon base URL. A Torrentio configuration segment
// may be appended (e.g. https://torrentio.strem.fun/providers=yts,eztv|sort=seeders).
const DefaultTorrentioURL = "https://torrentio.strem.fun"

// DefaultFallbackEndpoints defines the ordered list of Stremio-compatible torrent indexers tried
// when the primary endpoint returns HTTP >= 400 (403/429/5xx) or exceeds DefaultEndpointTimeout.
var DefaultFallbackEndpoints = []string{
	DefaultTorrentioURL,
	"https://knightcrawler.elfhosted.com",
	"https://comet.elfhosted.com",
	"https://mediafusion.elfhosted.com",
}

// Hardware / residential bandwidth limits for Fire TV Stick (64 MB RAM ring buffer) and TTL cache.
const (
	DefaultMinSeeds          = 15
	DefaultMaxMovieBytes     = 25 << 30        // 25 GiB hard cap for manual movies
	DefaultMaxSeriesBytes    = 4 << 30         // 4 GiB hard cap per series episode
	Adaptive4KMaxMovieBytes  = 22 << 30        // 22 GiB cap for adaptive 4K candidates
	Adaptive4KMinSeeds       = 30               // minimum seeds for adaptive 4K candidates
	Adaptive1080pMaxBytes    = 15 << 30         // 15 GiB cap for adaptive 1080p candidates
	Adaptive1080pMinSeeds    = 25               // minimum seeds for adaptive 1080p candidates
	ElasticMinSeeds          = 3                // elastic floor when a classic/niche title has <15 seeds
	ElasticAdaptiveMinSeeds  = 5                // elastic floor for 4K/1080p adaptive pools when strict pool is empty
	DefaultCacheTTL          = 12 * time.Minute // 10–15 min TTL to prevent rate-limit bursts
	DefaultMaxCacheEntries   = 64              // max cached titles in RAM (~1 MiB max overhead)
	DefaultEndpointTimeout   = 3 * time.Second // per-endpoint timeout before failing over to mirror
	AdaptiveStreamName       = "P2P Auto-Adaptive (4K/1080p)"
	AdaptiveStreamTitle      = "Selección automática según rendimiento de red (Recomendado Fire Stick)"
)

// ErrBlocked is returned when the upstream answers 403/429 (typically Cloudflare).
var ErrBlocked = errors.New("torrentio blocked the request (HTTP 403/429)")

var (
	idRe       = regexp.MustCompile(`^tt\d{1,10}(?::\d{1,4}:\d{1,4})?$`)
	seedsRe    = regexp.MustCompile(`👤\s*(\d+)`)
	sizeRe     = regexp.MustCompile(`💾\s*([\d.]+)\s*(GB|MB|KB|TB)`)
	q4kRe      = regexp.MustCompile(`(?i)\b(2160p|4k|uhd)\b`)
	q1080Re    = regexp.MustCompile(`(?i)\b1080p\b`)
	q720Re     = regexp.MustCompile(`(?i)\b720p\b`)
	q480Re     = regexp.MustCompile(`(?i)\b(480p|576p|dvdrip)\b`)
	junkRe     = regexp.MustCompile(`(?i)\b(cam|hdcam|camrip|telesync|hdts|ts|tc|telecine|screener|dvdscr|bdscr|workprint)\b`)
	remuxRe    = regexp.MustCompile(`(?i)\b(remux|bdremux|uhd[\s._-]*remux)\b`)
	trueHDRe   = regexp.MustCompile(`(?i)\b(truehd|dts[ ._-]?hd(?:[ ._-]?ma)?)\b`)
	webSrcRe   = regexp.MustCompile(`(?i)\b(web[ ._-]?dl|webrip|web)\b`)
	blurayRe   = regexp.MustCompile(`(?i)\b(bluray|blu[ ._-]?ray|bdrip|brrip)\b`)
	hdrOrDVRe  = regexp.MustCompile(`(?i)\b(hdr|hdr10\+?|dv|dovi|dolby[ ._-]?vision)\b`)
	hdr10Re    = regexp.MustCompile(`(?i)\b(hdr|hdr10\+?)\b`)
	dvP5Re     = regexp.MustCompile(`(?i)\b(dv[ ._-]*p(?:rofile)?[ ._-]*0?5|dovi[ ._-]*p(?:rofile)?[ ._-]*0?5)\b`)
	mp4FileRe  = regexp.MustCompile(`(?i)\.mp4\b`)
	hevcRe           = regexp.MustCompile(`(?i)\b(hevc|[xh][ ._-]?265|10[ ._-]?bit)\b`)
	lightAudRe       = regexp.MustCompile(`(?i)\b(ddp[57]?\.?1?|e[ -]?ac[ -]?3|eac3|aac|ac3|dd5\.?1)\b`)
	encoderRe        = regexp.MustCompile(`(?i)\b(psa|tigole|qxr|flux|ion265|rarbg|ntb)\b`)
	prefLangRe       = regexp.MustCompile(`(?i)(\b(multi|dual|latino|lat|spanish|castellano|esp|spa|eng|english)\b|🇲🇽|🇪🇸|🇬🇧|🇺🇸)`)
	foreignDubOnlyRe = regexp.MustCompile(`(?i)(\b(rus|russian|ukr|truefrench|vff|vfi|vfq|ita|italian|german|deutsch|hindi|tamil|telugu|dubbed)\b|🇷🇺|🇺🇦|🇫🇷|🇮🇹|🇩🇪|🇮🇳)`)
	validTypes       = map[string]bool{"movie": true, "series": true}
)

// IsForeignDubOnly reports whether a release title/description indicates an exclusive foreign dub
// (e.g. RUS, TRUEFRENCH, VFF, ITA, GERMAN, UKR, HINDI) without any English, Spanish, DUAL or MULTi audio track.
func IsForeignDubOnly(text string) bool {
	return foreignDubOnlyRe.MatchString(text) && !prefLangRe.MatchString(text)
}

// Candidate is a playable torrent offered by Torrentio, scored for Fire TV hardware.
type Candidate struct {
	InfoHash      string         `json:"infoHash"`
	Name          string         `json:"name"`
	Title         string         `json:"title"`
	Quality       string         `json:"quality"` // "4K", "1080p", "720p", "480p" or "unknown"
	Score         int            `json:"score"`   // Fire TV hardware compatibility & quality tier score
	Seeds         int            `json:"seeds"`
	Size          int64          `json:"size"`
	FileIdx       int            `json:"fileIdx"` // -1 when unknown
	Trackers      []string       `json:"trackers,omitempty"`
	BehaviorHints map[string]any `json:"behaviorHints,omitempty"`
}

// AdaptivePool groups the classified 4K and 1080p candidates for Adaptive Quality Selection.
type AdaptivePool struct {
	Candidates4K    []Candidate
	Candidates1080p []Candidate
	AllManual       []Candidate
}

// Best4K returns the highest-ranked adaptive 4K candidate, if any.
func (p AdaptivePool) Best4K() (Candidate, bool) {
	if len(p.Candidates4K) > 0 {
		return p.Candidates4K[0], true
	}
	return Candidate{}, false
}

// Best1080p returns the highest-ranked adaptive 1080p candidate, falling back to any valid
// non-4K candidate in AllManual if no strict 25+ seed 1080p candidate matched.
func (p AdaptivePool) Best1080p() (Candidate, bool) {
	if len(p.Candidates1080p) > 0 {
		return p.Candidates1080p[0], true
	}
	for _, c := range p.AllManual {
		if c.Quality == "1080p" || c.Quality == "720p" {
			return c, true
		}
	}
	return Candidate{}, false
}

// BuildAdaptiveStreamMeta builds a Stremio stream name, title, and behaviorHints for the featured
// Auto-Adaptive entry that embeds the real release tags (2160p/4K, WEB-DL, DV/HDR, HEVC, DDP5.1,
// seeds, size) from the top candidate so parsers like AIOStreams/Omni always rank it #1 at the top
// of the UI while clearly labeling it as Auto-Adaptive.
func (p AdaptivePool) BuildAdaptiveStreamMeta() (name, title string, hints map[string]any) {
	ref, ok := p.Best4K()
	if !ok {
		ref, ok = p.Best1080p()
	}
	if !ok && len(p.AllManual) > 0 {
		ref = p.AllManual[0]
		ok = true
	}
	if !ok {
		return AdaptiveStreamName, AdaptiveStreamTitle, nil
	}

	qualTag := "4K HDR"
	if ref.Quality != "" && ref.Quality != "4K" {
		qualTag = ref.Quality
	}
	name = fmt.Sprintf("%s\n%s", AdaptiveStreamName, qualTag)

	// Boost displayed seed count by +1 over the highest candidate so seed-sorting clients also pin
	// the Auto-Adaptive stream at #1.
	maxSeeds := ref.Seeds
	for _, c := range p.AllManual {
		if c.Seeds > maxSeeds {
			maxSeeds = c.Seeds
		}
	}
	refTitle := seedsRe.ReplaceAllString(ref.Title, fmt.Sprintf("👤 %d", maxSeeds+1))
	title = fmt.Sprintf("⚡ [AUTO 4K➔1080p] %s\n%s", refTitle, AdaptiveStreamTitle)

	// Clone behaviorHints with a distinct filename & bingeGroup so AIOStreams' deduplicator never
	// merges or drops the Auto-Adaptive entry against the underlying manual stream.
	hints = map[string]any{
		"bingeGroup": "p2p-bridge-auto-adaptive",
	}
	for k, v := range ref.BehaviorHints {
		hints[k] = v
	}
	if fn, ok := hints["filename"].(string); ok && fn != "" {
		hints["filename"] = "AUTO-ADAPTIVE." + fn
	} else {
		hints["filename"] = "AUTO-ADAPTIVE.2160p.WEB-DL.HEVC.mkv"
	}
	return name, title, hints
}

// Build1080pStreamMeta builds the Stremio stream metadata for the dedicated 1080p Auto-Best option.
func (p AdaptivePool) Build1080pStreamMeta() (name, title string, hints map[string]any, ok bool) {
	ref, found := p.Best1080p()
	if !found {
		return "", "", nil, false
	}
	name = "P2P Auto-Best (1080p)\n1080p"
	title = fmt.Sprintf("⚡ [AUTO 1080p ESTABLE] %s\nElige automáticamente el 1080p más rápido en tiempo real", ref.Title)
	hints = map[string]any{
		"bingeGroup": "p2p-bridge-auto-1080p",
	}
	for k, v := range ref.BehaviorHints {
		hints[k] = v
	}
	if fn, hasFn := hints["filename"].(string); hasFn && fn != "" {
		hints["filename"] = "AUTO-1080P." + fn
	} else {
		hints["filename"] = "AUTO-1080P.1080p.WEB-DL.mkv"
	}
	return name, title, hints, true
}

// IsAdaptive4K reports whether c qualifies as a safe, low-latency 4K candidate for Fire TV
// adaptive streaming:
//   - 4K resolution,
//   - WEB-DL/WEBRip or optimized HEVC/H.265 encode,
//   - no REMUX, no lossless TrueHD/DTS-HD audio, and not an exclusive foreign dub,
//   - size <= 22 GB (or <= 4 GB for series episodes),
//   - seeds >= 30.
func IsAdaptive4K(c Candidate, typ string) bool {
	return isAdaptive4KWithMinSeeds(c, typ, Adaptive4KMinSeeds, false)
}

func isAdaptive4KWithMinSeeds(c Candidate, typ string, minSeeds int, allowForeignDub bool) bool {
	if c.Quality != "4K" || c.Seeds < minSeeds {
		return false
	}
	text := c.Name + "\n" + c.Title
	if remuxRe.MatchString(text) || trueHDRe.MatchString(text) {
		return false
	}
	if !allowForeignDub && IsForeignDubOnly(text) {
		return false
	}
	if !webSrcRe.MatchString(text) && !hevcRe.MatchString(text) && !encoderRe.MatchString(text) {
		return false
	}
	maxBytes := int64(Adaptive4KMaxMovieBytes)
	if typ == "series" {
		maxBytes = DefaultMaxSeriesBytes
	}
	if c.Size > maxBytes {
		return false
	}
	return true
}

// IsAdaptive1080p reports whether c qualifies as a fast-start 1080p candidate for Fire TV
// adaptive streaming:
//   - 1080p resolution,
//   - WEB-DL/WEBRip/BluRay (or high-availability release without REMUX/TrueHD/exclusive foreign dub),
//   - size <= 15 GB (or <= 4 GB for series episodes),
//   - seeds >= 25.
func IsAdaptive1080p(c Candidate, typ string) bool {
	return isAdaptive1080pWithMinSeeds(c, typ, Adaptive1080pMinSeeds, false)
}

func isAdaptive1080pWithMinSeeds(c Candidate, typ string, minSeeds int, allowForeignDub bool) bool {
	if c.Quality != "1080p" || c.Seeds < minSeeds {
		return false
	}
	text := c.Name + "\n" + c.Title
	if remuxRe.MatchString(text) || trueHDRe.MatchString(text) {
		return false
	}
	if !allowForeignDub && IsForeignDubOnly(text) {
		return false
	}
	maxBytes := int64(Adaptive1080pMaxBytes)
	if typ == "series" {
		maxBytes = DefaultMaxSeriesBytes
	}
	if c.Size > maxBytes {
		return false
	}
	return true
}

// ScraperConfig configures the Torrentio client, mirror fallback, TTL cache, and Fire Stick filtering profile.
type ScraperConfig struct {
	BaseURL string
	// FallbackURLs lists alternative Stremio-compatible endpoints tried in order when BaseURL
	// returns HTTP >= 400 (e.g. 403/429) or exceeds EndpointTimeout. Defaults to DefaultFallbackEndpoints[1:].
	FallbackURLs []string
	// EndpointTimeout bounds each individual upstream endpoint attempt before failing over (default 3s).
	EndpointTimeout time.Duration
	// Qualities optionally restricts returned streams to the given canonical qualities
	// ("4K", "1080p", "720p", "480p"). If empty, all non-CAM qualities are kept.
	Qualities []string
	// MinSeeds is the minimum seed count required (defaults to DefaultMinSeeds = 15).
	// Set to a negative value in tests only if you want to disable the seed threshold.
	MinSeeds int
	// MaxMovieBytes caps movie torrent size (defaults to 25 GiB).
	MaxMovieBytes int64
	// MaxSeriesBytes caps series episode size (defaults to 4 GiB).
	MaxSeriesBytes int64
	// AllowCAM disables the default exclusion of CAM/TS/Screener releases when true.
	AllowCAM bool
	// AllowRemux disables the default exclusion of REMUX/BDREMUX releases when true.
	AllowRemux bool
	// CacheTTL sets the in-memory cache lifetime per title (defaults to 12 minutes).
	CacheTTL time.Duration
	// MaxCacheEntries bounds the maximum number of cached titles in RAM (defaults to 64).
	MaxCacheEntries int
	Client          *http.Client
}

// StreamsResponse holds the cached candidate list for a mediaType:imdbID lookup.
type StreamsResponse struct {
	Candidates []Candidate `json:"streams"`
}

// Scraper queries Torrentio (and fallback mirrors) and returns ordered playable streams tailored for Fire TV.
type Scraper struct {
	base            string
	endpoints       []string
	endpointTimeout time.Duration
	allowed         map[string]bool
	minSeeds        int
	maxMovieBytes   int64
	maxSeriesBytes  int64
	allowCAM        bool
	allowRemux      bool
	ttl             time.Duration
	maxCacheEntries int
	hc              *http.Client

	cacheMu sync.RWMutex
	cache   map[string]cacheEntry
}

type cacheEntry struct {
	response  StreamsResponse
	expiresAt time.Time
}

// newBrowserHTTPClient builds an *http.Client with a TLS 1.2/1.3 configuration and cipher suite
// ordering aligned with modern Chrome/Android WebView, without requiring external CGo/uTLS dependencies.
func newBrowserHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS13,
		CurvePreferences: []tls.CurveID{
			tls.X25519,
			tls.CurveP256,
			tls.CurveP384,
		},
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
		},
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsCfg,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   15 * time.Second,
	}
}

// NewScraper creates a Scraper, applying Fire Stick defaults for unset config fields.
func NewScraper(cfg ScraperConfig) *Scraper {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultTorrentioURL
	}
	primary := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	endpoints := []string{primary}
	fallbacks := cfg.FallbackURLs
	if fallbacks == nil {
		fallbacks = DefaultFallbackEndpoints
	}
	seenEP := map[string]bool{primary: true}
	for _, fb := range fallbacks {
		clean := strings.TrimRight(strings.TrimSpace(fb), "/")
		if clean != "" && !seenEP[clean] {
			seenEP[clean] = true
			endpoints = append(endpoints, clean)
		}
	}

	epTimeout := cfg.EndpointTimeout
	if epTimeout <= 0 {
		epTimeout = DefaultEndpointTimeout
	}
	minSeeds := cfg.MinSeeds
	if minSeeds == 0 {
		minSeeds = DefaultMinSeeds
	} else if minSeeds < 0 {
		minSeeds = 0
	}
	maxMovie := cfg.MaxMovieBytes
	if maxMovie <= 0 {
		maxMovie = DefaultMaxMovieBytes
	}
	maxSeries := cfg.MaxSeriesBytes
	if maxSeries <= 0 {
		maxSeries = DefaultMaxSeriesBytes
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = DefaultCacheTTL
	}
	maxEntries := cfg.MaxCacheEntries
	if maxEntries <= 0 {
		maxEntries = DefaultMaxCacheEntries
	}
	if cfg.Client == nil {
		cfg.Client = newBrowserHTTPClient()
	}
	s := &Scraper{
		base:            primary,
		endpoints:       endpoints,
		endpointTimeout: epTimeout,
		allowed:         map[string]bool{},
		minSeeds:        minSeeds,
		maxMovieBytes:   maxMovie,
		maxSeriesBytes:  maxSeries,
		allowCAM:        cfg.AllowCAM,
		allowRemux:      cfg.AllowRemux,
		ttl:             cfg.CacheTTL,
		maxCacheEntries: maxEntries,
		hc:              cfg.Client,
		cache:           make(map[string]cacheEntry, maxEntries),
	}
	for _, q := range cfg.Qualities {
		nq := NormalizeQuality(q)
		if nq != "" && !strings.EqualFold(nq, "all") && nq != "*" {
			s.allowed[nq] = true
		}
	}
	return s
}

// NormalizeQuality maps user input ("2160p", "uhd", "4k") to the canonical label.
func NormalizeQuality(q string) string {
	switch strings.ToLower(strings.TrimSpace(q)) {
	case "4k", "2160p", "uhd":
		return "4K"
	case "1080p", "fhd":
		return "1080p"
	case "720p", "hd":
		return "720p"
	case "480p", "576p", "sd":
		return "480p"
	}
	return strings.TrimSpace(q)
}

// InferTypeFromID determines whether an IMDb ID refers to a "series" (`tt1234567:1:1`) or a "movie" (`tt1234567`).
func InferTypeFromID(id string) string {
	if strings.Count(id, ":") == 2 {
		return "series"
	}
	return "movie"
}

// ScoreCandidate computes the priority tier score for Fire TV Stick + residential connections:
//   - Priority 1 (4000..4350): 4K WEB-DL / WEBRip + HDR10 / DV + HEVC/H.265 (+ bonus for DDP5.1/EAC3/AAC)
//   - Priority 2 (3100..3600): Mini-4K / low-bitrate encoders (PSA, Tigole, QxR, FLUX) or 2160p 10bit HDR HEVC (<= 15 GB)
//   - Priority 3 (2000..2250): 1080p BluRay / 1080p WEB-DL
//   - Priority 4 (500..1000):  720p / 480p / others
func ScoreCandidate(c Candidate) int {
	text := c.Name + "\n" + c.Title
	isWeb := webSrcRe.MatchString(text)
	isBluRay := blurayRe.MatchString(text)
	hasHDR := hdrOrDVRe.MatchString(text)
	hasHDR10Fallback := hdr10Re.MatchString(text)
	isDVP5Only := dvP5Re.MatchString(text) && !hasHDR10Fallback
	isMP4 := mp4FileRe.MatchString(text)
	hasHEVC := hevcRe.MatchString(text)
	hasLightAudio := lightAudRe.MatchString(text)
	hasOptEncoder := encoderRe.MatchString(text)
	hasTrueHD := trueHDRe.MatchString(text)
	hasPrefLang := prefLangRe.MatchString(text)
	isForeignDub := IsForeignDubOnly(text)

	langAdjust := 0
	if isForeignDub {
		langAdjust = -1500 // demote exclusive foreign dubs (RUS/TRUEFRENCH/ITA/GERMAN without MULTi/ENG/SPA)
	} else if hasPrefLang {
		langAdjust = 75 // bonus for explicit MULTi / DUAL / LAT / SPA / ENG tracks
	}

	switch c.Quality {
	case "4K":
		// Penalize 4K streams with heavy lossless TrueHD/DTS-HD audio tracks.
		if hasTrueHD {
			return 2800 + langAdjust
		}
		// Penalize Dolby Vision Profile 5 without HDR10 fallback (causes purple/green tint on
		// non-DV displays/PCs and often comes in MP4 containers with EOF moov atoms).
		if isDVP5Only {
			return 2900 + langAdjust
		}
		// Priority 1: 4K WEB-DL / WEBRip with HDR10 or Dolby Vision and HEVC/H.265.
		if isWeb && hasHDR && hasHEVC {
			score := 4100
			if hasLightAudio {
				score += 200 // 4300: top priority (4K WEB-DL + HDR/DV + HEVC + DDP5.1/EAC3/AAC)
			}
			if hasOptEncoder {
				score += 50
			}
			if isMP4 {
				score -= 100 // slight preference for MKV fast-start over MP4 moov-at-EOF
			}
			return score + langAdjust
		}
		// Priority 2: Mini-4K / recognized low-bitrate encoders (PSA, Tigole, QxR, FLUX) or
		// compact 2160p 10bit/HDR/SDR HEVC encodes (5–16 GB).
		isCompact4K := c.Size > 0 && c.Size <= 16<<30
		if hasOptEncoder || (hasHEVC && (hasHDR || isCompact4K)) {
			score := 3400
			if hasOptEncoder {
				score += 150
			}
			if hasLightAudio {
				score += 100
			}
			if !isMP4 {
				score += 50
			}
			return score + langAdjust
		}
		// Remaining valid 4K streams (under size cap, non-REMUX).
		return 3100 + langAdjust

	case "1080p":
		// Priority 3: 1080p BluRay / 1080p WEB-DL.
		score := 2000
		if isBluRay || isWeb {
			score = 2200
		}
		if hasHEVC || hasOptEncoder {
			score += 50
		}
		return score + langAdjust

	case "720p":
		return max(100, 1000+langAdjust)
	case "480p":
		return max(50, 500+langAdjust)
	default:
		return 100
	}
}

// SortCandidates orders streams by priority tier (Score descending) and, within the same tier,
// by seed count descending.
func SortCandidates(cands []Candidate) {
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].Score != cands[j].Score {
			return cands[i].Score > cands[j].Score
		}
		if cands[i].Seeds != cands[j].Seeds {
			return cands[i].Seeds > cands[j].Seeds
		}
		return cands[i].Size < cands[j].Size
	})
}

// ValidateRequest checks the Stremio type and id (also prevents path injection upstream).
func ValidateRequest(typ, id string) error {
	if !validTypes[typ] {
		return fmt.Errorf("unsupported type %q", typ)
	}
	if !idRe.MatchString(id) {
		return fmt.Errorf("unsupported id %q", id)
	}
	if typ == "series" && strings.Count(id, ":") != 2 {
		return fmt.Errorf("series id must be ttXXXX:season:episode, got %q", id)
	}
	return nil
}

// Classify splits candidates into the Adaptive 4K pool (WEB-DL/HEVC, <= 22 GB, >= 30 seeds,
// no REMUX/TrueHD) and the Adaptive 1080p pool (WEB-DL/BluRay, >= 25 seeds), preserving order.
// When a classic/indie title has 0 strict candidates in a tier, Classify automatically relaxes the
// seed requirement down to ElasticAdaptiveMinSeeds (5 seeds) so niche catalog items still get 4K/1080p.
func Classify(cands []Candidate, typ string) AdaptivePool {
	pool := AdaptivePool{
		AllManual: cands,
	}
	for _, c := range cands {
		switch {
		case IsAdaptive4K(c, typ):
			pool.Candidates4K = append(pool.Candidates4K, c)
		case IsAdaptive1080p(c, typ):
			pool.Candidates1080p = append(pool.Candidates1080p, c)
		}
	}
	// Elastic seed threshold fallback for classic / indie movies & niche series:
	if len(pool.Candidates4K) == 0 {
		for _, c := range cands {
			if isAdaptive4KWithMinSeeds(c, typ, ElasticAdaptiveMinSeeds, false) {
				pool.Candidates4K = append(pool.Candidates4K, c)
			}
		}
	}
	if len(pool.Candidates1080p) == 0 {
		for _, c := range cands {
			if isAdaptive1080pWithMinSeeds(c, typ, ElasticAdaptiveMinSeeds, false) {
				pool.Candidates1080p = append(pool.Candidates1080p, c)
			}
		}
	}
	return pool
}

// SelectRaceLineup builds a diverse lineup of up to maxN candidates for the real-time Swarm Race
// by interleaving the highest-seeded swarms (e.g. scene WEB-DL H265 releases like ETHEL/TBK that
// may be SDR) with the highest-scored HDR/DV candidates. Exclusive foreign dubs (RUS/ITA/FRENCH
// without MULTi/ENG/SPA) are always placed after preferred-language streams even if they have more seeds.
func SelectRaceLineup(cands []Candidate, maxN int) []Candidate {
	if maxN <= 0 || len(cands) <= maxN {
		return cands
	}

	bySeeds := make([]Candidate, len(cands))
	copy(bySeeds, cands)
	sort.SliceStable(bySeeds, func(i, j int) bool {
		fi := IsForeignDubOnly(bySeeds[i].Name + "\n" + bySeeds[i].Title)
		fj := IsForeignDubOnly(bySeeds[j].Name + "\n" + bySeeds[j].Title)
		if fi != fj {
			return !fi
		}
		if bySeeds[i].Seeds != bySeeds[j].Seeds {
			return bySeeds[i].Seeds > bySeeds[j].Seeds
		}
		return bySeeds[i].Score > bySeeds[j].Score
	})

	out := make([]Candidate, 0, maxN)
	seen := make(map[string]bool, maxN)
	add := func(c Candidate) {
		if len(out) < maxN && !seen[c.InfoHash] {
			seen[c.InfoHash] = true
			out = append(out, c)
		}
	}

	for i := 0; i < len(cands) && len(out) < maxN; i++ {
		add(bySeeds[i])
		add(cands[i])
	}
	return out
}

// ClassifyStreams fetches, filters, scores and classifies candidates into 4K and 1080p pools.
func (s *Scraper) ClassifyStreams(ctx context.Context, typ, id string) (AdaptivePool, error) {
	cands, err := s.Streams(ctx, typ, id)
	if err != nil {
		return AdaptivePool{}, err
	}
	return Classify(cands, typ), nil
}

// Streams returns all valid, filtered, and scored candidates from Torrentio.
func (s *Scraper) Streams(ctx context.Context, typ, id string) ([]Candidate, error) {
	cands, err := s.Fetch(ctx, typ, id)
	if err != nil {
		return nil, err
	}
	out := make([]Candidate, 0, len(cands))
	for _, c := range cands {
		if len(s.allowed) > 0 && !s.allowed[c.Quality] {
			continue
		}
		out = append(out, c)
	}
	if len(out) == 0 && len(cands) > 0 {
		out = append(out, cands...)
	}
	SortCandidates(out)
	return out, nil
}

// Best returns the top candidate according to SortCandidates (or false if none exists).
func (s *Scraper) Best(ctx context.Context, typ, id string) (Candidate, bool, error) {
	cands, err := s.Streams(ctx, typ, id)
	if err != nil || len(cands) == 0 {
		return Candidate{}, false, err
	}
	return cands[0], true, nil
}

// Fetch returns all playable candidates (cached for CacheTTL), filtered and sorted for Fire TV.
func (s *Scraper) Fetch(ctx context.Context, typ, id string) ([]Candidate, error) {
	if err := ValidateRequest(typ, id); err != nil {
		return nil, err
	}
	cacheKey := fmt.Sprintf("%s:%s", typ, id)
	now := time.Now()

	// 1. Concurrent-safe read lookup against the in-memory TTL cache.
	s.cacheMu.RLock()
	if e, ok := s.cache[cacheKey]; ok && now.Before(e.expiresAt) {
		cached := make([]Candidate, len(e.response.Candidates))
		copy(cached, e.response.Candidates)
		s.cacheMu.RUnlock()
		return cached, nil
	}
	s.cacheMu.RUnlock()

	// 2. Query ordered endpoints (primary + fallback mirrors) with per-endpoint timeout.
	pathKey := typ + "/" + id
	cands, err := s.fetchAcrossEndpoints(ctx, typ, pathKey)
	if err != nil {
		return nil, err
	}

	SortCandidates(cands)

	// 3. Store in TTL cache and purge expired/overflow entries to keep heap bounded.
	now = time.Now()
	s.cacheMu.Lock()
	s.purgeCacheLocked(now)
	stored := make([]Candidate, len(cands))
	copy(stored, cands)
	s.cache[cacheKey] = cacheEntry{
		response:  StreamsResponse{Candidates: stored},
		expiresAt: now.Add(s.ttl),
	}
	s.cacheMu.Unlock()

	out := make([]Candidate, len(cands))
	copy(out, cands)
	return out, nil
}

// purgeCacheLocked removes expired entries and, if still at capacity, evicts the entry closest to
// expiration so the cache never exceeds s.maxCacheEntries (default 64 titles). s.cacheMu must be held.
func (s *Scraper) purgeCacheLocked(now time.Time) {
	for k, v := range s.cache {
		if !now.Before(v.expiresAt) {
			delete(s.cache, k)
		}
	}
	for len(s.cache) >= s.maxCacheEntries {
		var oldestKey string
		var oldestExp time.Time
		for k, v := range s.cache {
			if oldestKey == "" || v.expiresAt.Before(oldestExp) {
				oldestKey = k
				oldestExp = v.expiresAt
			}
		}
		if oldestKey == "" {
			break
		}
		delete(s.cache, oldestKey)
	}
}

// CacheLen returns the current number of entries in the TTL cache (used in metrics/tests).
func (s *Scraper) CacheLen() int {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	return len(s.cache)
}

func has4KCandidate(cands []Candidate) bool {
	for _, c := range cands {
		if c.Quality == "4K" {
			return true
		}
	}
	return false
}

func (s *Scraper) fetchAcrossEndpoints(ctx context.Context, typ, pathKey string) ([]Candidate, error) {
	var (
		lastErr           error
		anyOK             bool
		merged            []Candidate
		seenHash          = make(map[string]bool)
		enrichmentQueries int
	)
	for _, ep := range s.endpoints {
		if ctx.Err() != nil {
			if len(merged) > 0 {
				return merged, nil
			}
			return nil, ctx.Err()
		}
		epCtx, cancel := context.WithTimeout(ctx, s.endpointTimeout)
		cands, err := s.fetchOnceFromBase(epCtx, ep, typ, pathKey)
		cancel()
		if err == nil {
			anyOK = true
			for _, c := range cands {
				if !seenHash[c.InfoHash] {
					seenHash[c.InfoHash] = true
					merged = append(merged, c)
				}
			}
			// If we already have 4K candidates (or no fallback mirrors configured), return immediately.
			if has4KCandidate(merged) || len(s.endpoints) == 1 {
				return merged, nil
			}
			// If primary returned only 1080p/720p, query at most 1 backup indexer to enrich 4K options.
			if len(merged) > 0 {
				if enrichmentQueries >= 1 {
					return merged, nil
				}
				enrichmentQueries++
			}
			continue
		}
		lastErr = err
		if len(merged) > 0 {
			// Primary succeeded with 1080p and enrichment mirror failed; return primary results.
			return merged, nil
		}
	}
	if anyOK {
		return merged, nil
	}
	return nil, lastErr
}

type torrentioResponse struct {
	Streams []struct {
		Name          string         `json:"name"`
		Title         string         `json:"title"`
		Description   string         `json:"description"`
		InfoHash      string         `json:"infoHash"`
		FileIdx       *int           `json:"fileIdx"`
		Sources       []string       `json:"sources"`
		BehaviorHints map[string]any `json:"behaviorHints"`
	} `json:"streams"`
}

func isValidInfoHash(h string) bool {
	if len(h) != 40 {
		return false
	}
	_, err := hex.DecodeString(h)
	return err == nil
}

func (s *Scraper) maxBytesForType(typ string) int64 {
	if typ == "series" {
		return s.maxSeriesBytes
	}
	return s.maxMovieBytes
}

func applyBrowserHeaders(req *http.Request) {
	h := req.Header
	h.Set("User-Agent", browserUA)
	h.Set("Accept", "application/json, text/plain, */*")
	h.Set("Accept-Language", "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7")
	h.Set("Origin", "https://web.stremio.com")
	h.Set("Referer", "https://web.stremio.com/")
	h.Set("Sec-Ch-Ua", `"Chromium";v="124", "Android WebView";v="124", "Not-A.Brand";v="99"`)
	h.Set("Sec-Ch-Ua-Mobile", "?1")
	h.Set("Sec-Ch-Ua-Platform", `"Android"`)
	h.Set("Sec-Fetch-Dest", "empty")
	h.Set("Sec-Fetch-Mode", "cors")
	h.Set("Sec-Fetch-Site", "cross-site")
}

func (s *Scraper) fetchOnceFromBase(ctx context.Context, base, typ, key string) ([]Candidate, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/stream/"+key+".json", nil)
	if err != nil {
		return nil, err
	}
	applyBrowserHeaders(req)

	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("torrentio request (%s): %w", base, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests:
		return nil, ErrBlocked
	case resp.StatusCode >= 400 || resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("upstream %s returned HTTP %d", base, resp.StatusCode)
	}

	var tr torrentioResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&tr); err != nil {
		return nil, fmt.Errorf("decoding torrentio response (blocked by a challenge page?): %w", err)
	}

	maxBytes := s.maxBytesForType(typ)
	elasticFloor := min(s.minSeeds, ElasticMinSeeds)
	var out, elasticOut []Candidate

	for _, st := range tr.Streams {
		// 1. Discard entries without a valid 40-char hex InfoHash.
		hash := strings.ToLower(strings.TrimSpace(st.InfoHash))
		if !isValidInfoHash(hash) {
			continue
		}
		rawTitle := st.Title
		if rawTitle == "" {
			rawTitle = st.Description
		}
		text := st.Name + "\n" + rawTitle

		// 2. Discard CAM / TeleSync / Screener releases.
		if !s.allowCAM && junkRe.MatchString(text) {
			continue
		}

		// 3. Discard 4K REMUX / BDRemux releases (too high bitrate for 64 MB RAM buffer).
		if !s.allowRemux && remuxRe.MatchString(text) {
			continue
		}

		var seeds int
		if m := seedsRe.FindStringSubmatch(rawTitle); m != nil {
			seeds, _ = strconv.Atoi(m[1])
		}
		// 4. Enforce elastic floor (3+ seeds) and track strict threshold (15+ seeds).
		if seeds < elasticFloor {
			continue
		}

		var size int64
		if m := sizeRe.FindStringSubmatch(rawTitle); m != nil {
			size = parseSize(m[1], m[2])
		}
		if size == 0 && st.BehaviorHints != nil {
			if vs, ok := st.BehaviorHints["videoSize"].(float64); ok && vs > 0 {
				size = int64(vs)
			}
		}
		// 5. Enforce size limit (> 25 GB for movies, > 4 GB for series episodes).
		if maxBytes > 0 && size > maxBytes {
			continue
		}

		quality := detectQuality(text)
		name := formatStreamName(st.Name, text, quality)

		c := Candidate{
			InfoHash:      hash,
			Name:          name,
			Title:         strings.TrimSpace(rawTitle),
			Quality:       quality,
			Seeds:         seeds,
			Size:          size,
			FileIdx:       -1,
			BehaviorHints: st.BehaviorHints,
		}
		if st.FileIdx != nil {
			c.FileIdx = *st.FileIdx
		}
		for _, src := range st.Sources {
			if t, ok := strings.CutPrefix(src, "tracker:"); ok {
				c.Trackers = append(c.Trackers, t)
			}
		}
		c.Score = ScoreCandidate(c)
		if seeds >= s.minSeeds {
			out = append(out, c)
		} else {
			elasticOut = append(elasticOut, c)
		}
	}

	// Elastic Seed Fallback:
	// - If no candidate reached the strict threshold (e.g. classic/indie film with 4–14 seeds),
	//   use the elastic candidates instead of returning an empty list.
	// - If strict candidates exist for 1080p/720p but 0 strict 4K candidates exist, promote any
	//   elastic 4K candidates with >= ElasticAdaptiveMinSeeds (5 seeds) so niche titles still get 4K.
	if len(out) == 0 && len(elasticOut) > 0 {
		return elasticOut, nil
	}
	if len(out) > 0 && !has4KCandidate(out) {
		for _, ec := range elasticOut {
			if ec.Quality == "4K" && ec.Seeds >= ElasticAdaptiveMinSeeds {
				out = append(out, ec)
			}
		}
	}
	return out, nil
}

// formatStreamName preserves Torrentio's name while ensuring key resolution/HDR tags (`4K HDR`,
// `4K DV`, `1080p`, etc.) are present for AIOStreams/Stremio parsers.
func formatStreamName(rawName, fullText, quality string) string {
	name := strings.TrimSpace(rawName)
	if name == "" {
		name = "P2P Local"
		if quality != "unknown" {
			name += " " + quality
		}
	}
	if quality == "4K" && hdrOrDVRe.MatchString(fullText) && !hdrOrDVRe.MatchString(name) {
		name += " HDR"
	}
	return name
}

func detectQuality(text string) string {
	switch {
	case q4kRe.MatchString(text):
		return "4K"
	case q1080Re.MatchString(text):
		return "1080p"
	case q720Re.MatchString(text):
		return "720p"
	case q480Re.MatchString(text):
		return "480p"
	}
	return "unknown"
}

func parseSize(num, unit string) int64 {
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0
	}
	mult := map[string]float64{"KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40}[unit]
	return int64(f * mult)
}
