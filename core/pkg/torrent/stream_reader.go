package torrent

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	anacrolix "github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
)

// PlayableStream is the interface satisfied by *Stream (and test doubles) for HTTP Range serving
// and adaptive pre-buffering.
type PlayableStream interface {
	io.ReadSeekCloser
	FileName() string
	FileSize() int64
	Prebuffer(ctx context.Context, startOffset, minBytes int64, timeout time.Duration) error
}

// Stream is an io.ReadSeekCloser over a single torrent file, suitable for http.ServeContent (and
// therefore for `Range: bytes=...` requests). Reads block until the data is available; seeks
// re-aim the sequential download window at the new position.
//
// Its underlying anacrolix.Reader is decoupled from the ephemeral HTTP request context until an
// explicit abort occurs, and Prebuffer warms the initial chunk in RAM before HTTP headers are sent.
type Stream struct {
	// Name is the base name of the streamed file; Size its length in bytes.
	Name string
	Size int64

	eng       *Engine
	sess      *session
	file      *anacrolix.File
	ih        metainfo.Hash
	r         anacrolix.Reader
	reqCtx    context.Context
	rCtx      context.Context
	rCancel   context.CancelFunc
	base      int64 // file offset inside the torrent
	pieceLen  int64
	raBytes   int64
	raPieces  int
	promoted  bool

	pos       int64
	lastPiece int
	closeOnce sync.Once
}

var _ PlayableStream = (*Stream)(nil)

// FileName returns the base name of the streamed video file.
func (s *Stream) FileName() string { return s.Name }

// FileSize returns the total length in bytes of the streamed video file.
func (s *Stream) FileSize() int64 { return s.Size }

// promoteToFullStream activates the full 48 MiB sequential readahead and pins the header/tail
// index pieces (MP4 moov / MKV cues) once this stream has won the prebuffer race. Keeping readahead
// minimal (1 piece) before Prebuffer completes ensures racing candidates don't waste bandwidth on
// future/EOF pieces before Piece 0 is verified.
func (s *Stream) promoteToFullStream() {
	if s.promoted {
		return
	}
	s.promoted = true
	if s.r != nil && s.raBytes > 0 {
		s.r.SetReadahead(s.raBytes)
	}
	if s.sess != nil && s.file != nil && s.eng != nil {
		s.sess.pinHeadPieces(s.file, s.pieceLen, max(s.eng.cfg.PrebufferBytes*3, 6<<20))
	}
}

// Prebuffer ensures at least minBytes (capped by file size) starting at startOffset
// are downloaded into the RAM cache within timeout before HTTP response headers are written.
// Afterward, it rewinds the reader back to 0 so http.ServeContent can seek/read normally.
func (s *Stream) Prebuffer(ctx context.Context, startOffset, minBytes int64, timeout time.Duration) error {
	if s.Size <= 0 || minBytes <= 0 {
		return nil
	}
	if startOffset < 0 || startOffset >= s.Size {
		startOffset = 0
	}
	remaining := s.Size - startOffset
	if minBytes > remaining {
		minBytes = remaining
	}
	if _, err := s.Seek(startOffset, io.SeekStart); err != nil {
		return err
	}

	pbCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Cancel the underlying reader if pbCtx expires during prebuffering.
	done := make(chan struct{})
	go func() {
		select {
		case <-pbCtx.Done():
			select {
			case <-done:
			default:
				s.rCancel()
			}
		case <-done:
		}
	}()

	buf := make([]byte, 64<<10)
	var total int64
	var readErr error
	for total < minBytes {
		toRead := int64(len(buf))
		if rem := minBytes - total; rem < toRead {
			toRead = rem
		}
		n, err := s.r.Read(buf[:toRead])
		if n > 0 {
			total += int64(n)
			s.pos += int64(n)
			s.updatePlayhead()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			readErr = err
			break
		}
	}
	close(done)

	if readErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(pbCtx.Err(), context.DeadlineExceeded) {
			return ErrPrebufferTimeout
		}
		return readErr
	}

	// Now that initial piece verification succeeded, activate the full 48 MiB sequential readahead
	// and tail piece pinning for the winning stream.
	s.promoteToFullStream()

	// Reset position to the start of the file so http.ServeContent can perform its own Seek/Read.
	_, err := s.Seek(0, io.SeekStart)
	return err
}

func (s *Stream) Read(p []byte) (int, error) {
	// Fast exit before calling into anacrolix if the HTTP client already disconnected.
	if err := s.reqCtx.Err(); err != nil {
		return 0, err
	}
	s.promoteToFullStream()

	// Watch reqCtx while blocked in r.Read so that a mid-read client disconnect unblocks promptly.
	done := make(chan struct{})
	go func() {
		select {
		case <-s.reqCtx.Done():
			select {
			case <-done:
			default:
				s.rCancel()
			}
		case <-done:
		}
	}()

	n, err := s.r.Read(p)
	close(done)

	s.pos += int64(n)
	s.updatePlayhead()
	if err != nil && s.reqCtx.Err() != nil {
		return n, s.reqCtx.Err()
	}
	return n, err
}

func (s *Stream) Seek(offset int64, whence int) (int64, error) {
	if err := s.reqCtx.Err(); err != nil {
		return s.pos, err
	}
	pos, err := s.r.Seek(offset, whence)
	if err == nil {
		s.pos = pos
		s.updatePlayhead()
	}
	return pos, err
}

// Close closes only this HTTP request's Reader; the underlying torrent session remains warm in RAM
// for IdleTimeout so subsequent requests from the video player reuse downloaded pieces.
func (s *Stream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.rCancel()
		err = s.r.Close()
		s.eng.release(s.sess)
	})
	return err
}

func (s *Stream) updatePlayhead() {
	if s.pieceLen <= 0 {
		return
	}
	piece := int((s.base + s.pos) / s.pieceLen)
	if piece == s.lastPiece {
		return
	}
	s.lastPiece = piece
	s.eng.store.SetPlayhead(s.ih, piece, s.raPieces)
}
