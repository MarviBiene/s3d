package sia

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

const (
	transferRateSampleInterval = time.Second
	transferRateWindow         = 5
)

const (
	activeUploadStateUploading int32 = iota
	activeUploadStateFinalizing
)

type activeSiaUpload struct {
	id       uint64
	label    string
	objects  int
	size     int64
	uploaded atomic.Int64
	state    atomic.Int32
}

func (u *activeSiaUpload) stateString() string {
	switch u.state.Load() {
	case activeUploadStateFinalizing:
		return "finalizing"
	default:
		return "uploading"
	}
}

type byteCountingReader struct {
	r       io.Reader
	onBytes func(int64)
}

func (r *byteCountingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.onBytes(int64(n))
	}
	return n, err
}

func (r *byteCountingReader) WriteTo(w io.Writer) (int64, error) {
	if wt, ok := r.r.(io.WriterTo); ok {
		return wt.WriteTo(&byteCountingWriter{w: w, onBytes: r.onBytes})
	}
	return io.Copy(w, readerOnly{Reader: r})
}

type byteCountingReadCloser struct {
	rc      io.ReadCloser
	onBytes func(int64)
}

func (r *byteCountingReadCloser) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if n > 0 {
		r.onBytes(int64(n))
	}
	return n, err
}

func (r *byteCountingReadCloser) WriteTo(w io.Writer) (int64, error) {
	if wt, ok := r.rc.(io.WriterTo); ok {
		return wt.WriteTo(&byteCountingWriter{w: w, onBytes: r.onBytes})
	}
	return io.Copy(w, readerOnly{Reader: r})
}

func (r *byteCountingReadCloser) Close() error {
	return r.rc.Close()
}

type byteCountingWriter struct {
	w       io.Writer
	onBytes func(int64)
}

func (w *byteCountingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n > 0 {
		w.onBytes(int64(n))
	}
	return n, err
}

type readerOnly struct {
	io.Reader
}

func (s *Sia) trackS3Ingress(r io.Reader) (io.Reader, func()) {
	s.s3IngressActive.Add(1)
	tracked := &byteCountingReader{
		r: r,
		onBytes: func(n int64) {
			s.s3IngressBytes.Add(n)
		},
	}
	return tracked, func() {
		s.s3IngressActive.Add(-1)
	}
}

func (s *Sia) beginSiaUpload(group uploadGroup) *activeSiaUpload {
	label := fmt.Sprintf("%d objects", len(group.objects))
	if len(group.objects) == 1 {
		label = group.objects[0].Bucket + "/" + group.objects[0].Name
	}

	u := &activeSiaUpload{
		id:      s.nextSiaUploadID.Add(1),
		label:   label,
		objects: len(group.objects),
		size:    group.totalSize,
	}
	u.state.Store(activeUploadStateUploading)

	s.activeSiaUploadsMu.Lock()
	s.activeSiaUploads[u.id] = u
	s.activeSiaUploadsMu.Unlock()
	return u
}

func (s *Sia) endSiaUpload(u *activeSiaUpload) {
	s.activeSiaUploadsMu.Lock()
	delete(s.activeSiaUploads, u.id)
	s.activeSiaUploadsMu.Unlock()
}

func (s *Sia) trackSiaUploadReader(rc io.ReadCloser, u *activeSiaUpload) io.ReadCloser {
	return &byteCountingReadCloser{
		rc: rc,
		onBytes: func(n int64) {
			s.siaUploadBytes.Add(n)
			u.uploaded.Add(n)
		},
	}
}

func (s *Sia) transferStatsLoop(ctx context.Context) {
	ticker := time.NewTicker(transferRateSampleInterval)
	defer ticker.Stop()

	lastAt := time.Now()
	lastIngress := s.s3IngressBytes.Load()
	lastSia := s.siaUploadBytes.Load()

	var ingressWindow [transferRateWindow]int64
	var siaWindow [transferRateWindow]int64
	var windowIndex, windowCount int
	var ingressSum, siaSum int64

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			elapsed := now.Sub(lastAt)
			if elapsed <= 0 {
				continue
			}

			ingress := s.s3IngressBytes.Load()
			siaUpload := s.siaUploadBytes.Load()
			ingressSample := int64(float64(ingress-lastIngress) / elapsed.Seconds())
			siaSample := int64(float64(siaUpload-lastSia) / elapsed.Seconds())

			if windowCount < transferRateWindow {
				windowCount++
			} else {
				ingressSum -= ingressWindow[windowIndex]
				siaSum -= siaWindow[windowIndex]
			}

			ingressWindow[windowIndex] = ingressSample
			siaWindow[windowIndex] = siaSample
			ingressSum += ingressSample
			siaSum += siaSample
			windowIndex = (windowIndex + 1) % transferRateWindow

			s.s3IngressRate.Store(ingressSum / int64(windowCount))
			s.siaUploadRate.Store(siaSum / int64(windowCount))

			lastAt = now
			lastIngress = ingress
			lastSia = siaUpload
		}
	}
}

func (s *Sia) addTransferStats(stats *s3.UploadStats) {
	t := &s3.TransferStats{
		S3IngressActive: s.s3IngressActive.Load(),
		S3IngressBytes:  s.s3IngressBytes.Load(),
		S3IngressRate:   s.s3IngressRate.Load(),
		SiaUploadBytes:  s.siaUploadBytes.Load(),
		SiaUploadRate:   s.siaUploadRate.Load(),
	}

	s.diskUsageMu.Lock()
	t.BufferUsed = int64(s.diskUsage)
	t.BufferLimit = int64(s.diskUsageLimit)
	s.diskUsageMu.Unlock()

	s.activeSiaUploadsMu.Lock()
	t.ActiveUploads = make([]s3.ActiveUploadStats, 0, len(s.activeSiaUploads))
	for _, u := range s.activeSiaUploads {
		t.ActiveUploads = append(t.ActiveUploads, s3.ActiveUploadStats{
			ID:       u.id,
			Label:    u.label,
			Objects:  u.objects,
			Size:     u.size,
			Uploaded: u.uploaded.Load(),
			State:    u.stateString(),
		})
	}
	s.activeSiaUploadsMu.Unlock()

	sort.Slice(t.ActiveUploads, func(i, j int) bool {
		return t.ActiveUploads[i].ID < t.ActiveUploads[j].ID
	})
	t.SiaUploadActive = int64(len(t.ActiveUploads))
	stats.Transfer = t
}
