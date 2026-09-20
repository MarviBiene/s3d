package sia

import (
	"bytes"
	"io"
	"testing"

	"github.com/SiaFoundation/s3d/sia/objects"
)

func TestTrackS3Ingress(t *testing.T) {
	s := &Sia{}
	r, done := s.trackS3Ingress(bytes.NewReader(make([]byte, 1024)))
	if got := s.s3IngressActive.Load(); got != 1 {
		t.Fatalf("expected 1 active ingress stream, got %d", got)
	}

	n, err := io.Copy(io.Discard, r)
	if err != nil {
		t.Fatal(err)
	} else if n != 1024 {
		t.Fatalf("expected 1024 bytes, got %d", n)
	} else if got := s.s3IngressBytes.Load(); got != 1024 {
		t.Fatalf("expected 1024 tracked ingress bytes, got %d", got)
	}

	done()
	if got := s.s3IngressActive.Load(); got != 0 {
		t.Fatalf("expected no active ingress streams, got %d", got)
	}
}

func TestActiveSiaUploadProgress(t *testing.T) {
	s := &Sia{
		activeSiaUploads: make(map[uint64]*activeSiaUpload),
	}
	group := uploadGroup{
		objects: []objects.ObjectForUpload{{
			Bucket: "bucket",
			Name:   "large.img",
			Length: 2048,
		}},
		totalSize: 2048,
	}

	active := s.beginSiaUpload(group)
	if active.label != "bucket/large.img" {
		t.Fatalf("unexpected upload label %q", active.label)
	}

	rc := io.NopCloser(bytes.NewReader(make([]byte, 768)))
	tracked := s.trackSiaUploadReader(rc, active)
	n, err := io.Copy(io.Discard, tracked)
	if err != nil {
		t.Fatal(err)
	} else if n != 768 {
		t.Fatalf("expected 768 bytes, got %d", n)
	}
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}

	var stats struct {
		active int64
		bytes  int64
	}
	stats.active = int64(len(s.activeSiaUploads))
	stats.bytes = s.siaUploadBytes.Load()
	if stats.active != 1 {
		t.Fatalf("expected 1 active Sia upload, got %d", stats.active)
	} else if stats.bytes != 768 {
		t.Fatalf("expected 768 tracked Sia bytes, got %d", stats.bytes)
	} else if got := active.uploaded.Load(); got != 768 {
		t.Fatalf("expected 768 upload-progress bytes, got %d", got)
	}

	active.state.Store(activeUploadStateFinalizing)
	if got := active.stateString(); got != "finalizing" {
		t.Fatalf("expected finalizing state, got %q", got)
	}

	s.endSiaUpload(active)
	if len(s.activeSiaUploads) != 0 {
		t.Fatal("expected active upload to be removed")
	}
}
