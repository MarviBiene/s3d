package s3

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/SiaFoundation/s3d/internal/prometheus"
	"go.sia.tech/jape"
)

// BackupSQLite3Request is the request body for the [POST] /system/sqlite3/backup
// endpoint.
type BackupSQLite3Request struct {
	// Path is the absolute filesystem path where the backup file will be
	// written. It must not already exist.
	Path string `json:"path"`
}

// ActiveUploadStats reports progress for one upload group currently being
// streamed from the local buffer into the Sia SDK.
type ActiveUploadStats struct {
	ID       uint64 `json:"id"`
	Label    string `json:"label"`
	Objects  int    `json:"objects"`
	Size     int64  `json:"size"`
	Uploaded int64  `json:"uploaded"`
	State    string `json:"state"`
}

// UploadStats contains statistics about the background upload pipeline and
// live transfer activity. Transfer byte totals are process-lifetime counters.
type UploadStats struct {
	PendingObjects   int64 `json:"pendingObjects"`
	PendingSize      int64 `json:"pendingSize"`
	UploadedObjects  int64 `json:"uploadedObjects"`
	UploadedSize     int64 `json:"uploadedSize"`
	UnpinnedObjects  int64 `json:"unpinnedObjects"`
	FailedUploads    int64 `json:"failedUploads"`
	OrphanedObjects  int64 `json:"orphanedObjects"`
	MultipartUploads int64 `json:"multipartUploads"`

	S3IngressActive int64 `json:"s3IngressActive"`
	S3IngressBytes  int64 `json:"s3IngressBytes"`
	S3IngressRate   int64 `json:"s3IngressRate"`

	SiaUploadActive int64 `json:"siaUploadActive"`
	SiaUploadBytes  int64 `json:"siaUploadBytes"`
	SiaUploadRate   int64 `json:"siaUploadRate"`

	BufferUsed  int64 `json:"bufferUsed"`
	BufferLimit int64 `json:"bufferLimit"`

	ActiveUploads []ActiveUploadStats `json:"activeUploads,omitempty"`
}

// PrometheusMetric implements the prometheus.Marshaller interface for the
// upload stats response.
func (s UploadStats) PrometheusMetric() []prometheus.Metric {
	metrics := []prometheus.Metric{
		{
			Name:  "s3d_upload_pending_objects",
			Value: float64(s.PendingObjects),
		},
		{
			Name:  "s3d_upload_pending_size_bytes",
			Value: float64(s.PendingSize),
		},
		{
			Name:  "s3d_upload_uploaded_objects",
			Value: float64(s.UploadedObjects),
		},
		{
			Name:  "s3d_upload_uploaded_size_bytes",
			Value: float64(s.UploadedSize),
		},
		{
			Name:  "s3d_upload_unpinned_objects",
			Value: float64(s.UnpinnedObjects),
		},
		{
			Name:  "s3d_upload_failed_uploads",
			Value: float64(s.FailedUploads),
		},
		{
			Name:  "s3d_upload_orphaned_objects",
			Value: float64(s.OrphanedObjects),
		},
		{
			Name:  "s3d_upload_multipart_uploads",
			Value: float64(s.MultipartUploads),
		},
		{
			Name:  "s3d_transfer_s3_ingress_active",
			Value: float64(s.S3IngressActive),
		},
		{
			Name:  "s3d_transfer_s3_ingress_bytes_total",
			Value: float64(s.S3IngressBytes),
		},
		{
			Name:  "s3d_transfer_s3_ingress_bytes_per_second",
			Value: float64(s.S3IngressRate),
		},
		{
			Name:  "s3d_transfer_sia_upload_active",
			Value: float64(s.SiaUploadActive),
		},
		{
			Name:  "s3d_transfer_sia_upload_bytes_total",
			Value: float64(s.SiaUploadBytes),
		},
		{
			Name:  "s3d_transfer_sia_upload_bytes_per_second",
			Value: float64(s.SiaUploadRate),
		},
		{
			Name:  "s3d_upload_buffer_used_bytes",
			Value: float64(s.BufferUsed),
		},
		{
			Name:  "s3d_upload_buffer_limit_bytes",
			Value: float64(s.BufferLimit),
		},
	}
	for _, upload := range s.ActiveUploads {
		labels := map[string]any{
			"id":      fmt.Sprint(upload.ID),
			"state":   upload.State,
			"objects": upload.Objects,
		}
		metrics = append(metrics,
			prometheus.Metric{
				Name:   "s3d_transfer_sia_active_upload_size_bytes",
				Labels: labels,
				Value:  float64(upload.Size),
			},
			prometheus.Metric{
				Name:   "s3d_transfer_sia_active_upload_uploaded_bytes",
				Labels: labels,
				Value:  float64(upload.Uploaded),
			},
		)
	}
	return metrics
}

// handlePrometheus serves the admin API metrics in the Prometheus text
// exposition format. Currently the only metrics exposed are the background
// upload stats.
func (s *s3) handlePrometheus(jc jape.Context) {
	stats, err := s.backend.UploadStats(jc.Request.Context())
	if jc.Check("failed to get upload stats", err) != nil {
		return
	}

	jc.ResponseWriter.Header().Set("Content-Type", "text/plain; version=0.0.4")
	if jc.Check("failed to marshal prometheus response", prometheus.NewEncoder(jc.ResponseWriter).Append(stats)) != nil {
		return
	}
}

// handleGetUploadStats serves the background upload pipeline stats as JSON.
func (s *s3) handleGetUploadStats(jc jape.Context) {
	stats, err := s.backend.UploadStats(jc.Request.Context())
	if jc.Check("failed to get upload stats", err) != nil {
		return
	}
	jc.Encode(stats)
}

// handleFlushObjects flushes all pending objects to Sia via Backend.FlushObjects.
func (s *s3) handleFlushObjects(jc jape.Context) {
	jc.Check("failed to flush objects", s.backend.FlushObjects(jc.Request.Context()))
}

// handleBackupSQLite3 creates a backup of the SQLite3 database at the path
// provided in the request body. The backup is a consistent snapshot even if
// the database is being written to concurrently.
func (s *s3) handleBackupSQLite3(jc jape.Context) {
	var req BackupSQLite3Request
	if jc.Decode(&req) != nil {
		return
	} else if req.Path == "" {
		jc.Error(fmt.Errorf("path must not be empty"), http.StatusBadRequest)
		return
	} else if !filepath.IsAbs(req.Path) {
		jc.Error(fmt.Errorf("path must be absolute: %q", req.Path), http.StatusBadRequest)
		return
	} else if _, err := os.Stat(req.Path); err == nil {
		jc.Error(fmt.Errorf("destination already exists: %q", req.Path), http.StatusBadRequest)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		jc.Error(fmt.Errorf("failed to stat destination: %w", err), http.StatusBadRequest)
		return
	}
	jc.Check("failed to backup database", s.backend.BackupSQLite3(jc.Request.Context(), req.Path))
}
