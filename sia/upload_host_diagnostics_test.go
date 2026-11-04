package sia

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestSDKUploadDiagnosticsHostOutcomes(t *testing.T) {
	d := NewSDKUploadDiagnostics()
	log := zap.New(d.Core())
	base := time.Now()

	d.recordSDKMessage(base, "slab 0 shard 1 uploaded to ed25519:good in 1.5s")
	d.recordSDKMessage(base.Add(time.Second), "slab 0 shard 2 upload to host ed25519:bad failed after 6.2s transport timeout")
	d.recordSDKMessage(base.Add(2*time.Second), "slab 0 shard 3 upload to host ed25519:slow lost a race")
	d.recordSDKMessage(base.Add(3*time.Second), "slab 0 shard 4 racing slow host with ed25519:racer after 2.5s")
	d.recordSDKMessage(base.Add(4*time.Second), "failed to pin slab on attempt 1: temporary unavailable; retrying in 250ms")
	d.recordSDKMessage(base.Add(5*time.Second), "slab 0 upload attempt 1/3 rejected: slab upload too old")

	// The zap core must also observe debug events even when used directly.
	log.Debug("slab 1 shard 0 upload to host ed25519:bad failed after 3s connection reset by peer")

	got := d.UploadDiagnostics()
	if got.TotalSuccesses != 1 {
		t.Fatalf("expected 1 success, got %d", got.TotalSuccesses)
	} else if got.TotalFailures != 2 {
		t.Fatalf("expected 2 failures, got %d", got.TotalFailures)
	} else if got.TotalRaceLosses != 1 {
		t.Fatalf("expected 1 race loss, got %d", got.TotalRaceLosses)
	} else if got.SlowRacesStarted != 1 {
		t.Fatalf("expected 1 slow race start, got %d", got.SlowRacesStarted)
	} else if got.PinRetries != 1 {
		t.Fatalf("expected 1 pin retry, got %d", got.PinRetries)
	} else if got.SlabRetries != 1 {
		t.Fatalf("expected 1 slab retry, got %d", got.SlabRetries)
	}

	var good, bad, slow bool
	for _, host := range got.Hosts {
		switch host.Host {
		case "ed25519:good":
			good = true
			if host.Successes != 1 || host.Failures != 0 || host.AvgSuccessMS != 1500 || host.MaxSuccessMS != 1500 {
				t.Fatalf("unexpected good host stats: %+v", host)
			}
		case "ed25519:bad":
			bad = true
			if host.Failures != 2 {
				t.Fatalf("expected 2 failures for bad host, got %+v", host)
			}
			if len(host.Reasons) != 2 {
				t.Fatalf("expected two grouped reasons, got %+v", host.Reasons)
			}
		case "ed25519:slow":
			slow = true
			if host.RaceLosses != 1 {
				t.Fatalf("expected one race loss, got %+v", host)
			}
		}
	}
	if !good || !bad || !slow {
		t.Fatalf("expected good, bad and slow hosts, got %+v", got.Hosts)
	}

	// Successful writes are counted in the host table but intentionally do not
	// fill the recent problem-event feed.
	for _, event := range got.Recent {
		if event.Kind == "success" {
			t.Fatalf("unexpected success in recent problem events: %+v", event)
		}
	}
}

func TestSDKUploadDiagnosticsFinalFailureAndReset(t *testing.T) {
	d := NewSDKUploadDiagnostics()
	d.RecordUploadFailure("slab repack", "finalize", errors.New("not enough shards: 47/48"))

	got := d.UploadDiagnostics()
	if got.FinalFailures != 1 {
		t.Fatalf("expected one final upload failure, got %d", got.FinalFailures)
	} else if len(got.FinalFailureReasons) != 1 {
		t.Fatalf("expected one final failure reason, got %+v", got.FinalFailureReasons)
	} else if len(got.Recent) != 1 || got.Recent[0].Kind != "upload_failed" {
		t.Fatalf("expected one recent upload failure, got %+v", got.Recent)
	}

	before := got.Since
	time.Sleep(time.Millisecond)
	d.ResetUploadDiagnostics()
	got = d.UploadDiagnostics()
	if !got.Since.After(before) {
		t.Fatalf("expected reset to advance observation start: before=%v after=%v", before, got.Since)
	}
	if got.TotalSuccesses != 0 || got.TotalFailures != 0 || got.TotalRaceLosses != 0 || got.FinalFailures != 0 {
		t.Fatalf("expected counters reset, got %+v", got)
	}
	if len(got.Hosts) != 0 || len(got.Recent) != 0 || len(got.FinalFailureReasons) != 0 {
		t.Fatalf("expected detail collections reset, got %+v", got)
	}
}
