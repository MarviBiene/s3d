package sia

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"go.uber.org/zap/zapcore"
)

const maxRecentUploadDiagnosticEvents = 250

var (
	shardUploadSuccessRE = regexp.MustCompile(`^slab ([0-9]+) shard ([0-9]+) uploaded to ([^ ]+) in ([^ ]+)$`)
	shardUploadFailureRE = regexp.MustCompile(`^slab ([0-9]+) shard ([0-9]+) upload to host ([^ ]+) failed after ([^ ]+) (.+)$`)
	shardRaceLostRE      = regexp.MustCompile(`^slab ([0-9]+) shard ([0-9]+) upload to host ([^ ]+) lost a race$`)
	shardRaceStartRE     = regexp.MustCompile(`^slab ([0-9]+) shard ([0-9]+) racing slow host with ([^ ]+) after ([^ ]+)$`)
	pinRetryRE           = regexp.MustCompile(`^failed to pin slab on attempt ([0-9]+): (.+); retrying in ([^ ]+)$`)
	slabRetryRE          = regexp.MustCompile(`^slab ([0-9]+) upload attempt ([0-9]+)/([0-9]+) rejected: (.+)$`)
)

type uploadHostRecord struct {
	successes       int64
	failures        int64
	raceLosses      int64
	successDuration time.Duration
	maxSuccess      time.Duration
	lastSeen        time.Time
	lastError       string
	reasons         map[string]int64
}

// SDKUploadDiagnostics records upload host outcomes emitted by the storage SDK.
// The recorder is process-local and intentionally bounded; resetting it never
// changes host state, objects, or persisted data.
type SDKUploadDiagnostics struct {
	mu sync.Mutex

	since time.Time
	hosts map[string]*uploadHostRecord

	recent []s3.UploadDiagnosticEvent

	slowRacesStarted int64
	pinRetries       int64
	slabRetries      int64

	finalFailures       int64
	finalFailureReasons map[string]int64
}

// NewSDKUploadDiagnostics creates an empty upload diagnostics recorder.
func NewSDKUploadDiagnostics() *SDKUploadDiagnostics {
	d := new(SDKUploadDiagnostics)
	d.ResetUploadDiagnostics()
	return d
}

// Core returns a zap core that observes SDK log entries at every level without
// changing the configured output log level.
func (d *SDKUploadDiagnostics) Core() zapcore.Core {
	return uploadDiagnosticsCore{diagnostics: d}
}

type uploadDiagnosticsCore struct {
	diagnostics *SDKUploadDiagnostics
	fields      []zapcore.Field
}

func (c uploadDiagnosticsCore) Enabled(zapcore.Level) bool { return true }

func (c uploadDiagnosticsCore) With(fields []zapcore.Field) zapcore.Core {
	clone := c
	clone.fields = append(append([]zapcore.Field(nil), c.fields...), fields...)
	return clone
}

func (c uploadDiagnosticsCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return checked.AddCore(entry, c)
}

func (c uploadDiagnosticsCore) Write(entry zapcore.Entry, _ []zapcore.Field) error {
	c.diagnostics.recordSDKMessage(entry.Time, entry.Message)
	return nil
}

func (c uploadDiagnosticsCore) Sync() error { return nil }

func parseDiagnosticInt(value string) (int, bool) {
	n, err := strconv.Atoi(value)
	return n, err == nil
}

func parseDiagnosticDuration(value string) time.Duration {
	d, _ := time.ParseDuration(value)
	return d
}

func normalizeUploadFailureReason(reason string) string {
	lower := strings.ToLower(reason)
	switch {
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "timed out"):
		return "timeout"
	case strings.Contains(lower, "connection refused"):
		return "connection refused"
	case strings.Contains(lower, "connection reset"):
		return "connection reset"
	case strings.Contains(lower, "broken pipe"):
		return "broken pipe"
	case strings.Contains(lower, "unexpected eof"), strings.TrimSpace(lower) == "eof":
		return "EOF"
	case strings.Contains(lower, "no more hosts"):
		return "no more hosts"
	case strings.Contains(lower, "insufficient hosts"):
		return "insufficient hosts"
	case strings.Contains(lower, "retry limit"):
		return "retry limit exceeded"
	case strings.Contains(lower, "price"):
		return "pricing/RPC error"
	case strings.Contains(lower, "cancel"):
		return "cancelled"
	}
	reason = strings.TrimSpace(reason)
	if len(reason) > 160 {
		return reason[:157] + "..."
	}
	return reason
}

func (d *SDKUploadDiagnostics) hostLocked(host string) *uploadHostRecord {
	h := d.hosts[host]
	if h == nil {
		h = &uploadHostRecord{reasons: make(map[string]int64)}
		d.hosts[host] = h
	}
	return h
}

func (d *SDKUploadDiagnostics) appendEventLocked(event s3.UploadDiagnosticEvent) {
	if event.At.IsZero() {
		event.At = time.Now()
	}
	d.recent = append(d.recent, event)
	if len(d.recent) > maxRecentUploadDiagnosticEvents {
		copy(d.recent, d.recent[len(d.recent)-maxRecentUploadDiagnosticEvents:])
		d.recent = d.recent[:maxRecentUploadDiagnosticEvents]
	}
}

func (d *SDKUploadDiagnostics) recordSDKMessage(at time.Time, message string) {
	if at.IsZero() {
		at = time.Now()
	}

	if match := shardUploadSuccessRE.FindStringSubmatch(message); match != nil {
		if _, ok := parseDiagnosticInt(match[1]); !ok {
			return
		}
		if _, ok := parseDiagnosticInt(match[2]); !ok {
			return
		}
		elapsed := parseDiagnosticDuration(match[4])

		d.mu.Lock()
		h := d.hostLocked(match[3])
		h.successes++
		h.successDuration += elapsed
		if elapsed > h.maxSuccess {
			h.maxSuccess = elapsed
		}
		h.lastSeen = at
		d.mu.Unlock()
		return
	}

	if match := shardUploadFailureRE.FindStringSubmatch(message); match != nil {
		slab, okSlab := parseDiagnosticInt(match[1])
		shard, okShard := parseDiagnosticInt(match[2])
		if !okSlab || !okShard {
			return
		}
		elapsed := parseDiagnosticDuration(match[4])
		exact := strings.TrimSpace(match[5])
		category := normalizeUploadFailureReason(exact)

		d.mu.Lock()
		h := d.hostLocked(match[3])
		h.failures++
		h.lastSeen = at
		h.lastError = exact
		h.reasons[category]++
		d.appendEventLocked(s3.UploadDiagnosticEvent{
			At: at, Kind: "failure", Host: match[3], SlabIndex: slab, ShardIndex: shard,
			ElapsedMS: elapsed.Milliseconds(), Reason: category, Detail: exact,
		})
		d.mu.Unlock()
		return
	}

	if match := shardRaceLostRE.FindStringSubmatch(message); match != nil {
		slab, okSlab := parseDiagnosticInt(match[1])
		shard, okShard := parseDiagnosticInt(match[2])
		if !okSlab || !okShard {
			return
		}

		d.mu.Lock()
		h := d.hostLocked(match[3])
		h.raceLosses++
		h.lastSeen = at
		d.appendEventLocked(s3.UploadDiagnosticEvent{
			At: at, Kind: "race_lost", Host: match[3], SlabIndex: slab, ShardIndex: shard,
			Reason: "lost race to faster host",
		})
		d.mu.Unlock()
		return
	}

	if match := shardRaceStartRE.FindStringSubmatch(message); match != nil {
		slab, okSlab := parseDiagnosticInt(match[1])
		shard, okShard := parseDiagnosticInt(match[2])
		if !okSlab || !okShard {
			return
		}
		elapsed := parseDiagnosticDuration(match[4])

		d.mu.Lock()
		d.slowRacesStarted++
		d.appendEventLocked(s3.UploadDiagnosticEvent{
			At: at, Kind: "race_started", Host: match[3], SlabIndex: slab, ShardIndex: shard,
			ElapsedMS: elapsed.Milliseconds(), Reason: "racer started for slow shard",
			Detail: "host shown is the replacement racer, not the slow host",
		})
		d.mu.Unlock()
		return
	}

	if match := pinRetryRE.FindStringSubmatch(message); match != nil {
		attempt, ok := parseDiagnosticInt(match[1])
		if !ok {
			return
		}
		exact := strings.TrimSpace(match[2])

		d.mu.Lock()
		d.pinRetries++
		d.appendEventLocked(s3.UploadDiagnosticEvent{
			At: at, Kind: "pin_retry", Attempt: attempt, Reason: normalizeUploadFailureReason(exact), Detail: exact,
		})
		d.mu.Unlock()
		return
	}

	if match := slabRetryRE.FindStringSubmatch(message); match != nil {
		slab, okSlab := parseDiagnosticInt(match[1])
		attempt, okAttempt := parseDiagnosticInt(match[2])
		if !okSlab || !okAttempt {
			return
		}
		exact := strings.TrimSpace(match[4])

		d.mu.Lock()
		d.slabRetries++
		d.appendEventLocked(s3.UploadDiagnosticEvent{
			At: at, Kind: "slab_retry", SlabIndex: slab, ShardIndex: -1, Attempt: attempt,
			Reason: normalizeUploadFailureReason(exact), Detail: exact,
		})
		d.mu.Unlock()
	}
}

// RecordUploadFailure records a whole-upload failure after the SDK returns an
// error to s3d. Host-level attempts remain available separately.
func (d *SDKUploadDiagnostics) RecordUploadFailure(label, phase string, err error) {
	if d == nil || err == nil {
		return
	}
	exact := err.Error()
	category := normalizeUploadFailureReason(exact)

	d.mu.Lock()
	d.finalFailures++
	d.finalFailureReasons[category]++
	d.appendEventLocked(s3.UploadDiagnosticEvent{
		At: time.Now(), Kind: "upload_failed", Label: label, Phase: phase,
		Reason: category, Detail: exact, SlabIndex: -1, ShardIndex: -1,
	})
	d.mu.Unlock()
}

// UploadDiagnostics returns a point-in-time copy of the diagnostics.
func (d *SDKUploadDiagnostics) UploadDiagnostics() s3.UploadDiagnostics {
	if d == nil {
		return s3.UploadDiagnostics{}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	out := s3.UploadDiagnostics{
		Since:            d.since,
		SlowRacesStarted: d.slowRacesStarted,
		PinRetries:       d.pinRetries,
		SlabRetries:      d.slabRetries,
		FinalFailures:    d.finalFailures,
		Recent:           append([]s3.UploadDiagnosticEvent(nil), d.recent...),
	}

	for host, record := range d.hosts {
		stat := s3.UploadHostDiagnostic{
			Host:        host,
			Successes:   record.successes,
			Failures:    record.failures,
			RaceLosses:  record.raceLosses,
			MaxSuccessMS: record.maxSuccess.Milliseconds(),
			LastSeen:    record.lastSeen,
			LastError:   record.lastError,
		}
		if record.successes > 0 {
			stat.AvgSuccessMS = (record.successDuration / time.Duration(record.successes)).Milliseconds()
		}
		for reason, count := range record.reasons {
			stat.Reasons = append(stat.Reasons, s3.UploadDiagnosticReason{Reason: reason, Count: count})
		}
		sort.Slice(stat.Reasons, func(i, j int) bool {
			if stat.Reasons[i].Count == stat.Reasons[j].Count {
				return stat.Reasons[i].Reason < stat.Reasons[j].Reason
			}
			return stat.Reasons[i].Count > stat.Reasons[j].Count
		})
		out.TotalSuccesses += record.successes
		out.TotalFailures += record.failures
		out.TotalRaceLosses += record.raceLosses
		out.Hosts = append(out.Hosts, stat)
	}

	for reason, count := range d.finalFailureReasons {
		out.FinalFailureReasons = append(out.FinalFailureReasons, s3.UploadDiagnosticReason{Reason: reason, Count: count})
	}
	sort.Slice(out.FinalFailureReasons, func(i, j int) bool {
		if out.FinalFailureReasons[i].Count == out.FinalFailureReasons[j].Count {
			return out.FinalFailureReasons[i].Reason < out.FinalFailureReasons[j].Reason
		}
		return out.FinalFailureReasons[i].Count > out.FinalFailureReasons[j].Count
	})
	sort.Slice(out.Hosts, func(i, j int) bool {
		pi := out.Hosts[i].Failures + out.Hosts[i].RaceLosses
		pj := out.Hosts[j].Failures + out.Hosts[j].RaceLosses
		if pi == pj {
			if out.Hosts[i].Failures == out.Hosts[j].Failures {
				return out.Hosts[i].Host < out.Hosts[j].Host
			}
			return out.Hosts[i].Failures > out.Hosts[j].Failures
		}
		return pi > pj
	})

	return out
}

// ResetUploadDiagnostics clears the process-local counters and starts a new
// observation window.
func (d *SDKUploadDiagnostics) ResetUploadDiagnostics() {
	if d == nil {
		return
	}

	d.mu.Lock()
	d.since = time.Now()
	d.hosts = make(map[string]*uploadHostRecord)
	d.recent = nil
	d.slowRacesStarted = 0
	d.pinRetries = 0
	d.slabRetries = 0
	d.finalFailures = 0
	d.finalFailureReasons = make(map[string]int64)
	d.mu.Unlock()
}
