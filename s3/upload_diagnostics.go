package s3

import (
	"net/http"
	"time"

	"go.sia.tech/jape"
)

// UploadDiagnosticsProvider supplies process-local SDK upload diagnostics to the
// admin API.
type UploadDiagnosticsProvider interface {
	UploadDiagnostics() UploadDiagnostics
	ResetUploadDiagnostics()
}

// UploadDiagnosticReason is a grouped failure reason and occurrence count.
type UploadDiagnosticReason struct {
	Reason string `json:"reason"`
	Count  int64  `json:"count"`
}

// UploadHostDiagnostic aggregates successful and problematic shard writes for
// one Sia host during the current observation window.
type UploadHostDiagnostic struct {
	Host         string                   `json:"host"`
	Successes    int64                    `json:"successes"`
	Failures     int64                    `json:"failures"`
	RaceLosses   int64                    `json:"raceLosses"`
	AvgSuccessMS int64                    `json:"avgSuccessMs"`
	MaxSuccessMS int64                    `json:"maxSuccessMs"`
	LastSeen     time.Time                `json:"lastSeen"`
	LastError    string                   `json:"lastError,omitempty"`
	Reasons      []UploadDiagnosticReason `json:"reasons,omitempty"`
}

// UploadDiagnosticEvent is one recent SDK upload event.
type UploadDiagnosticEvent struct {
	At          time.Time `json:"at"`
	Kind        string    `json:"kind"`
	Host        string    `json:"host,omitempty"`
	Label       string    `json:"label,omitempty"`
	Phase       string    `json:"phase,omitempty"`
	SlabIndex   int       `json:"slabIndex"`
	ShardIndex  int       `json:"shardIndex"`
	Attempt     int       `json:"attempt,omitempty"`
	ElapsedMS   int64     `json:"elapsedMs,omitempty"`
	Reason      string    `json:"reason,omitempty"`
	Detail      string    `json:"detail,omitempty"`
}

// UploadDiagnostics summarizes host-level and whole-upload diagnostics since
// the last process start or manual reset.
type UploadDiagnostics struct {
	Since               time.Time                `json:"since"`
	TotalSuccesses      int64                    `json:"totalSuccesses"`
	TotalFailures       int64                    `json:"totalFailures"`
	TotalRaceLosses     int64                    `json:"totalRaceLosses"`
	SlowRacesStarted    int64                    `json:"slowRacesStarted"`
	PinRetries          int64                    `json:"pinRetries"`
	SlabRetries         int64                    `json:"slabRetries"`
	FinalFailures       int64                    `json:"finalFailures"`
	FinalFailureReasons []UploadDiagnosticReason `json:"finalFailureReasons,omitempty"`
	Hosts               []UploadHostDiagnostic  `json:"hosts,omitempty"`
	Recent              []UploadDiagnosticEvent `json:"recent,omitempty"`
}

// WithUploadDiagnostics attaches upload diagnostics to the admin handler.
func WithUploadDiagnostics(provider UploadDiagnosticsProvider) Option {
	return func(s *s3) {
		s.uploadDiagnostics = provider
	}
}

func (s *s3) handleUploadDiagnosticsPage(jc jape.Context) {
	jc.ResponseWriter.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = jc.ResponseWriter.Write([]byte(uploadDiagnosticsPageHTML))
}

func (s *s3) handleUploadDiagnosticsData(jc jape.Context) {
	if s.uploadDiagnostics == nil {
		jc.Error(http.ErrNotSupported, http.StatusServiceUnavailable)
		return
	}
	jc.Encode(s.uploadDiagnostics.UploadDiagnostics())
}

func (s *s3) handleResetUploadDiagnostics(jc jape.Context) {
	if s.uploadDiagnostics == nil {
		jc.Error(http.ErrNotSupported, http.StatusServiceUnavailable)
		return
	}
	s.uploadDiagnostics.ResetUploadDiagnostics()
	jc.Encode(s.uploadDiagnostics.UploadDiagnostics())
}

const uploadDiagnosticsPageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>s3d Upload Diagnostics</title>
<style>
:root { color-scheme:dark; font-family:Inter,ui-sans-serif,system-ui,sans-serif; }
body { margin:0; background:#0d1117; color:#e6edf3; }
main { max-width:1250px; margin:0 auto; padding:28px 18px 60px; }
h1 { margin:0 0 6px; font-size:28px; }
h2 { font-size:18px; margin:28px 0 10px; }
p,.muted { color:#9da7b3; line-height:1.5; }
a { color:#58a6ff; }
.top { display:flex; justify-content:space-between; gap:12px; align-items:flex-start; flex-wrap:wrap; }
.actions { display:flex; gap:8px; flex-wrap:wrap; }
button { padding:9px 14px; border-radius:6px; border:1px solid #30363d; background:#21262d; color:#e6edf3; cursor:pointer; font-weight:600; }
button.danger { background:#8b1e2d; border-color:#b62336; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(155px,1fr)); gap:10px; }
.card { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:12px; }
.label { color:#8b949e; font-size:11px; text-transform:uppercase; letter-spacing:.04em; }
.value { font-size:21px; font-weight:650; margin-top:4px; }
.panel { background:#161b22; border:1px solid #30363d; border-radius:10px; padding:14px; }
.tableWrap { overflow-x:auto; -webkit-overflow-scrolling:touch; }
table { width:100%; border-collapse:collapse; min-width:920px; }
th,td { text-align:left; padding:9px 8px; border-bottom:1px solid #30363d; vertical-align:top; font-size:13px; }
th { color:#8b949e; position:sticky; top:0; background:#161b22; }
.bad { color:#ff7b72; font-weight:650; }
.warn { color:#d29922; }
.good { color:#3fb950; }
.host { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; white-space:nowrap; }
.reason { display:inline-block; border:1px solid #30363d; border-radius:999px; padding:2px 7px; margin:1px 3px 2px 0; }
.event { padding:9px 0; border-bottom:1px solid #21262d; font-size:13px; }
.event:last-child { border-bottom:0; }
.eventMeta { color:#8b949e; margin-bottom:3px; }
.detail { color:#c9d1d9; word-break:break-word; }
.note { font-size:13px; color:#8b949e; }
@media (max-width:650px) {
  main { padding:20px 12px 48px; }
  h1 { font-size:24px; }
  .value { font-size:18px; }
}
</style>
</head>
<body>
<main>
<div class="top">
  <div>
    <h1>Upload diagnostics</h1>
    <p>Host-level shard outcomes and upload failure causes observed by this s3d process.</p>
  </div>
  <div class="actions">
    <button onclick="location.href='/repack'">Repacker</button>
    <button id="refresh">Refresh</button>
    <button id="reset" class="danger">Reset stats</button>
  </div>
</div>

<div id="summary" class="grid"></div>
<p id="window" class="note"></p>

<h2>Hosts</h2>
<div class="panel tableWrap">
<table>
<thead><tr>
  <th>Host</th><th>Success</th><th>Failures</th><th>Lost races</th><th>Failure %</th>
  <th>Avg success</th><th>Max success</th><th>Reasons</th><th>Last error</th>
</tr></thead>
<tbody id="hosts"><tr><td colspan="9" class="muted">Loading…</td></tr></tbody>
</table>
</div>
<p class="note">A lost race means the host was slower than another successful attempt. It is tracked separately from an RPC failure. “Race started” cannot identify the original slow host until a winner is known.</p>

<h2>Whole-upload failures</h2>
<div class="panel" id="finalFailures">None</div>

<h2>Recent events</h2>
<div class="panel" id="events">Loading…</div>
<p id="error" class="bad"></p>

<script>
const $=id=>document.getElementById(id);
function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g,ch=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[ch]));
}
function card(label,value) { return '<div class="card"><div class="label">'+esc(label)+'</div><div class="value">'+esc(value)+'</div></div>'; }
function shortHost(v) { v=String(v||""); return v.length>24 ? v.slice(0,12)+"…"+v.slice(-10) : v; }
function ms(v) {
  if (!Number.isFinite(v) || v<=0) return "-";
  if (v<1000) return Math.round(v)+" ms";
  if (v<60000) return (v/1000).toFixed(1)+" s";
  return (v/60000).toFixed(1)+" min";
}
function when(v) { if (!v) return "-"; const d=new Date(v); return Number.isNaN(d.getTime()) ? "-" : d.toLocaleString(); }
function reasons(rs) {
  return (rs||[]).length ? rs.map(r=>'<span class="reason">'+esc(r.reason)+' × '+esc(r.count)+'</span>').join("") : "-";
}
async function json(url,init) {
  const r=await fetch(url,init);
  const t=await r.text();
  if (!r.ok) throw new Error(t||r.statusText);
  return t ? JSON.parse(t) : {};
}
function eventTitle(e) {
  const names={success:"success",failure:"RPC failure",race_lost:"lost race",race_started:"race started",pin_retry:"pin retry",slab_retry:"slab retry",upload_failed:"UPLOAD FAILED"};
  return names[e.kind]||e.kind;
}
async function load() {
  $("error").textContent="";
  try {
    const d=await json("/diagnostics/uploads/data");
    $("summary").innerHTML=
      card("Successful shard writes",(d.totalSuccesses||0).toLocaleString())+
      card("RPC failures",(d.totalFailures||0).toLocaleString())+
      card("Lost slow-host races",(d.totalRaceLosses||0).toLocaleString())+
      card("Slow races started",(d.slowRacesStarted||0).toLocaleString())+
      card("Slab retries",(d.slabRetries||0).toLocaleString())+
      card("Pin retries",(d.pinRetries||0).toLocaleString())+
      card("Whole uploads failed",(d.finalFailures||0).toLocaleString());
    $("window").textContent="Observation window since "+when(d.since)+" · stats are in-memory and reset on process restart.";

    const hosts=d.hosts||[];
    $("hosts").innerHTML=hosts.length ? hosts.map(h=>{
      const denom=(h.successes||0)+(h.failures||0);
      const pct=denom ? 100*(h.failures||0)/denom : 0;
      const cls=(h.failures||0)>0 ? "bad" : ((h.raceLosses||0)>0 ? "warn" : "good");
      return '<tr>'+
        '<td class="host" title="'+esc(h.host)+'">'+esc(shortHost(h.host))+'</td>'+
        '<td class="good">'+esc(h.successes||0)+'</td>'+
        '<td class="'+cls+'">'+esc(h.failures||0)+'</td>'+
        '<td class="'+((h.raceLosses||0)>0?"warn":"")+'">'+esc(h.raceLosses||0)+'</td>'+
        '<td>'+esc(pct.toFixed(2))+'%</td>'+
        '<td>'+esc(ms(h.avgSuccessMs||0))+'</td>'+
        '<td>'+esc(ms(h.maxSuccessMs||0))+'</td>'+
        '<td>'+reasons(h.reasons)+'</td>'+
        '<td class="detail">'+esc(h.lastError||"-")+'</td>'+
        '</tr>';
    }).join("") : '<tr><td colspan="9" class="muted">No host outcomes recorded yet.</td></tr>';

    $("finalFailures").innerHTML=(d.finalFailureReasons||[]).length
      ? reasons(d.finalFailureReasons)
      : '<span class="muted">No whole-upload failures recorded.</span>';

    const ev=(d.recent||[]).slice().reverse();
    $("events").innerHTML=ev.length ? ev.map(e=>{
      const loc=(e.slabIndex>=0 ? 'slab '+e.slabIndex : '')+(e.shardIndex>=0 ? ' · shard '+e.shardIndex : '');
      const host=e.host ? ' · host '+shortHost(e.host) : '';
      const elapsed=e.elapsedMs ? ' · '+ms(e.elapsedMs) : '';
      const attempt=e.attempt ? ' · attempt '+e.attempt : '';
      const label=e.label ? ' · '+e.label+(e.phase?' / '+e.phase:'') : '';
      return '<div class="event"><div class="eventMeta">'+esc(when(e.at))+' · <b>'+esc(eventTitle(e))+'</b>'+
        esc(label)+(loc?' · '+esc(loc):'')+esc(host)+esc(attempt)+esc(elapsed)+'</div>'+
        (e.reason||e.detail ? '<div class="detail">'+esc(e.reason||"")+(e.detail && e.detail!==e.reason ? ' · '+esc(e.detail) : '')+'</div>' : '')+
        '</div>';
    }).join("") : '<span class="muted">No diagnostic events recorded yet.</span>';
  } catch(e) {
    $("error").textContent=e.message;
  }
}
$("refresh").onclick=load;
$("reset").onclick=async()=>{
  if (!confirm("Reset the in-memory upload diagnostics counters? This does not change any Sia data or host state.")) return;
  try { await json("/diagnostics/uploads/reset",{method:"POST"}); await load(); }
  catch(e) { $("error").textContent=e.message; }
};
load();
setInterval(load,5000);
</script>
</main>
</body>
</html>`
