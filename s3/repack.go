package s3

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"go.sia.tech/jape"
)

var (
	// ErrRepackRunning is returned when a second repack is started while one is
	// already active.
	ErrRepackRunning = errors.New("a slab repack is already running")
	// ErrNoRepackCandidates is returned when the selected limits do not produce
	// any safe objects to rewrite.
	ErrNoRepackCandidates = errors.New("no slabs match the repack limits")
)

// RepackOptions controls which sparse slabs are selected for repacking.
type RepackOptions struct {
	MaxSlabFillPercent float64 `json:"maxSlabFillPercent"`
	MaxObjectSize      int64   `json:"maxObjectSize"`
	ReuploadBudget     int64   `json:"reuploadBudget"`
}

// RepackPreview reports current slab waste and a conservative plan produced by
// the supplied limits. Reclaim estimates count waste in slabs whose complete
// set of live objects can be rewritten within the plan.
type RepackPreview struct {
	Options RepackOptions `json:"options"`

	Slabs            int     `json:"slabs"`
	LogicalCapacity  int64   `json:"logicalCapacity"`
	LiveBytes        int64   `json:"liveBytes"`
	LogicalWaste     int64   `json:"logicalWaste"`
	WastePercent     float64 `json:"wastePercent"`
	PhysicalCapacity int64   `json:"physicalCapacity"`
	PhysicalWaste    int64   `json:"physicalWaste"`

	CandidateSlabs           int   `json:"candidateSlabs"`
	SelectedSlabs            int   `json:"selectedSlabs"`
	ObjectsToRewrite         int   `json:"objectsToRewrite"`
	RewriteBytes             int64 `json:"rewriteBytes"`
	EstimatedLogicalReclaim  int64 `json:"estimatedLogicalReclaim"`
	EstimatedPhysicalReclaim int64 `json:"estimatedPhysicalReclaim"`

	BusySlabs          int `json:"busySlabs"`
	SkippedLargeSlabs  int `json:"skippedLargeSlabs"`
	SkippedBudgetSlabs int `json:"skippedBudgetSlabs"`
}

// RepackStatus reports the lifecycle and progress of the in-memory repack job.
// Each object swap is protected by an all-or-nothing metadata transaction, so
// cancellation before that point leaves the old object references intact.
type RepackStatus struct {
	State      string        `json:"state"`
	Phase      string        `json:"phase,omitempty"`
	Options    RepackOptions `json:"options"`
	StartedAt  time.Time     `json:"startedAt,omitempty"`
	FinishedAt time.Time     `json:"finishedAt,omitempty"`

	ObjectsTotal       int    `json:"objectsTotal"`
	ObjectsCompleted   int    `json:"objectsCompleted"`
	CurrentObject      string `json:"currentObject,omitempty"`
	CurrentObjectID    string `json:"currentObjectID,omitempty"`
	CurrentObjectIndex int    `json:"currentObjectIndex,omitempty"`
	CurrentObjectSize  int64  `json:"currentObjectSize,omitempty"`
	RewriteBytes       int64  `json:"rewriteBytes"`
	RewrittenBytes     int64  `json:"rewrittenBytes"`

	EstimatedLogicalReclaim  int64  `json:"estimatedLogicalReclaim"`
	EstimatedPhysicalReclaim int64  `json:"estimatedPhysicalReclaim"`
	Error                    string `json:"error,omitempty"`
}

func repackOptionsFromQuery(r *http.Request) (opts RepackOptions, err error) {
	q := r.URL.Query()
	if value := q.Get("maxSlabFillPercent"); value != "" {
		opts.MaxSlabFillPercent, err = strconv.ParseFloat(value, 64)
		if err != nil {
			return opts, fmt.Errorf("invalid maxSlabFillPercent: %w", err)
		}
	}
	if value := q.Get("maxObjectSize"); value != "" {
		opts.MaxObjectSize, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return opts, fmt.Errorf("invalid maxObjectSize: %w", err)
		}
	}
	if value := q.Get("reuploadBudget"); value != "" {
		opts.ReuploadBudget, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return opts, fmt.Errorf("invalid reuploadBudget: %w", err)
		}
	}
	return opts, nil
}

func (s *s3) handleRepackPage(jc jape.Context) {
	jc.ResponseWriter.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = jc.ResponseWriter.Write([]byte(repackPageHTML))
}

func (s *s3) handleRepackPreview(jc jape.Context) {
	opts, err := repackOptionsFromQuery(jc.Request)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}
	preview, err := s.backend.RepackPreview(jc.Request.Context(), opts)
	if jc.Check("failed to preview slab repack", err) != nil {
		return
	}
	jc.Encode(preview)
}

func (s *s3) handleRepackStatus(jc jape.Context) {
	status, err := s.backend.RepackStatus(jc.Request.Context())
	if jc.Check("failed to get slab repack status", err) != nil {
		return
	}
	jc.Encode(status)
}

func (s *s3) handleStartRepack(jc jape.Context) {
	var opts RepackOptions
	if jc.Decode(&opts) != nil {
		return
	}
	status, err := s.backend.StartRepack(jc.Request.Context(), opts)
	switch {
	case errors.Is(err, ErrRepackRunning):
		jc.Error(err, http.StatusConflict)
		return
	case errors.Is(err, ErrNoRepackCandidates):
		jc.Error(err, http.StatusBadRequest)
		return
	case err != nil:
		jc.Error(fmt.Errorf("failed to start slab repack: %w", err), http.StatusInternalServerError)
		return
	}
	jc.Encode(status)
}

func (s *s3) handleCancelRepack(jc jape.Context) {
	status, err := s.backend.CancelRepack(jc.Request.Context())
	if jc.Check("failed to cancel slab repack", err) != nil {
		return
	}
	jc.Encode(status)
}

const repackPageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>s3d Slab Repacker</title>
<style>
:root { color-scheme: dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }
body { margin:0; background:#0d1117; color:#e6edf3; }
main { max-width:1100px; margin:0 auto; padding:32px 20px 64px; }
h1 { margin:0 0 6px; font-size:28px; }
h2 { font-size:18px; margin:30px 0 12px; }
p { color:#9da7b3; line-height:1.5; }
.panel { background:#161b22; border:1px solid #30363d; border-radius:10px; padding:18px; margin-top:16px; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }
.card { background:#0d1117; border:1px solid #30363d; border-radius:8px; padding:13px; }
.cardLink { display:block; color:inherit; text-decoration:none; transition:border-color .15s, background .15s; }
.cardLink:hover,.cardLink:focus { border-color:#58a6ff; background:#111820; outline:none; }
.cardHint { color:#58a6ff; font-size:12px; margin-top:7px; }
.label { color:#8b949e; font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
.value { font-size:22px; font-weight:650; margin-top:5px; }
.controls { display:grid; grid-template-columns:repeat(auto-fit,minmax(230px,1fr)); gap:14px; align-items:start; }
label { display:block; color:#c9d1d9; font-size:13px; }
.controlTitle { display:block; font-weight:600; }
.help { display:block; margin-top:8px; color:#8b949e; font-size:12px; line-height:1.45; }
input { margin-top:7px; width:100%; box-sizing:border-box; padding:9px 10px; border-radius:6px; border:1px solid #30363d; background:#0d1117; color:#e6edf3; }
button { padding:9px 15px; border-radius:6px; border:1px solid #3b82f6; background:#1f6feb; color:white; cursor:pointer; font-weight:600; }
button.secondary { background:#21262d; border-color:#30363d; }
button.danger { background:#8b1e2d; border-color:#b62336; }
button:disabled { opacity:.45; cursor:not-allowed; }
.actions { display:flex; gap:9px; margin-top:16px; flex-wrap:wrap; }
.progress { height:10px; background:#21262d; border-radius:999px; overflow:hidden; margin-top:12px; }
.progress > div { height:100%; background:#2f81f7; width:0%; transition:width .2s; }
.note { font-size:13px; color:#8b949e; }
.uploadRow { margin:10px 0; padding:10px 0; border-bottom:1px solid #21262d; }
.uploadRow:last-child { border-bottom:0; }
.uploadLine { margin-top:5px; line-height:1.45; }
.miniProgress { height:7px; background:#21262d; border-radius:999px; overflow:hidden; margin-top:5px; }
.miniProgress > div { height:100%; background:#2f81f7; transition:width .2s; }
.retry { color:#d29922; }
.retryDetails { margin-top:7px; word-break:break-word; }
.error { color:#ff7b72; white-space:pre-wrap; }
.ok { color:#3fb950; }
</style>
</head>
<body>
<main>
<h1>Slab Repacker</h1>
<p>Measure packed-slab waste and rewrite only the live objects needed to release sparse slabs. Old object references remain intact until every replacement has been uploaded and pinned.</p>
<p class="note"><a href="/diagnostics/uploads">Upload diagnostics</a> · inspect failed host attempts, slow-host race losses and final upload errors.</p>

<div class="panel">
  <div class="controls">
    <label>
      <span class="controlTitle">Slab fill threshold (%)</span>
      <input id="fill" type="number" min="0.1" max="99" step="0.1" value="10">
      <span class="help">Only slabs with less than this percentage of live data are candidates. Lower values target emptier slabs and usually reclaim more space per byte re-uploaded. Example: at 10%, a 96 MiB slab must contain less than 9.6 MiB of live data.</span>
    </label>
    <label>
      <span class="controlTitle">Max object size (MiB)</span>
      <input id="maxObject" type="number" min="1" step="1" value="10">
      <span class="help">Safety limit for objects that would need to be rewritten. If a candidate slab references any live object larger than this value, that entire slab is skipped. Raise it to consider more slabs, but large objects can cause much more download and re-upload traffic.</span>
    </label>
    <label>
      <span class="controlTitle">Re-upload budget (GiB)</span>
      <input id="budget" type="number" min="0.01" step="0.1" value="1">
      <span class="help">Maximum total size of complete objects this repack run may rewrite. It is a traffic/work limit, not the amount of waste reclaimed. When adding another slab would exceed the budget, that slab is skipped.</span>
    </label>
  </div>
  <div class="actions">
    <button id="preview">Preview</button>
    <button id="start">Start repack</button>
    <button id="cancel" class="danger" disabled>Cancel</button>
  </div>
  <p class="note"><b>How to use these:</b> start with the defaults and click Preview. A lower fill threshold is more selective, a higher max object size allows more candidates, and a higher re-upload budget lets one run process more data. Preview does not modify anything. Repacking causes Sia download/upload traffic and does not refund storage already paid on old contracts. Snapshot protection and normal orphan/prune cleanup remain active.</p>
</div>

<h2>Current waste</h2>
<div class="grid" id="overall"></div>

<h2>Preview</h2>
<div class="grid" id="plan"></div>
<p id="previewError" class="error"></p>

<h2>Job</h2>
<div class="panel">
  <div id="jobText">Idle</div>
  <div class="progress"><div id="bar"></div></div>
  <p id="jobDetail" class="note"></p>
  <p id="jobError" class="error"></p>
</div>

<h2>Live transfers</h2>
<div class="grid" id="transfers"></div>
<div class="panel">
  <div class="label">Active Sia uploads</div>
  <div id="activeUploads" class="note" style="margin-top:10px">None</div>
</div>

<script>
const $ = id => document.getElementById(id);
const MiB = 1024*1024, GiB = 1024*1024*1024;
let latestRepackUpload = null;
function bytes(n) {
  if (!Number.isFinite(n)) return "-";
  const u=["B","KiB","MiB","GiB","TiB"]; let i=0, v=n;
  while (v>=1024 && i<u.length-1) { v/=1024; i++; }
  return (i ? v.toFixed(2) : Math.round(v))+" "+u[i];
}
function options() {
  return {
    maxSlabFillPercent: Number($("fill").value),
    maxObjectSize: Math.round(Number($("maxObject").value)*MiB),
    reuploadBudget: Math.round(Number($("budget").value)*GiB)
  };
}
function esc(v) {
  return String(v).replace(/[&<>"']/g, ch => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[ch]));
}
function card(label,value) { return '<div class="card"><div class="label">'+esc(label)+'</div><div class="value">'+esc(value)+'</div></div>'; }
function cardLink(label,value,href,hint) {
  return '<a class="card cardLink" href="'+esc(href)+'"><div class="label">'+esc(label)+'</div><div class="value">'+esc(value)+'</div>'+
    (hint ? '<div class="cardHint">'+esc(hint)+'</div>' : '')+'</a>';
}
function duration(ms) {
  if (!Number.isFinite(ms) || ms < 0) return "-";
  const total=Math.floor(ms/1000), s=total%60, m=Math.floor(total/60)%60, h=Math.floor(total/3600);
  return (h ? h+":" : "")+String(m).padStart(2,"0")+":"+String(s).padStart(2,"0");
}
function shortHost(v) {
  v=String(v || "");
  return v.length>20 ? v.slice(0,10)+"…"+v.slice(-8) : v;
}
async function json(url, init) {
  const r=await fetch(url, init);
  const t=await r.text();
  if (!r.ok) throw new Error(t || r.statusText);
  return t ? JSON.parse(t) : {};
}
async function preview() {
  $("previewError").textContent="";
  const o=options();
  const q=new URLSearchParams(o);
  try {
    const p=await json("/repack/preview?"+q);
    $("overall").innerHTML =
      card("Slabs", p.slabs.toLocaleString())+
      card("Logical capacity", bytes(p.logicalCapacity))+
      card("Live data", bytes(p.liveBytes))+
      card("Logical waste", bytes(p.logicalWaste))+
      card("Waste", p.wastePercent.toFixed(2)+"%")+
      card("Physical waste", bytes(p.physicalWaste));
    $("plan").innerHTML =
      card("Sparse candidates", p.candidateSlabs.toLocaleString())+
      card("Selected slabs", p.selectedSlabs.toLocaleString())+
      card("Objects to rewrite", p.objectsToRewrite.toLocaleString())+
      card("Re-upload", bytes(p.rewriteBytes))+
      card("Est. logical reclaim", bytes(p.estimatedLogicalReclaim))+
      card("Est. physical reclaim", bytes(p.estimatedPhysicalReclaim))+
      card("Busy slabs skipped", p.busySlabs.toLocaleString())+
      card("Large-object slabs skipped", p.skippedLargeSlabs.toLocaleString())+
      card("Budget-limited slabs", p.skippedBudgetSlabs.toLocaleString());
  } catch(e) {
    $("previewError").textContent=e.message;
  }
}
async function start() {
  if (!confirm("Start the repack with the current limits? Existing object references are kept until replacements are pinned.")) return;
  try {
    await json("/repack/start", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(options())});
    await status();
  } catch(e) { $("jobError").textContent=e.message; }
}
async function cancel() {
  try { await json("/repack/cancel", {method:"POST"}); await status(); }
  catch(e) { $("jobError").textContent=e.message; }
}
function phaseLabel(p) {
  const labels = {
    planning:"Planning",
    rewriting:"Downloading + uploading",
    finalizing:"Finalizing packed upload",
    pinning:"Pinning replacements",
    committing:"Swapping metadata",
    cleanup:"Cleaning old objects"
  };
  return labels[p] || p || "Idle";
}
async function status() {
  try {
    const s=await json("/repack/status");
    const running=s.state==="running";
    $("start").disabled=running;
    $("cancel").disabled=!running;
    let pct=s.rewriteBytes>0 ? Math.min(100,100*s.rewrittenBytes/s.rewriteBytes) : 0;
    if (s.phase==="finalizing" && latestRepackUpload && latestRepackUpload.expectedPhysical>0) {
      pct=Math.min(100,100*(latestRepackUpload.physicalSent || 0)/latestRepackUpload.expectedPhysical);
    }
    $("bar").style.width=pct+"%";
    const siaProgress=(s.phase==="finalizing" && latestRepackUpload && latestRepackUpload.expectedPhysical)
      ? ' · Sia '+bytes(latestRepackUpload.physicalSent || 0)+' / '+bytes(latestRepackUpload.expectedPhysical)+' minimum'
      : '';
    $("jobText").innerHTML =
      '<b>'+s.state+'</b> · '+phaseLabel(s.phase)+' · '+s.objectsCompleted+'/'+s.objectsTotal+' objects · '+
      'source '+bytes(s.rewrittenBytes)+' / '+bytes(s.rewriteBytes)+siaProgress+
      (s.estimatedLogicalReclaim ? ' · est. reclaim '+bytes(s.estimatedLogicalReclaim)+' logical' : '');
    $("jobDetail").textContent = s.currentObject
      ? 'Current object '+(s.currentObjectIndex || 0)+'/'+s.objectsTotal+' · '+s.currentObject+
        ' · '+bytes(s.currentObjectSize || 0)+(s.currentObjectID ? ' · '+s.currentObjectID : '')
      : '';
    $("jobError").textContent=s.error || "";
  } catch(e) { $("jobError").textContent=e.message; }
}
async function transferStats() {
  try {
    const s=await json("/stats/uploads");
    const t=s.transfer || {};
    $("transfers").innerHTML =
      card("S3 ingress", (t.ingressActive || 0)+" active · "+bytes(t.ingressRate || 0)+"/s")+
      card("Sia upload", ((t.activeUploads || []).length)+" active · "+bytes(t.uploadRate || 0)+"/s")+
      card("Buffer", bytes(s.bufferUsed || 0)+" / "+bytes(s.bufferLimit || 0))+
      card("Pending", (s.pendingObjects || 0).toLocaleString()+" · "+bytes(s.pendingSize || 0))+
      card("Uploaded", (s.uploadedObjects || 0).toLocaleString()+" · "+bytes(s.uploadedSize || 0))+
      card("Orphaned", (s.orphanedObjects || 0).toLocaleString())+
      cardLink("Failed uploads", (s.failedUploads || 0).toLocaleString(), "/diagnostics/uploads", "Open diagnostics →")+
      card("Sia sent this run", bytes(t.uploadBytes || 0));
    const active=t.activeUploads || [];
    latestRepackUpload=active.find(u => u.label==="slab repack") || null;
    $("activeUploads").innerHTML = active.length ? active.map(u => {
      const sourcePct=u.size>0 ? Math.min(100,100*(u.sent || 0)/u.size) : 0;
      const physicalPct=u.expectedPhysical>0 ? Math.min(100,100*(u.physicalSent || 0)/u.expectedPhysical) : 0;
      const state=u.finalizing ? "finalizing" : "uploading";
      const observedRetry=u.observedRetryShards || 0;
      const extraPhysical=Math.max(0,(u.physicalSent || 0)-(u.expectedPhysical || 0));
      const shardSize=(u.expectedShards>0 && u.expectedPhysical>0) ? u.expectedPhysical/u.expectedShards : 0;
      const inferredRetry=shardSize>0 ? Math.floor(extraPhysical/shardSize) : 0;
      const retry=Math.max(observedRetry,inferredRetry);
      const retryPhysical=Math.max(u.retryBytes || 0,extraPhysical);
      let retryLine="";
      if (retry>0 || retryPhysical>0) {
        retryLine='<div class="uploadLine retry"><b>Retry activity:</b> '+
          esc(retry)+' successful shard re-upload'+(retry===1?'':'s')+
          ' · '+esc(bytes(retryPhysical))+
          ' · '+esc(u.retriedSlabs || 0)+' slab'+((u.retriedSlabs || 0)===1?'':'s')+
          (u.maxSlabAttempt>1 ? ' · max attempt '+esc(u.maxSlabAttempt) : '')+
          '</div>';
      }
      const retryDetails=(u.retryDetails || []).length ? '<details class="retryDetails"><summary>Retry details ('+
        esc((u.retryDetails || []).length)+' shard'+((u.retryDetails || []).length===1?'':'s')+')</summary>'+
        (u.retryDetails || []).map(r => '<div>Slab '+esc(r.slabIndex)+' · shard '+esc(r.shardIndex)+
          ' · '+esc(r.successfulUploads)+' successful writes'+
          (r.lastHost ? ' · host '+esc(shortHost(r.lastHost)) : '')+
          (r.lastElapsedMs ? ' · '+esc(duration(r.lastElapsedMs)) : '')+
          '</div>').join("")+'</details>' : '';
      return '<div class="uploadRow"><b>'+esc(u.label)+'</b> · '+esc(state)+' · '+esc(u.objects)+' objects'+
        '<div class="uploadLine">Source: '+esc(bytes(u.sent || 0))+' / '+esc(bytes(u.size || 0))+
        ' ('+esc(sourcePct.toFixed(1))+'%)</div>'+
        '<div class="miniProgress"><div style="width:'+esc(sourcePct.toFixed(2))+'%"></div></div>'+
        '<div class="uploadLine">Sia successful data: '+esc(bytes(u.physicalSent || 0))+
        (u.expectedPhysical ? ' / '+esc(bytes(u.expectedPhysical))+' minimum ('+esc(physicalPct.toFixed(1))+'%)' : '')+
        '</div>'+
        '<div class="miniProgress"><div style="width:'+esc(physicalPct.toFixed(2))+'%"></div></div>'+
        '<div class="uploadLine">Shard writes: '+esc(u.shardUploads || 0)+
        (u.expectedShards ? ' · observed unique '+esc(u.observedUniqueShards || 0)+' / '+esc(u.expectedShards) : '')+
        (u.finalizing ? ' · finalizing '+esc(duration(u.finalizingForMs || 0)) : '')+
        '</div>'+retryLine+retryDetails+'</div>';
    }).join("") : "None";
  } catch(e) {
    $("activeUploads").textContent="Transfer stats unavailable: "+e.message;
  }
}
$("preview").onclick=preview;
$("start").onclick=start;
$("cancel").onclick=cancel;
preview(); status(); transferStats();
setInterval(status, 1000);
setInterval(transferStats, 1000);
</script>
</main>
</body>
</html>`
