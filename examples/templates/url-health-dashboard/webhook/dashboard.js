import { checkBatchSchedule } from "starter:app-obelisk-schedule/workflow";
import * as obelisk from "obelisk:webhook@1.0.0";

const MAX_URLS = 20;

export default async function handle(request) {
  const path = new URL(request.url).pathname;
  if (path === "/" || path === "") return htmlPage();
  if (path === "/api/check" && request.method === "POST") return await scheduleCheck(request);
  if (path.startsWith("/api/result/")) return getResult(path.substring("/api/result/".length));
  return new Response("not found", { status: 404 });
}

async function scheduleCheck(request) {
  let body;
  try {
    body = await request.json();
  } catch (e) {
    return json({ error: `invalid JSON body: ${e}` }, 400);
  }
  const urls = body?.urls;
  if (!Array.isArray(urls) || urls.length === 0) {
    return json({ error: 'body must be {"urls": ["https://...", ...]}' }, 400);
  }
  if (urls.length > MAX_URLS) {
    return json({ error: `at most ${MAX_URLS} URLs per batch` }, 400);
  }
  const bad = urls.find((u) => typeof u !== "string" || !/^https:\/\//i.test(u));
  if (bad !== undefined) {
    return json({ error: `only https:// URLs are allowed: ${bad}` }, 400);
  }
  return json({ execution_id: checkBatchSchedule(null, urls) });
}

function getResult(execId) {
  let results;
  try {
    results = obelisk.tryGet(execId);
  } catch (e) {
    if (e instanceof obelisk.ChildError) return json({ finished: true, error: e.message });
    return json({ error: `unknown execution: ${e}` }, 404);
  }
  if (results === undefined) {
    return json({ finished: false, status: obelisk.getStatus(execId).status });
  }
  return json({ finished: true, results });
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function htmlPage() {
  const page = HTML.replace("__WEBUI_URL__", JSON.stringify(process.env.WEBUI_URL ?? null));
  return new Response(page, { headers: { "content-type": "text/html; charset=utf-8" } });
}

const HTML = /* html */ `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>URL Health Dashboard</title>
<style>
  :root {
    --bg: #0f172a;
    --panel: #111c34;
    --panel-2: #182444;
    --border: #253357;
    --text: #e2e8f0;
    --muted: #94a3b8;
    --accent: #60a5fa;
    --accent-2: #38bdf8;
    --ok: #22c55e;
    --warn: #eab308;
    --err: #ef4444;
    --code-bg: #0b1226;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, system-ui, sans-serif;
    font-size: 15px; line-height: 1.45; }
  a { color: var(--accent-2); text-decoration: none; }
  a:hover { text-decoration: underline; }
  .wrap { max-width: 1100px; margin: 0 auto; padding: 32px 24px 96px; }
  header { display: flex; align-items: baseline; justify-content: space-between;
    gap: 12px; margin-bottom: 28px; flex-wrap: wrap; }
  header h1 { margin: 0; font-size: 28px; font-weight: 650; letter-spacing: -0.01em;
    background: linear-gradient(90deg, #93c5fd, #67e8f9); -webkit-background-clip: text;
    background-clip: text; color: transparent; }
  header .sub { color: var(--muted); font-size: 13px; }
  .card { background: var(--panel); border: 1px solid var(--border);
    border-radius: 14px; padding: 20px; box-shadow: 0 20px 50px rgba(0,0,0,0.25); }
  .card + .card { margin-top: 20px; }
  label { display: block; font-size: 13px; color: var(--muted); margin-bottom: 8px; }
  textarea { width: 100%; min-height: 130px; resize: vertical; background: var(--code-bg);
    color: var(--text); border: 1px solid var(--border); border-radius: 10px;
    padding: 12px 14px; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 13px; outline: none; }
  textarea:focus { border-color: var(--accent); box-shadow: 0 0 0 3px rgba(96,165,250,0.15); }
  .row { display: flex; gap: 12px; align-items: center; margin-top: 12px; flex-wrap: wrap; }
  button { border: 0; cursor: pointer; padding: 10px 18px; border-radius: 10px;
    font-weight: 600; font-size: 14px; letter-spacing: 0.01em;
    background: linear-gradient(90deg, #3b82f6, #06b6d4); color: white;
    transition: transform .05s ease, filter .15s ease; }
  button:hover { filter: brightness(1.1); }
  button:active { transform: translateY(1px); }
  button:disabled { opacity: 0.6; cursor: not-allowed; }
  button.secondary { background: var(--panel-2); border: 1px solid var(--border); color: var(--text); }
  .status-line { color: var(--muted); font-size: 13px; margin-left: auto; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
    gap: 14px; margin-top: 6px; }
  .result { background: var(--panel-2); border: 1px solid var(--border);
    border-radius: 12px; padding: 14px 16px; display: flex; flex-direction: column; gap: 8px;
    position: relative; overflow: hidden; }
  .result::before { content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 4px;
    background: var(--muted); }
  .result.ok::before { background: var(--ok); }
  .result.warn::before { background: var(--warn); }
  .result.err::before { background: var(--err); }
  .result .top { display: flex; align-items: center; gap: 10px; }
  .badge { display: inline-flex; align-items: center; gap: 6px; padding: 3px 8px;
    border-radius: 999px; font-size: 12px; font-weight: 600; font-variant-numeric: tabular-nums; }
  .badge.ok { background: rgba(34,197,94,0.15); color: #86efac; }
  .badge.warn { background: rgba(234,179,8,0.15); color: #fde68a; }
  .badge.err { background: rgba(239,68,68,0.15); color: #fca5a5; }
  .url { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12.5px;
    color: var(--text); word-break: break-all; }
  .title { font-size: 14px; font-weight: 600; }
  .kv { display: grid; grid-template-columns: auto 1fr; gap: 2px 12px; font-size: 12.5px;
    color: var(--muted); }
  .kv b { color: var(--text); font-weight: 500; font-variant-numeric: tabular-nums; }
  .latency-bar { height: 4px; background: rgba(255,255,255,0.05); border-radius: 2px;
    overflow: hidden; margin-top: 2px; }
  .latency-bar > span { display: block; height: 100%;
    background: linear-gradient(90deg, #22d3ee, #60a5fa); }
  .empty { color: var(--muted); font-size: 14px; padding: 8px 2px; }
  .error { color: #fca5a5; font-size: 12.5px; word-break: break-word; }
  .foot { color: var(--muted); font-size: 12px; margin-top: 20px; text-align: center; }
  .spinner { display: inline-block; width: 14px; height: 14px; border-radius: 50%;
    border: 2px solid rgba(255,255,255,0.15); border-top-color: var(--accent-2);
    animation: spin 0.8s linear infinite; vertical-align: -3px; margin-right: 6px; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>URL Health Dashboard</h1>
      <div class="sub">Powered by an Obelisk workflow that fans out parallel HTTP GETs.</div>
    </div>
    <div class="sub" id="exec-hint"></div>
  </header>

  <div class="card">
    <label for="urls">URLs to check (one per line, https only)</label>
    <textarea id="urls" spellcheck="false" placeholder="https://example.com&#10;https://obeli.sk">https://example.com
https://obeli.sk
https://httpbin.org/status/200
https://httpbin.org/status/404
https://httpbin.org/status/503</textarea>
    <div class="row">
      <button id="go">Check now</button>
      <button id="clear" class="secondary">Clear</button>
      <span class="status-line" id="status">Idle.</span>
    </div>
  </div>

  <div class="card" id="results-card" style="display:none;">
    <div class="row" style="margin: 0 0 12px; justify-content: space-between;">
      <strong>Results</strong>
      <span class="status-line" id="summary"></span>
    </div>
    <div class="grid" id="grid"></div>
  </div>

  <div class="foot">Every check above ran as a retried activity inside a durable, replayable Obelisk workflow.</div>
</div>

<script>
const $ = (id) => document.getElementById(id);
let activePoll = null;
// Without WEBUI_URL, assume the Web UI listens on port 8080 of the dashboard's host.
const webuiUrl = (__WEBUI_URL__ ?? (location.protocol + "//" + location.hostname + ":8080")).replace(/\\/+$/, "");

$("clear").addEventListener("click", () => {
  $("urls").value = "";
  $("results-card").style.display = "none";
  $("status").textContent = "Idle.";
  $("exec-hint").textContent = "";
});

$("go").addEventListener("click", async () => {
  const urls = $("urls").value.split(/\\r?\\n/).map(s => s.trim()).filter(Boolean);
  if (urls.length === 0) { flash("Add at least one URL.", true); return; }
  $("go").disabled = true;
  clearInterval(activePoll);
  $("status").innerHTML = '<span class="spinner"></span>Scheduling workflow…';
  $("exec-hint").textContent = "";
  try {
    const r = await fetch("/api/check", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ urls }) });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || ("HTTP " + r.status));
    const execId = j.execution_id;
    const link = document.createElement("a");
    link.href = webuiUrl + "/execution/" + encodeURIComponent(execId);
    link.target = "_blank"; link.rel = "noreferrer noopener";
    link.textContent = execId;
    $("exec-hint").replaceChildren("execution ", link);
    poll(execId);
  } catch (e) {
    flash(String(e.message || e), true);
    $("go").disabled = false;
  }
});

function poll(execId) {
  const t0 = Date.now();
  const tick = async () => {
    try {
      const r = await fetch("/api/result/" + encodeURIComponent(execId));
      const j = await r.json();
      const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
      if (!r.ok) throw new Error(j.error || ("HTTP " + r.status));
      if (!j.finished) {
        $("status").innerHTML = '<span class="spinner"></span>Workflow ' + esc(j.status || "pending") + ", " + elapsed + "s";
        return;
      }
      clearInterval(activePoll);
      $("go").disabled = false;
      if (j.error) {
        flash("Workflow failed: " + j.error, true);
        return;
      }
      render(j.results);
      $("status").textContent = "Completed in " + elapsed + "s.";
    } catch (e) {
      $("status").textContent = "Poll error: " + e.message;
    }
  };
  tick();
  activePoll = setInterval(tick, 700);
}

function statusClass(status) {
  if (status >= 200 && status < 300) return "ok";
  if (status >= 300 && status < 400) return "warn";
  return "err";
}

function render(results) {
  const grid = $("grid");
  grid.innerHTML = "";
  $("results-card").style.display = "";
  const checks = results.map(({ check }) => check.ok).filter(Boolean);
  const maxLatency = Math.max(1, ...checks.map(c => c.latency_ms));
  const okCount = checks.filter(c => statusClass(c.status) !== "err").length;
  for (const { url, check } of results) {
    const c = check.ok;
    const cls = c ? statusClass(c.status) : "err";
    const card = document.createElement("div");
    card.className = "result " + cls;
    const badge = document.createElement("span");
    badge.className = "badge " + cls;
    badge.textContent = c ? (c.status + (c.status_text ? " " + c.status_text : "")) : "ERR";
    const top = document.createElement("div");
    top.className = "top";
    top.appendChild(badge);
    const urlLink = document.createElement("a");
    urlLink.href = url; urlLink.target = "_blank"; urlLink.rel = "noreferrer noopener";
    urlLink.textContent = url;
    urlLink.className = "url";
    top.appendChild(urlLink);
    card.appendChild(top);
    if (c) {
      appendCheck(card, c, maxLatency);
    } else {
      const e = document.createElement("div");
      e.className = "error";
      e.textContent = check.err;
      card.appendChild(e);
    }
    grid.appendChild(card);
  }
  $("summary").textContent = okCount + " / " + results.length + " healthy, max latency " + maxLatency + " ms";
}

function appendCheck(card, c, maxLatency) {
  if (c.title) {
    const t = document.createElement("div");
    t.className = "title";
    t.textContent = c.title;
    card.appendChild(t);
  }
  const kv = document.createElement("div");
  kv.className = "kv";
  kv.innerHTML =
    "<span>latency</span><b>" + c.latency_ms + " ms</b>" +
    "<span>size</span><b>" + fmtBytes(c.size_bytes) + "</b>" +
    (c.content_type ? ("<span>content-type</span><b>" + esc(c.content_type) + "</b>") : "") +
    (c.server ? ("<span>server</span><b>" + esc(c.server) + "</b>") : "");
  card.appendChild(kv);
  const bar = document.createElement("div");
  bar.className = "latency-bar";
  const span = document.createElement("span");
  span.style.width = ((c.latency_ms / maxLatency) * 100).toFixed(1) + "%";
  bar.appendChild(span);
  card.appendChild(bar);
}

function fmtBytes(n) {
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  return (n / 1024 / 1024).toFixed(2) + " MB";
}

function esc(s) { const d = document.createElement("div"); d.textContent = s; return d.innerHTML; }

function flash(msg, err) {
  $("status").textContent = msg;
  $("status").style.color = err ? "#fca5a5" : "";
  setTimeout(() => { $("status").style.color = ""; }, 4000);
}
</script>
</body>
</html>`;
