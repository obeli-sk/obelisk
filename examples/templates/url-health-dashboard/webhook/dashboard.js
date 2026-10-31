import { checkSiteSchedule } from "starter:app-obelisk-schedule/workflow";
import * as obelisk from "obelisk:webhook@1.0.0";

const WORKFLOW_FFQN = "starter:app/workflow.check-site";
const HISTORY_LENGTH = 30;

export default async function handle(request) {
  const path = new URL(request.url).pathname;
  if (path === "/" || path === "") return htmlPage();
  if (path === "/api/status") return await status();
  if (path === "/api/check" && request.method === "POST") return checkNow();
  return new Response("not found", { status: 404 });
}

function checkNow() {
  return json({ execution_id: checkSiteSchedule(null, process.env.TARGET_URL) }, 202);
}

// Only listing runs needs the API; results are read with `tryGet`.
async function listRuns() {
  const response = await fetch(
    `${process.env.OBELISK_API_URL}/v1/executions?function=${encodeURIComponent(WORKFLOW_FFQN)}&length=${HISTORY_LENGTH}`,
    {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.INTERNAL_TOKEN}`,
      },
    },
  );
  if (!response.ok) throw new Error(`listing executions failed: HTTP ${response.status}`);
  return await response.json();
}

async function status() {
  let runs;
  try {
    runs = await listRuns();
  } catch (e) {
    return json({ error: String(e.message ?? e) }, 502);
  }
  const probes = [];
  let running = null;
  for (const run of runs) {
    const finishedAt = run.pending_state.status === "finished" ? run.pending_state.finished_at : null;
    if (!finishedAt) {
      running ??= run.execution_id;
    } else {
      const p = probe(run.execution_id, finishedAt);
      // Runs probing a previously configured URL are left out.
      if (p.url === undefined || p.url === process.env.TARGET_URL) probes.push(p);
    }
  }
  return json({ target: process.env.TARGET_URL, running, probes });
}

function probe(executionId, at) {
  let result;
  try {
    result = obelisk.tryGet(executionId);
  } catch (e) {
    if (!(e instanceof obelisk.ChildError)) throw e;
    return { execution_id: executionId, at, error: String(e.value ?? e.message) };
  }
  const { url, check } = result;
  return check.ok ? { execution_id: executionId, at, url, check: check.ok }
    : { execution_id: executionId, at, url, error: check.err };
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

function htmlPage() {
  const config = { target: process.env.TARGET_URL, webui: process.env.OBELISK_UI_URL };
  const page = HTML.replace("__CONFIG__", JSON.stringify(config));
  return new Response(page, { headers: { "content-type": "text/html; charset=utf-8" } });
}

const HTML = /* html */ `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Site Health</title>
<style>
  :root {
    --bg: #0f172a; --panel: #111c34; --panel-2: #182444; --border: #253357;
    --text: #e2e8f0; --muted: #94a3b8; --accent: #38bdf8;
    --ok: #22c55e; --warn: #eab308; --err: #ef4444;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, system-ui, sans-serif;
    font-size: 15px; line-height: 1.45; }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  .wrap { max-width: 1000px; margin: 0 auto; padding: 32px 20px 80px; }
  header { display: flex; align-items: flex-start; justify-content: space-between;
    gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
  h1 { margin: 0; font-size: 26px; font-weight: 650; letter-spacing: -0.01em;
    background: linear-gradient(90deg, #93c5fd, #67e8f9); -webkit-background-clip: text;
    background-clip: text; color: transparent; }
  h2 { margin: 0 0 12px; font-size: 14px; font-weight: 600; }
  .sub, .muted { color: var(--muted); font-size: 13px; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12.5px; }
  .card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px;
    padding: 20px; box-shadow: 0 20px 50px rgba(0,0,0,0.25); }
  .card + .card { margin-top: 18px; }
  button { border: 0; cursor: pointer; padding: 10px 18px; border-radius: 10px; font-weight: 600;
    font-size: 14px; color: white; background: linear-gradient(90deg, #3b82f6, #06b6d4); }
  button:hover { filter: brightness(1.1); }
  button:disabled { opacity: 0.6; cursor: progress; }
  .hero { display: flex; align-items: center; gap: 16px; }
  .dot { width: 14px; height: 14px; border-radius: 50%; background: var(--muted); flex: none; }
  .dot.ok { background: var(--ok); box-shadow: 0 0 12px var(--ok); }
  .dot.err { background: var(--err); box-shadow: 0 0 12px var(--err); }
  .state { font-size: 28px; font-weight: 700; letter-spacing: -0.02em; }
  .state.ok { color: #86efac; }
  .state.err { color: #fca5a5; }
  .tiles { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    margin-top: 18px; }
  .tile { background: var(--panel-2); border: 1px solid var(--border); border-radius: 12px;
    padding: 12px 14px; }
  .tile .k { color: var(--muted); font-size: 11.5px; text-transform: uppercase; letter-spacing: .07em; }
  .tile .v { font-size: 20px; font-weight: 650; font-variant-numeric: tabular-nums; }
  .strip { display: grid; grid-template-columns: repeat(${HISTORY_LENGTH}, 1fr); align-items: end;
    gap: 3px; height: 70px; }
  .bar { border-radius: 3px 3px 1px 1px; background: var(--ok); opacity: .85; }
  .bar:hover { opacity: 1; }
  .bar.down { background: var(--err); }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; color: var(--muted); font-weight: 500; font-size: 11.5px;
    text-transform: uppercase; letter-spacing: .06em; padding: 0 10px 8px; }
  td { padding: 8px 10px; border-top: 1px solid var(--border); white-space: nowrap; }
  .tag { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .tag.ok { background: rgba(34,197,94,0.15); color: #86efac; }
  .tag.warn { background: rgba(234,179,8,0.15); color: #fde68a; }
  .tag.err { background: rgba(239,68,68,0.15); color: #fca5a5; }
  .banner { margin-top: 14px; font-size: 13px; color: var(--muted); }
  .banner.error { color: #fca5a5; }
  .foot { color: var(--muted); font-size: 12px; margin-top: 22px; text-align: center; }
  @media (max-width: 560px) { .opt { display: none; } }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>Site Health</h1>
      <div class="sub">A cron schedule runs a durable workflow every 5 minutes to probe
        <a class="mono" id="target" target="_blank" rel="noreferrer noopener"></a>.</div>
    </div>
    <button id="check">Probe now</button>
  </header>

  <div class="card">
    <div class="hero">
      <span class="dot" id="dot"></span>
      <div>
        <div class="state" id="state">Loading…</div>
        <div class="sub" id="state-meta"></div>
      </div>
    </div>
    <div class="tiles" id="tiles"></div>
    <div class="banner" id="banner"></div>
  </div>

  <div class="card">
    <h2>Recent probes <span class="muted">(bar height is latency)</span></h2>
    <div class="strip" id="strip"></div>
  </div>

  <div class="card">
    <h2>Probe log</h2>
    <table>
      <thead><tr><th>Execution</th><th>Status</th><th>Latency</th>
        <th class="opt">Size</th><th class="opt">Title</th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>

  <div class="foot">Every probe is a retried activity inside a replayable Obelisk workflow.</div>
</div>

<script>
const CONFIG = __CONFIG__;
const $ = (id) => document.getElementById(id);
$("target").textContent = CONFIG.target;
$("target").href = CONFIG.target;

function esc(v) {
  const d = document.createElement("div");
  d.textContent = v == null ? "" : String(v);
  return d.innerHTML;
}
function fmtBytes(n) {
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  return (n / 1024 / 1024).toFixed(2) + " MB";
}
function ago(iso) {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 90) return s + "s ago";
  const m = Math.round(s / 60);
  return m < 90 ? m + "m ago" : Math.round(m / 60) + "h ago";
}
function statusClass(status) {
  if (status >= 200 && status < 300) return "ok";
  if (status >= 300 && status < 400) return "warn";
  return "err";
}
function healthy(p) {
  return p.check != null && statusClass(p.check.status) !== "err";
}
function execLink(id) {
  const href = CONFIG.webui.replace(/\\/+$/, "") + "/execution/" + encodeURIComponent(id);
  return '<a class="mono" href="' + esc(href) + '" target="_blank" rel="noreferrer noopener">' + esc(id) + "</a>";
}

async function refresh() {
  try {
    const r = await fetch("/api/status", { cache: "no-store" });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || ("HTTP " + r.status));
    render(data);
  } catch (e) {
    $("banner").className = "banner error";
    $("banner").textContent = "Could not read status: " + (e.message || e);
  }
}

function render(data) {
  const probes = data.probes;
  const latest = probes[0];
  $("banner").className = "banner";
  $("banner").innerHTML = data.running ? "Probe running: " + execLink(data.running) : "";

  if (!latest) {
    $("dot").className = "dot";
    $("state").className = "state";
    $("state").textContent = "Waiting for the first probe";
    $("state-meta").textContent = "The cron schedule runs every 5 minutes, or click Probe now.";
    return;
  }
  const up = healthy(latest);
  $("dot").className = "dot " + (up ? "ok" : "err");
  $("state").className = "state " + (up ? "ok" : "err");
  $("state").textContent = up ? "Up" : "Down";
  $("state-meta").textContent = (latest.check
    ? "HTTP " + latest.check.status + " " + latest.check.status_text
    : latest.error) + " · " + ago(latest.at);

  const checks = probes.map((p) => p.check).filter(Boolean);
  const latencies = checks.map((c) => c.latency_ms);
  const upCount = probes.filter(healthy).length;
  const tiles = [
    ["uptime", Math.round((1000 * upCount) / probes.length) / 10 + "%"],
    ["avg latency", latencies.length
      ? Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length) + " ms" : "–"],
    ["max latency", latencies.length ? Math.max(...latencies) + " ms" : "–"],
    ["probes", upCount + " / " + probes.length + " up"],
  ];
  $("tiles").innerHTML = tiles.map(([k, v]) =>
    '<div class="tile"><div class="k">' + esc(k) + '</div><div class="v">' + esc(v) + "</div></div>"
  ).join("");

  const maxLatency = Math.max(1, ...latencies);
  $("strip").innerHTML = probes.slice().reverse().map((p) => {
    const height = p.check ? Math.max(10, Math.round((p.check.latency_ms / maxLatency) * 100)) : 100;
    const tip = new Date(p.at).toLocaleString() + " · "
      + (p.check ? "HTTP " + p.check.status + " · " + p.check.latency_ms + " ms" : p.error);
    return '<div class="bar' + (healthy(p) ? "" : " down") + '" style="height:' + height
      + '%" title="' + esc(tip) + '"></div>';
  }).join("");

  $("rows").innerHTML = probes.map((p) => {
    const c = p.check;
    const tag = c
      ? '<span class="tag ' + statusClass(c.status) + '">' + c.status + "</span>"
      : '<span class="tag err" title="' + esc(p.error) + '">error</span>';
    return "<tr><td>" + execLink(p.execution_id) + '<br><span class="muted">' + esc(ago(p.at))
      + "</span></td><td>" + tag + "</td><td>" + (c ? c.latency_ms + " ms" : "–")
      + '</td><td class="opt">' + (c ? fmtBytes(c.size_bytes) : "–")
      + '</td><td class="opt">' + esc(c?.title ?? "") + "</td></tr>";
  }).join("");
}

$("check").addEventListener("click", async () => {
  $("check").disabled = true;
  try {
    const r = await fetch("/api/check", { method: "POST" });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || ("HTTP " + r.status));
    $("banner").className = "banner";
    $("banner").innerHTML = "Scheduled " + execLink(data.execution_id);
    setTimeout(refresh, 1500);
  } catch (e) {
    $("banner").className = "banner error";
    $("banner").textContent = "Could not schedule a probe: " + (e.message || e);
  } finally {
    $("check").disabled = false;
  }
});

refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>`;
