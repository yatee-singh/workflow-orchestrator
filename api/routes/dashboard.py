"""Dashboard API + embedded status board page.

GET /dashboard          — HTML status board (auto-refreshes every 2s)
GET /api/dashboard/summary — JSON: order counts, queue depths, RPC latency
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_db
from shared.config import get_settings
from shared.messaging.rpc import rpc_call
from shared.messaging.connection import get_channel
from shared.models import Order, OrderStatus

logger = logging.getLogger(__name__)
router = APIRouter(tags=["dashboard"])

# Poll interval for the HTML page
REFRESH_MS = 200


async def _queue_stats() -> list[dict[str, Any]]:
    """Fetch queue depth/consumer stats from the RabbitMQ management API."""
    settings = get_settings()
    # rabbitmq_management_url already ends in /api
    url = f"{settings.rabbitmq_management_url.rstrip('/')}/queues"
    try:
        async with httpx.AsyncClient(timeout=3.0, trust_env=False) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            rows = resp.json()
    except Exception as exc:  # management UI may be disabled in prod
        logger.warning("RabbitMQ management API unavailable: %s", exc)
        return []

    stats = []
    for q in rows:
        if not q.get("name", "").startswith("q."):
            continue
        stats.append(
            {
                "name": q["name"],
                "ready": q.get("messages_ready", 0),
                "unacked": q.get("messages_unacknowledged", 0),
                "consumers": q.get("consumers", 0),
            }
        )
    # Deepest queues first so trouble (DLQ, backlog) floats to the top
    stats.sort(key=lambda s: -(s["ready"] + s["unacked"]))
    return stats


async def _rpc_latency_ms() -> float | None:
    """Time a real RPC round trip against the inventory worker.

    Cached for 15s: each RPC call creates an exclusive callback queue, so
    probing on every 2s page refresh would churn queues on the broker.
    """
    now = time.monotonic()
    if now - _rpc_cache["ts"] < 15 and _rpc_cache["value"] is not None:
        return _rpc_cache["value"]
    try:
        channel = await get_channel()
        start = time.perf_counter()
        result = await rpc_call(
            channel, body={"action": "check_availability", "sku": "SKU-TEE-BLK"}
        )
        elapsed = round((time.perf_counter() - start) * 1000, 1)
        _rpc_cache.update(ts=now, value=elapsed if result is not None else None)
        return _rpc_cache["value"]
    except Exception as exc:
        logger.warning("RPC latency probe failed: %s", exc)
        return None


_rpc_cache: dict[str, Any] = {"ts": 0.0, "value": None}


@router.get("/api/dashboard/summary")
async def summary(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    # Order counts per status
    rows = await db.execute(select(Order.status, func.count()).group_by(Order.status))
    counts = {status.value: 0 for status in OrderStatus}
    for status, count in rows.all():
        counts[status.value] = count

    queues = await _queue_stats()
    return {
        "orders": counts,
        "queues": queues,
        "rpc_latency_ms": await _rpc_latency_ms(),
        "total_events_pending": sum(q["ready"] + q["unacked"] for q in queues),
    }


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page() -> str:
    return _PAGE


_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OrderFlow dashboard</title>
<style>
  :root {
    --bg: #f7f7f8; --panel: #ffffff; --ink: #1a1a1e; --muted: #6b6b76;
    --line: #e3e3e8; --ok: #18794e; --warn: #b45309; --bad: #c0362c;
    --accent: #2563eb;
  }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#111114; --panel:#1a1a1f; --ink:#ececf0; --muted:#9a9aa6;
            --line:#2c2c33; --ok:#3fbf7f; --warn:#e0a44a; --bad:#e0655c; --accent:#60a5fa; }
  }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--ink);
         font:15px/1.5 -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
  header { display:flex; align-items:baseline; gap:14px; flex-wrap:wrap;
           padding:18px 24px 6px; }
  h1 { font-size:20px; margin:0; font-weight:600; }
  .health { font-size:13px; padding:2px 10px; border-radius:999px;
            border:1px solid var(--line); background:var(--panel); }
  .health.ok { color:var(--ok); border-color:var(--ok); }
  .health.bad { color:var(--bad); border-color:var(--bad); }
  .latency { font-size:13px; color:var(--muted); }
  .spacer { flex:1; }
  button { background:var(--accent); color:#fff; border:0; border-radius:8px;
           padding:8px 14px; font-size:14px; cursor:pointer; }
  button:disabled { opacity:.5; cursor:default; }
  main { padding:12px 24px 32px; display:grid; gap:16px; max-width:1100px; }
  section { background:var(--panel); border:1px solid var(--line);
            border-radius:12px; padding:16px 18px; }
  h2 { font-size:13px; text-transform:uppercase; letter-spacing:.06em;
       color:var(--muted); margin:0 0 12px; font-weight:500; }
  /* status board */
  .board { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; }
  .col { border:1px solid var(--line); border-radius:10px; padding:10px 12px; }
  .col .n { font-size:30px; font-weight:600; }
  .col .l { font-size:12px; color:var(--muted); text-transform:capitalize; }
  .col.failed .n { color:var(--bad); }
  /* queue table */
  table { width:100%; border-collapse:collapse; font-size:14px; }
  th { text-align:left; font-weight:500; color:var(--muted); font-size:12px;
       padding:6px 8px; border-bottom:1px solid var(--line); }
  td { padding:7px 8px; border-bottom:1px solid var(--line); }
  tr:last-child td { border-bottom:0; }
  td.num { text-align:right; font-variant-numeric:tabular-nums; }
  .depth { display:inline-block; min-width:64px; height:8px; border-radius:4px;
           background:var(--line); position:relative; overflow:hidden; vertical-align:middle;
           margin-left:8px; }
  .depth i { position:absolute; inset:0 auto 0 0; background:var(--accent); }
  .depth.alarm i { background:var(--bad); }
  .empty { color:var(--muted); font-size:14px; }
  #msg { font-size:13px; color:var(--muted); min-height:1.2em; margin-top:8px; }
</style>
</head>
<body>
<header>
  <h1>OrderFlow</h1>
  <span class="health" id="health">checking…</span>
  <span class="latency" id="latency"></span>
  <span class="spacer"></span>
  <button id="place">Place demo order</button>
</header>
<main>
  <section>
    <h2>Order status board</h2>
    <div class="board" id="board"></div>
    <div id="msg"></div>
  </section>
  <section>
    <h2>Queues — depth &amp; consumers</h2>
    <div id="queues" class="empty">loading…</div>
  </section>
</main>
<script>
const BOARD = ["pending","reserved","paid","shipped","payment_failed","reservation_failed"];
const esc = s => String(s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));

async function refresh() {
  try {
    const [sum, health] = await Promise.all([
      fetch("/api/dashboard/summary").then(r => r.json()),
      fetch("/health").then(r => r.json()),
    ]);

    const h = document.getElementById("health");
    h.textContent = health.status === "ok" ? "all systems ok" : "degraded";
    h.className = "health " + (health.status === "ok" ? "ok" : "bad");

    document.getElementById("latency").textContent =
      sum.rpc_latency_ms != null
        ? `RPC round trip: ${sum.rpc_latency_ms} ms`
        : "RPC: no response";

    document.getElementById("board").innerHTML = BOARD.map(k => {
      const n = sum.orders[k] ?? 0;
      const failed = k.endsWith("failed") ? " failed" : "";
      return `<div class="col${failed}"><div class="n">${n}</div><div class="l">${k.replace("_"," ")}</div></div>`;
    }).join("");

    const max = Math.max(1, ...sum.queues.map(q => q.ready + q.unacked));
    const rows = sum.queues.map(q => {
      const depth = q.ready + q.unacked;
      const alarm = q.name === "q.dlq" && depth > 0 ? " alarm" : "";
      const pct = Math.round(depth / max * 100);
      return `<tr><td>${esc(q.name)}</td>
        <td class="num">${depth}<span class="depth${alarm}"><i style="width:${pct}%"></i></span></td>
        <td class="num">${q.unacked}</td><td class="num">${q.consumers}</td></tr>`;
    }).join("");
    document.getElementById("queues").innerHTML = sum.queues.length
      ? `<table><thead><tr><th>Queue</th><th class="num">Ready</th>
         <th class="num">Unacked</th><th class="num">Consumers</th></tr></thead>
         <tbody>${rows}</tbody></table>`
      : "management API unavailable";
  } catch (e) {
    document.getElementById("health").textContent = "API unreachable";
    document.getElementById("health").className = "health bad";
  }
}

document.getElementById("place").addEventListener("click", async () => {
  const btn = document.getElementById("place");
  btn.disabled = true;
  try {
    const r = await fetch("/orders", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        customer_email: "demo-" + Math.random().toString(36).slice(2, 8) + "@example.com",
        items: [{sku: "SKU-TEE-BLK", quantity: 1}],
      }),
    });
    const j = await r.json();
    document.getElementById("msg").textContent =
      r.ok ? "order " + j.order.id.slice(0, 8) + " published — watch it move across the board"
           : "failed: " + (j.detail || r.status);
  } catch { document.getElementById("msg").textContent = "request failed"; }
  btn.disabled = false;
});

refresh();
setInterval(refresh, __REFRESH_MS__);
</script>
</body>
</html>
""".replace("__REFRESH_MS__", str(REFRESH_MS))