// server.js
"use strict";
const express = require("express");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

const app = express();
app.use(express.json({ limit: "1mb" }));
app.use(express.static(__dirname));

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(__dirname, "state.json");

/* ============================================================
   CONSTANTS
   ============================================================ */
const FRESH_MS = 6000;
const REST_EVERY = 5000;
const QUEUE_TTL = 120000;
const WS_SILENT = 15000;
const CONN_DEBOUNCE = 2500;
const BTC_BIAS_THRESHOLD = 0.5;

/* ============================================================
   STATE
   ============================================================ */
const state = {
  symbols: ["BNB","SOL","XRP","XMR","HYPE","ZEC"],
  checked: ["BNB"],
  prices: {}, prev: {}, tickAt: {}, lastRest: {},
  positions: [],
  queue: [],
  btcPrice: null, btc24h: null,
  settings: {
    tp: 0.20, sl: 0.10, driftUp: 0.03, driftDn: 0.03,
    amt: 10, plus: 3, fee: "",
    biasMode: "auto", plusOverride: "",
    autoRestart: true, target: "*"
  },
  session: { start: Date.now(), realizedPnl: 0, realizedFees: 0, tradeCount: 0, wins: 0, losses: 0 },
  cycleBatch: [],
  posSeq: 1,
  logs: [],
  wsOpen: false, gotData: false, wsStatus: "connecting", wsStatusTxt: "Connecting…",
  restartPending: false, suppressRestart: false
};

/* ============================================================
   HELPERS
   ============================================================ */
const normSym = s => String(s || "").toUpperCase().trim();
const num = v => { const n = parseFloat(v); return isFinite(n) ? n : 0; };
const fmtN = n => (n == null || !isFinite(n)) ? "—" : (Math.abs(n) < 1e-9 ? "0" : n.toFixed(4).replace(/\.?0+$/,""));

function log(msg, cls){
  const entry = { t: Date.now(), msg, cls: cls || "" };
  state.logs.unshift(entry);
  if (state.logs.length > 100) state.logs.length = 100;
  console.log(`[${new Date(entry.t).toLocaleTimeString()}] ${msg.replace(/<[^>]+>/g, "")}`);
}

/* ============================================================
   PERSISTENCE
   ============================================================ */
let saveTimer = null;
function saveState(){
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      const toSave = {
        symbols: state.symbols,
        checked: state.checked,
        positions: state.positions,
        queue: state.queue,
        cycleBatch: state.cycleBatch,
        posSeq: state.posSeq,
        settings: state.settings,
        session: state.session
      };
      fs.writeFileSync(STATE_FILE, JSON.stringify(toSave, null, 2));
    } catch(e){ console.error("save failed", e.message); }
  }, 300);
}

function loadState(){
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const d = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    if (d.symbols) state.symbols = d.symbols;
    if (d.checked) state.checked = d.checked;
    if (d.positions) state.positions = d.positions;
    if (d.queue) state.queue = d.queue;
    if (d.cycleBatch) state.cycleBatch = d.cycleBatch;
    if (d.posSeq) state.posSeq = d.posSeq;
    if (d.settings) Object.assign(state.settings, d.settings);
    if (d.session) Object.assign(state.session, d.session);

    /* auto-add missing symbols from restored positions/batch */
    const needed = new Set();
    state.positions.forEach(p => needed.add(normSym(p.sym)));
    state.cycleBatch.forEach(b => needed.add(normSym(b.sym)));
    needed.forEach(s => { if (!state.symbols.includes(s)) state.symbols.push(s); });

    log(`♻️ State restored — ${state.positions.length} positions, ${state.cycleBatch.length} in batch`, "pos");
  } catch(e){ console.error("load failed", e.message); }
}

/* ============================================================
   PRICE STORE
   ============================================================ */
function setPrice(sym, p){
  const S = normSym(sym);
  const v = parseFloat(p);
  if (!isFinite(v) || v <= 0) return false;
  const old = state.prices[S];
  if (old !== v) state.prev[S] = old;
  state.prices[S] = v;
  state.tickAt[S] = Date.now();
  return true;
}
const px = sym => { const v = state.prices[normSym(sym)]; return (v == null || !isFinite(v)) ? null : v; };
const isFresh = (sym, ms = FRESH_MS) => { const t = state.tickAt[normSym(sym)]; return t != null && (Date.now() - t) <= ms; };
const freshPx = sym => isFresh(sym) ? px(sym) : null;

/* ============================================================
   PnL
   ============================================================ */
function grossAt(pos, cur){
  if (cur == null || !isFinite(cur)) return 0;
  return pos.side === "LONG" ? (cur - pos.entry) * pos.amt : (pos.entry - cur) * pos.amt;
}
const netAt = (pos, cur) => grossAt(pos, cur) - pos.fees;
const totalFees = () => state.positions.reduce((s, p) => s + p.fees, 0);
function totalNet(freshOnly){
  let sum = 0;
  for (const p of state.positions){
    const c = freshOnly ? freshPx(p.sym) : px(p.sym);
    if (c == null){ if (freshOnly) return null; continue; }
    sum += netAt(p, c);
  }
  return sum;
}
function feePct(){
  const v = num(state.settings.fee);
  return (v > 0) ? v / 100 : 0;
}

/* ============================================================
   BIAS
   ============================================================ */
function currentBias(){
  const mode = state.settings.biasMode || "auto";
  const b24 = state.btc24h;
  let side = null, source = "auto";
  if (mode === "LONG"){ side = "LONG"; source = "manual"; }
  else if (mode === "SHORT"){ side = "SHORT"; source = "manual"; }
  else if (b24 != null && isFinite(b24)) side = (b24 >= BTC_BIAS_THRESHOLD) ? "LONG" : "SHORT";

  const ovRaw = num(state.settings.plusOverride);
  const ovHas = ovRaw > 0;
  const plusRaw = ovHas ? ovRaw : num(state.settings.plus);
  const plus = plusRaw > 0 ? plusRaw : 0;
  const plusSource = ovHas ? "override" : (plus > 0 ? "plus$" : "none");

  return { side, plus, b24, source, plusSource, mode };
}

/* ============================================================
   BATCH
   ============================================================ */
const bKey = (sym, side) => normSym(sym) + "|" + side;
function addToBatch(sym, side, notional){
  const S = normSym(sym);
  if (!state.symbols.includes(S)) return;
  if (!isFinite(notional) || notional <= 0) return;
  const i = state.cycleBatch.findIndex(b => bKey(b.sym, b.side) === bKey(S, side));
  if (i >= 0) state.cycleBatch[i].notional = notional;
  else state.cycleBatch.push({ sym: S, side, notional });
}
function removeFromBatch(sym, side){
  const S = normSym(sym);
  state.cycleBatch = state.cycleBatch.filter(b => !(normSym(b.sym) === S && (!side || b.side === side)));
}
function pruneBatch(){
  state.cycleBatch = state.cycleBatch.filter(b => state.symbols.includes(normSym(b.sym)));
}

/* ============================================================
   WEBSOCKET (server-side, runs 24/7)
   ============================================================ */
const WS_URLS = [
  s => "wss://fstream.binance.com/market/stream?streams=" + s,
  s => "wss://fstream.binance.com/public/stream?streams=" + s,
  s => "wss://data-stream.binance.vision/stream?streams=" + s
];
let ws = null, retry = 0, lastMsg = 0, lastConnectAt = 0;

function connect(){
  const now = Date.now();
  if (now - lastConnectAt < CONN_DEBOUNCE) return;
  lastConnectAt = now;

  if (ws) { try { ws.onclose = null; ws.close(); } catch(e){} ws = null; }
  const syms = [...state.symbols];
  state.gotData = false;
  if (!syms.length){ state.wsStatus = "off"; state.wsStatusTxt = "No symbols"; return; }

  const streamList = syms.map(s => s.toLowerCase() + "usdt@miniTicker");
  if (!syms.includes("BTC")) streamList.push("btcusdt@miniTicker");
  const streams = streamList.join("/");

  const tryOpen = (ui) => {
    state.wsStatus = "wait"; state.wsStatusTxt = "Connecting…";
    let sock;
    try { sock = new WebSocket(WS_URLS[ui](streams)); }
    catch(e){ setTimeout(() => tryOpen((ui + 1) % WS_URLS.length), 2000); return; }
    ws = sock;

    sock.on("open", () => {
      retry = 0;
      state.wsOpen = true;
      lastMsg = Date.now();
      lastConnectAt = Date.now();
      state.wsStatus = "wait"; state.wsStatusTxt = "Connected — waiting for data…";
    });

    sock.on("message", raw => {
      try {
        const m = JSON.parse(raw);
        const d = m.data; if (!d || !d.s) return;
        const sym = normSym(d.s.endsWith("USDT") ? d.s.slice(0, -4) : d.s);
        const p = parseFloat(d.c);
        if (!isFinite(p) || p <= 0) return;

        lastMsg = Date.now();
        if (!state.gotData){ state.gotData = true; state.wsStatus = "on"; state.wsStatusTxt = "Connected (live)"; }

        if (sym === "BTC"){
          const o = parseFloat(d.o);
          state.btcPrice = p;
          if (isFinite(o) && o > 0) state.btc24h = ((p - o) / o) * 100;
        }

        setPrice(sym, p);
        onPrice();
      } catch(e){}
    });

    sock.on("close", () => {
      state.wsOpen = false;
      state.wsStatus = "off"; state.wsStatusTxt = "Reconnecting…";
      retry++;
      setTimeout(() => tryOpen((ui + 1) % WS_URLS.length), Math.min(15000, 1000 * retry));
    });
    sock.on("error", () => { try { sock.close(); } catch(e){} });
  };
  tryOpen(0);
}

setInterval(() => {
  if (!state.wsOpen || !state.symbols.length) return;
  if (Date.now() - lastMsg > WS_SILENT){
    state.wsStatus = "off"; state.wsStatusTxt = "Stream silent — reconnecting…";
    log("Watchdog: no data for 15s, reconnecting…", "neg");
    lastMsg = Date.now();
    connect();
  }
}, 5000);

/* ============================================================
   REST FALLBACK
   ============================================================ */
async function restPrice(sym){
  const S = normSym(sym) + "USDT";
  const urls = [
    "https://fapi.binance.com/fapi/v1/ticker/price?symbol=" + S,
    "https://api.binance.com/api/v3/ticker/price?symbol=" + S,
    "https://api.binance.us/api/v3/ticker/price?symbol=" + S
  ];
  for (const u of urls){
    try {
      const r = await fetch(u, { cache: "no-store" });
      if (!r.ok) continue;
      const j = await r.json();
      const p = parseFloat(j.price);
      if (isFinite(p) && p > 0){ setPrice(sym, p); return p; }
    } catch(e){}
  }
  return null;
}
async function restPriceBatch(syms){
  const uniq = [...new Set(syms.map(normSym))].filter(s => state.symbols.includes(s));
  await Promise.all(uniq.map(async s => {
    if (isFresh(s, 1500)) return;
    await restPrice(s);
  }));
  flushQueue();
}
async function restPriceAll(){
  const wanted = new Set(state.symbols.map(normSym));
  const urls = [
    "https://fapi.binance.com/fapi/v1/ticker/24hr",
    "https://api.binance.com/api/v3/ticker/24hr",
    "https://api.binance.us/api/v3/ticker/24hr"
  ];
  for (const u of urls){
    try {
      const r = await fetch(u, { cache: "no-store" });
      if (!r.ok) continue;
      const j = await r.json();
      if (!Array.isArray(j)) continue;
      let n = 0;
      for (const t of j){
        if (!t || typeof t.symbol !== "string" || !t.symbol.endsWith("USDT")) continue;
        const sym = t.symbol.slice(0, -4);
        const p = parseFloat(t.lastPrice);
        if (sym === "BTC"){
          const pc = parseFloat(t.priceChangePercent);
          if (isFinite(p)) state.btcPrice = p;
          if (isFinite(pc)) state.btc24h = pc;
        }
        if (!wanted.has(sym)) continue;
        if (setPrice(sym, p)) n++;
      }
      if (n){ flushQueue(); onPrice(); }
      return n;
    } catch(e){}
  }
  return 0;
}
function refreshStale(syms){
  const now = Date.now();
  const uniq = [...new Set(syms.map(normSym))].filter(s =>
    state.symbols.includes(s) && !isFresh(s) && (now - (state.lastRest[s] || 0) > 4000));
  if (!uniq.length) return;
  uniq.forEach(s => state.lastRest[s] = now);
  restPriceBatch(uniq);
}
let restBusy = false;
setInterval(async () => {
  if (restBusy || !state.symbols.length) return;
  const stale = state.symbols.filter(s => !isFresh(s));
  if (!stale.length) return;
  restBusy = true;
  try { await restPriceBatch(stale); } finally { restBusy = false; }
}, REST_EVERY);

/* ============================================================
   FETCH
   ============================================================ */
let fetching = false;
async function doFetch(){
  if (fetching) return { ok: false, msg: "Already fetching" };
  const syms = [...state.symbols];
  if (!syms.length){ log("Fetch: no symbols in list", "neg"); return { ok: false }; }
  fetching = true;
  log(`FETCH → all ${syms.length} symbols: ${syms.join(", ")} …`);

  if (!state.wsOpen || !state.gotData) connect();
  await restPriceAll();

  const deadline = Date.now() + 8000;
  while (Date.now() < deadline){
    const missing = syms.filter(s => px(s) == null);
    if (!missing.length) break;
    await restPriceBatch(missing);
    await new Promise(r => setTimeout(r, 400));
  }

  const ok  = syms.filter(s => px(s) != null);
  const bad = syms.filter(s => px(s) == null);

  if (ok.length) log(`✓ ${ok.map(s => `${s} ${px(s)}`).join(" · ")}`, "pos");
  if (bad.length) log(`✗ No price for: ${bad.join(", ")}`, "neg");
  if (ok.length) log(`FETCH DONE — ${ok.length}/${syms.length} confirmed, trading unlocked`, "pos");

  fetching = false;
  return { ok: ok.length > 0, count: ok.length, total: syms.length };
}

/* ============================================================
   TRADING
   ============================================================ */
function findPosIndex(sym, side){
  const S = normSym(sym);
  for (let i = 0; i < state.positions.length; i++){
    const p = state.positions[i];
    if (normSym(p.sym) === S && p.side === side) return i;
  }
  return -1;
}

function executeOpen(sym, side, amt, source, notional, forcedPx, batchNotional){
  const S = normSym(sym);
  const price = (forcedPx != null && isFinite(forcedPx)) ? forcedPx : freshPx(S);
  if (price == null) return false;
  if (!isFinite(amt) || amt <= 0) return false;

  if (!notional) notional = amt * price;
  const bN = (batchNotional != null && isFinite(batchNotional) && batchNotional > 0) ? batchNotional : notional;

  const f = feePct();
  const addFee = amt * price * f;
  const idx = findPosIndex(S, side);

  if (idx !== -1){
    const p = state.positions[idx];
    const oldAmt = p.amt, oldEntry = p.entry;
    const newAmt = oldAmt + amt;
    const newEntry = (oldEntry * oldAmt + price * amt) / newAmt;
    p.entry = newEntry; p.amt = newAmt; p.fees += addFee;
    p.adds = (p.adds || 1) + 1;
    p.notional = (p.notional || (oldAmt * oldEntry)) + notional;
    addToBatch(S, side, bN);
    log(`⟳ AVERAGE ${side} ${S}: ${fmtN(oldAmt)} @ ${oldEntry} + ${fmtN(amt)} @ ${price} → ${fmtN(newAmt)} @ ${newEntry} (×${p.adds})`, "pos");
  } else {
    state.positions.push({
      id: state.posSeq++, sym: S, side, entry: price, amt,
      fees: addFee, adds: 1, notional, openedAt: Date.now()
    });
    addToBatch(S, side, bN);
    log(`✚ OPEN ${side} ${S}: ${fmtN(amt)} @ ${price} ($${fmtN(notional)} USDT)` +
        (f ? ` (fee ${fmtN(addFee)})` : "") +
        (source === "queue" ? " [queued fill]" : "") +
        (source === "auto" ? " [auto]" : ""), "pos");
  }
  saveState();
  return true;
}

function openPos(side){
  const notional = num(state.settings.amt);
  if (notional <= 0){ log("Invalid notional — must be > 0", "neg"); return; }

  const targetSetting = state.settings.target || "*";
  let targets = targetSetting === "*" ? [...state.checked] : [targetSetting];
  targets = targets.map(normSym).filter(Boolean);
  if (!targets.length){ log("No symbol selected — check a box first", "neg"); return; }

  const stale = [];
  targets.forEach(s => {
    const price = freshPx(s);
    if (price != null){
      const qty = notional / price;
      executeOpen(s, side, qty, "live", notional);
    } else stale.push(s);
  });

  if (stale.length){
    log(`Waiting for a fresh price on: ${stale.join(", ")} …`);
    restPriceBatch(stale).then(() => {
      const still = [];
      stale.forEach(s => {
        const price = freshPx(s);
        if (price != null){
          const qty = notional / price;
          executeOpen(s, side, qty, "live", notional);
        } else still.push(s);
      });
      still.forEach(s => {
        if (!state.queue.some(q => normSym(q.sym) === s && q.side === side))
          state.queue.push({ sym: s, side, notional, baseNotional: notional, at: Date.now() });
      });
      if (still.length) log(`Queued ${side} ${still.join(", ")} — fills on next fresh tick`);
      saveState();
    });
  }
}

function flushQueue(){
  if (!state.queue.length) return;
  const now = Date.now();
  for (let i = state.queue.length - 1; i >= 0; i--){
    const q = state.queue[i];
    if (now - (q.at || 0) > QUEUE_TTL){ state.queue.splice(i, 1); log(`Queued ${q.side} ${q.sym} expired`, "neg"); continue; }
    const price = freshPx(q.sym);
    if (price != null){
      const qty = q.notional / price;
      if (executeOpen(q.sym, q.side, qty, "queue", q.notional, null, q.baseNotional)) state.queue.splice(i, 1);
    }
  }
}

function closePos(id, reason, usePx){
  const idx = state.positions.findIndex(p => p.id === id);
  if (idx < 0) return;
  const p = state.positions[idx];

  let cur = (usePx != null && isFinite(usePx)) ? usePx : freshPx(p.sym);
  if (cur == null) cur = px(p.sym) ?? p.entry;

  const f = feePct();
  p.fees += p.amt * cur * f;
  const g = grossAt(p, cur);
  const net = g - p.fees;

  state.session.realizedPnl += net;
  state.session.realizedFees += p.fees;
  state.session.tradeCount++;
  if (net >= 0) state.session.wins++; else state.session.losses++;

  if (reason === "✕") removeFromBatch(p.sym, p.side);

  log(`${reason || "CLOSE"} ${p.side} ${p.sym} qty ${fmtN(p.amt)} @ ${cur} → PnL ${net>=0?"+":""}${fmtN(net)}`, net >= 0 ? "pos" : "neg");

  state.positions.splice(idx, 1);
  saveState();
  maybeAutoRestart();
}

function closeAll(reason){
  if (!state.positions.length) return false;
  [...state.positions].forEach(p => closePos(p.id, reason));
  log("All positions closed.", "neg");
  return true;
}

function maybeAutoRestart(){
  if (state.restartPending) return;
  if (state.suppressRestart) return;
  if (!state.settings.autoRestart) return;
  if (state.positions.length || state.queue.length) return;
  pruneBatch();
  if (!state.cycleBatch.length) return;
  scheduleRestart();
}

let restartTimer = null;
function scheduleRestart(){
  state.restartPending = true;
  const count = state.cycleBatch.length;
  log(`⟳ Auto-restart queued (${count} positions) — re-opening at fresh prices…`, "pos");
  clearTimeout(restartTimer);
  restartTimer = setTimeout(() => runRestart(0), 800);
}

function runRestart(attempt){
  if (!state.restartPending) return;
  if (!state.settings.autoRestart){ state.restartPending = false; log("Auto-restart skipped — checkbox off", "neg"); return; }
  if (state.positions.length){ state.restartPending = false; log("Auto-restart skipped — positions already open", "neg"); return; }

  pruneBatch();
  const snap = state.cycleBatch.filter(b => state.symbols.includes(normSym(b.sym)));
  if (!snap.length){ state.restartPending = false; return; }

  const stale = snap.filter(b => !isFresh(b.sym));
  if (stale.length && attempt < 25){
    refreshStale(stale.map(b => b.sym));
    setTimeout(() => runRestart(attempt + 1), 300);
    return;
  }

  state.restartPending = false;

  const bias = currentBias();
  const modeTxt = bias.source === "manual"
    ? `${bias.mode} (manual)`
    : (bias.b24 != null ? `BTC 24h ${bias.b24>=0?"+":""}${bias.b24.toFixed(2)}%` : "BTC 24h n/a");

  if (bias.side){
    const plusTxt = bias.plus > 0 ? `+${fmtN(bias.plus)} USDT on ${bias.side} side (${bias.plusSource})` : `${bias.side} side — Plus = 0`;
    log(`⚖ Bias: ${modeTxt} → ${plusTxt}`, "pos");
  } else {
    log(`⚖ Bias: ${modeTxt} → no bias, opening at base notional`, "neg");
  }

  let opened = 0;
  const deferred = [];

  snap.forEach(b => {
    const price = freshPx(b.sym);
    const biased = (bias.side && b.side === bias.side && bias.plus > 0) ? b.notional + bias.plus : b.notional;
    if (price != null){
      const qty = biased / price;
      if (executeOpen(b.sym, b.side, qty, "auto", biased, null, b.notional)) opened++;
      else deferred.push({ ...b, biased });
    } else deferred.push({ ...b, biased });
  });

  log(`⟳ AUTO-RESTART — ${opened}/${snap.length} opened at fresh price`, opened ? "pos" : "neg");

  deferred.forEach(b => {
    if (!state.queue.some(q => normSym(q.sym) === normSym(b.sym) && q.side === b.side))
      state.queue.push({ sym: b.sym, side: b.side, notional: b.biased, baseNotional: b.notional, at: Date.now() });
  });
  if (deferred.length) log(`Deferred ${deferred.map(b => b.sym).join(", ")} — awaiting fresh price`, "neg");
  saveState();
}

/* ============================================================
   AUTO EXIT CHECKS (run on every tick, server-side)
   ============================================================ */
function checkAuto(){
  if (!state.positions.length) return;
  const tp = num(state.settings.tp);
  const sl = num(state.settings.sl);
  const tpAmt = tp > 0 ? tp : null;
  const slAmt = sl > 0 ? sl : null;
  if (tpAmt == null && slAmt == null) return;

  const stale = new Set();
  [...state.positions].forEach(p => {
    const cur = freshPx(p.sym);
    if (cur == null){ stale.add(normSym(p.sym)); return; }
    const net = netAt(p, cur);
    if (tpAmt != null && net >= tpAmt) closePos(p.id, `TP HIT (${fmtN(net)} ≥ ${fmtN(tpAmt)})`, cur);
    else if (slAmt != null && net <= -slAmt) closePos(p.id, `SL HIT (${fmtN(net)} ≤ ${fmtN(-slAmt)})`, cur);
  });
  if (stale.size) refreshStale([...stale]);
}

function checkDrift(){
  const up = num(state.settings.driftUp);
  const dn = num(state.settings.driftDn);
  const upT = up > 0 ? up : null;
  const dnT = dn > 0 ? dn : null;
  if (upT == null && dnT == null) return;
  if (!state.positions.length) return;
  if (state.restartPending || state.suppressRestart) return;

  const nt = totalNet(true);
  if (nt == null){ refreshStale(state.positions.map(p => p.sym)); return; }

  if (upT != null && nt >= upT){
    log(`⚠ +DRIFT HIT — total net +${fmtN(nt)} (≥ +${fmtN(upT)}) → closing ALL`, "neg");
    closeAll(`+DRIFT HIT`);
  } else if (dnT != null && nt <= -dnT){
    log(`⚠ −DRIFT HIT — total net ${fmtN(nt)} (≤ −${fmtN(dnT)}) → closing ALL`, "neg");
    closeAll(`−DRIFT HIT`);
  }
}

function onPrice(){
  flushQueue();
  checkAuto();
  checkDrift();
  maybeAutoRestart();
}

/* ============================================================
   HTTP API
   ============================================================ */
app.get("/api/state", (req, res) => {
  const now = Date.now();
  const positions = state.positions.map(p => {
    const cur = px(p.sym);
    const net = (cur != null) ? netAt(p, cur) : 0;
    const pct = (p.entry > 0 && cur != null)
      ? ((p.side === "LONG" ? (cur - p.entry) : (p.entry - cur)) / p.entry) * 100 : null;
    return { ...p, cur, net, pct, fresh: isFresh(p.sym) };
  });
  const queue = state.queue.map(q => ({ ...q }));

  const nt = totalNet(false), tf = totalFees();

  const bias = currentBias();

  res.json({
    ts: now,
    symbols: state.symbols,
    checked: [...state.checked],
    prices: state.prices,
    tickAt: state.tickAt,
    btcPrice: state.btcPrice,
    btc24h: state.btc24h,
    bias,
    settings: state.settings,
    session: state.session,
    positions,
    queue,
    cycleBatch: state.cycleBatch,
    totalNet: nt,
    totalFees: tf,
    wsStatus: state.wsStatus,
    wsStatusTxt: state.wsStatusTxt,
    restartPending: state.restartPending,
    logs: state.logs.slice(0, 60)
  });
});

app.post("/api/settings", (req, res) => {
  const s = req.body || {};
  for (const k of ["tp","sl","driftUp","driftDn","amt","plus","fee","biasMode","plusOverride","autoRestart","target"]){
    if (k in s) state.settings[k] = s[k];
  }
  saveState();
  res.json({ ok: true });
});

app.post("/api/open", (req, res) => {
  const side = (req.body && req.body.side) === "SHORT" ? "SHORT" : "LONG";
  openPos(side);
  res.json({ ok: true });
});

app.post("/api/close", (req, res) => {
  const id = parseInt(req.body && req.body.id, 10);
  if (isFinite(id)) closePos(id, "✕");
  res.json({ ok: true });
});

app.post("/api/closeAll", (req, res) => {
  state.queue = [];
  state.suppressRestart = true;
  state.cycleBatch = [];
  clearTimeout(restartTimer);
  state.restartPending = false;
  closeAll("EXIT ALL");
  saveState();
  setTimeout(() => { state.suppressRestart = false; }, 2000);
  res.json({ ok: true });
});

app.post("/api/resetSession", (req, res) => {
  state.session = { start: Date.now(), realizedPnl: 0, realizedFees: 0, tradeCount: 0, wins: 0, losses: 0 };
  saveState();
  log("Session stats reset — clock restarted", "pos");
  res.json({ ok: true });
});

app.post("/api/symbols", async (req, res) => {
  const body = req.body || {};
  const action = body.action;
  const symRaw = body.symbol;
  const S = normSym(symRaw);

  if (action === "add" && S){
    if (!state.symbols.includes(S)){
      state.symbols.push(S);
      log(`Added symbol ${S}`);
      if (!state.checked.includes(S)) state.checked.push(S);
      saveState();
      connect();
      await doFetch();
    }
  } else if (action === "remove" && S){
    if (state.positions.some(p => normSym(p.sym) === S)){
      log(`Cannot remove ${S} — close its positions first`, "neg");
    } else {
      state.symbols = state.symbols.filter(s => s !== S);
      state.checked = state.checked.filter(s => s !== S);
      state.queue = state.queue.filter(q => normSym(q.sym) !== S);
      removeFromBatch(S);
      delete state.prices[S]; delete state.prev[S]; delete state.tickAt[S];
      log(`Removed symbol ${S}`);
      saveState();
      connect();
    }
  } else if (action === "toggle" && S){
    if (body.checked && !state.checked.includes(S)) state.checked.push(S);
    else if (!body.checked) state.checked = state.checked.filter(s => s !== S);
    saveState();
  }
  res.json({ ok: true });
});

app.post("/api/fetch", async (req, res) => {
  const r = await doFetch();
  res.json(r || { ok: true });
});

app.post("/api/removeFromBatch", (req, res) => {
  const b = req.body || {};
  removeFromBatch(b.sym, b.side);
  saveState();
  res.json({ ok: true });
});

/* ============================================================
   BOOT
   ============================================================ */
loadState();
connect();
app.listen(PORT, () => {
  console.log(`Delta Neutral Simulator listening on :${PORT}`);
  setTimeout(doFetch, 900);
});

/* Save state periodically just in case */
setInterval(saveState, 5000);
process.on("SIGINT", () => { saveState(); process.exit(0); });
process.on("SIGTERM", () => { saveState(); process.exit(0); });
