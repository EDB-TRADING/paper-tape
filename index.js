/**
 * Paper Tape — server-side mark/execution watcher.
 *
 * WHY THIS EXISTS
 * ----------------
 * The app itself runs entirely in the browser: as long as a trader's tab is
 * open, their own client checks their pending/limit orders, TP/SL and
 * liquidations against live prices every ~30s and writes the result to
 * Firestore. That's fine while they're connected, but it means nothing
 * happens — orders never fill, positions never mark-to-market, the
 * leaderboard never moves for them — the moment they close the tab.
 *
 * This Cloud Function runs on a 1-minute schedule and does exactly what an
 * idle trader's own browser would have done, but only for traders who have
 * actually gone idle: every client heartbeats a `lastActiveAt` timestamp
 * into its own trader doc every ~10s whenever it has open positions/orders
 * (see persistAccount() in index.html). If that timestamp is fresher than
 * IDLE_THRESHOLD_MS, this function leaves that trader alone — their own
 * browser is already handling it live, and touching it here would just race
 * the client (double fills, double fees, etc). Once it goes stale, this
 * function takes over: pending-order fills, TP/SL, liquidations, and a
 * fresh mark-to-market equity write, so the leaderboard keeps moving too.
 *
 * v2 — COVERS BOTH BOOKS. The first version of this file only ever touched
 * the "normal" book (the top-level cash/positions/... fields). A trader's
 * "prop" book (the optional FTMO-style challenge account, stored under the
 * `propBook` field — see startChallenge()/persistAccount() in index.html)
 * got ZERO server-side coverage: leaving a prop position open with a stop-
 * loss set, then closing the tab, meant it would only ever get checked again
 * the next time that trader's own browser ran a price tick — potentially
 * filling far past the stop once they came back, exactly like the normal
 * book before this file existed at all. This version processes BOTH books,
 * independently, every run, and also re-evaluates the prop challenge's
 * daily-loss / max-drawdown FAIL conditions server-side (using the exact
 * same thresholds as updateChallengeStatus() in index.html) so a challenge
 * can't silently blow through its daily-loss or drawdown floor while nobody
 * is watching either.
 *
 * EXPLICITLY OUT OF SCOPE for this version (unchanged from v1's scope, and
 * deliberately not attempted here — see the note above evaluateChallengeFail()):
 *   - Badge awarding for challenge milestones (prop_step1_passed, etc.) —
 *     first_trade/ten_trades/first_liquidation/big_win (the ones tied
 *     directly to a fill/close, on EITHER book) are still awarded, same as
 *     v1 did for the normal book.
 *   - Challenge STEP PROGRESSION (step1 -> step2 -> funded) — passing a
 *     step/getting funded is only ever evaluated client-side, the next time
 *     that trader's own browser is open. Only the FAIL path (daily loss /
 *     drawdown breach) is enforced here. Practically: a trader who hits
 *     their profit target while offline just sees it recognized a little
 *     later (next time they open the app) instead of instantly — no money
 *     or fairness impact. A trader who BLOWS their daily-loss/drawdown floor
 *     while offline is caught immediately, which is the safety-relevant half.
 *   - Copy-trading mirroring (mirrorOrderToOtherBook() in index.html) — a
 *     brand-new fill created here (limit order filling, TP/SL, liquidation)
 *     never mirrors onto the other book. Copy-trading only mirrors NEW
 *     entries submitted live from the ticket anyway, so this only matters
 *     for the (rare) case of a limit order filling while idle with copy-
 *     trading on — it just won't mirror in that one case.
 *
 * DEPLOYMENT (you'll need to do this yourself — it needs your Firebase
 * login/project, which this assistant doesn't have access to):
 *   1. Firebase console → this project → upgrade to the "Blaze" (pay-as-you-
 *      go) plan. Required for Cloud Functions to make outbound network
 *      calls (to Binance/Crypto.com). At this traffic level (1 run/minute,
 *      a handful of small HTTP calls) it should stay within the free monthly
 *      quota, i.e. ~$0.
 *   2. `npm install -g firebase-tools` (if you don't have it), `firebase login`.
 *   3. From the project root (the folder that contains this `functions/`
 *      folder): `firebase deploy --only functions`.
 *   4. Check Firebase console → Functions → paperTapeMark shows green/
 *      healthy runs, and → Firestore → traders/<some id with an open
 *      position> to watch `equity` (and, if that trader has a prop book,
 *      `propEquity`) / `serverMarkedAt` update once you stop touching the
 *      tab for a minute or two.
 */

const {onSchedule} = require("firebase-functions/v2/scheduler");
const {initializeApp} = require("firebase-admin/app");
const {getFirestore, FieldValue, Timestamp} = require("firebase-admin/firestore");

initializeApp();
const db = getFirestore();

/* ============================= CONSTANTS (must match index.html) ============================= */
const MMR = 0.004;                  // flat maintenance margin rate
// Taker: market-order entries and every exit (SL/TP/liquidation-check/manual
// close all fill at the current market price). Maker: limit-order entries
// only. These MUST stay numerically identical to index.html's — they were
// found to have drifted (this file had 0.0005) while writing v2; fixed here.
const TAKER_FEE_RATE = 0.0004;      // 0.04% commission — market-order entry AND every non-liquidation exit
const MAKER_FEE_RATE = 0.0002;      // 0.02% commission — limit-order entry fill only (fee is pre-computed client-side into entryFeeEst, kept here for reference/parity)
const LIQUIDATION_FEE_RATE = 0.005; // extra 0.5% of notional penalty on liquidation (replaces the normal exit fee)
const IDLE_THRESHOLD_MS = 60 * 1000; // a client heartbeats every ~10s while it has positions/orders open in EITHER book

// Prop-firm challenge rules — must match CHALLENGE_RULES in index.html.
// Only the FAIL half (daily loss / drawdown) is enforced here — see the
// file-level comment above for why step progression is left to the client.
const CHALLENGE_RULES = {
  maxDailyLossPct: 5,
  maxDrawdownPct: 10,
};

const CRYPTO_API_BASE = "https://api.crypto.com/exchange/v1/public";
const BINANCE_API_BASE = "https://api.binance.com/api/v3";

// Mirrors ASSETS in index.html exactly (symbol/instrument/binanceSymbol/
// indexRatio only — decimals/name/icon/color are display-only, not needed
// here). v1 of this file was missing EURUSD and SILVER entirely, so those
// two assets got no server-side TP/SL/liquidation coverage at all — fixed
// here too. QQQ is delisted from the watchlist in index.html but stays in
// ASSETS there (and here) so a pre-existing open QQQ position can still be
// marked/liquidated/closed correctly.
const ASSETS = [
  // 2026-10 (v3380) : cryptos au prix des perps Binance (binanceFutures), comme index.html.
  {symbol: "BTC",    instrument: "BTC_USD",      binanceFutures: "BTCUSDT", binanceSymbol: "BTCUSDT"},
  {symbol: "ETH",    instrument: "ETH_USD",      binanceFutures: "ETHUSDT", binanceSymbol: "ETHUSDT"},
  {symbol: "SOL",    instrument: "SOL_USD",      binanceFutures: "SOLUSDT", binanceSymbol: "SOLUSDT"},
  {symbol: "HYPE",   instrument: "HYPE_USD",     binanceFutures: "HYPEUSDT", binanceSymbol: "HYPEUSDT"},
  {symbol: "EURUSD", instrument: "EUR_USDT",     binanceSymbol: "EURUSDT"},
  // 2026-10 : comme index.html, SPY/QQQ/GOLD/SILVER au prix des perps
  // Hyperliquid « xyz » (même source que le graphique TradingView), avec les
  // anciennes sources en secours (indexRatio ne s'applique qu'au secours Crypto.com).
  {symbol: "SPY",    hyperliquid: {dex: "xyz", coin: "SP500"},  instrument: "SPYUSD-PERP",  indexRatio: 10.08},
  {symbol: "QQQ",    hyperliquid: {dex: "xyz", coin: "XYZ100"}, instrument: "QQQUSD-PERP",  indexRatio: 41.07},
  {symbol: "GOLD",   hyperliquid: {dex: "xyz", coin: "GOLD"},   instrument: "PAXG_USD",     binanceSymbol: "PAXGUSDT"},
  {symbol: "SILVER", hyperliquid: {dex: "xyz", coin: "SILVER"}, instrument: "XAGUSD-PERP"},
  // Ajouts 2026-10 (mêmes sources que index.html). ZEC et ASTER ne sont pas
  // sur Crypto.com : secours sur les perps Hyperliquid (dex principal). Ce
  // secours compte vraiment ici, car Binance refuse souvent les requêtes
  // venant de serveurs aux États-Unis (où tournent les Cloud Functions).
  {symbol: "XRP",    instrument: "XRP_USD",      binanceFutures: "XRPUSDT", binanceSymbol: "XRPUSDT"},
  {symbol: "ZEC",                                binanceFutures: "ZECUSDT", binanceSymbol: "ZECUSDT",   hlFallback: {dex: "", coin: "ZEC"}},
  {symbol: "ASTER",                              binanceFutures: "ASTERUSDT", binanceSymbol: "ASTERUSDT", hlFallback: {dex: "", coin: "ASTER"}},
  {symbol: "SUI",    instrument: "SUI_USD",      binanceFutures: "SUIUSDT", binanceSymbol: "SUIUSDT"},
  {symbol: "DOGE",   instrument: "DOGE_USD",     binanceFutures: "DOGEUSDT", binanceSymbol: "DOGEUSDT"},
  {symbol: "LINK",   instrument: "LINK_USD",     binanceFutures: "LINKUSDT", binanceSymbol: "LINKUSDT"},
  {symbol: "AVAX",   instrument: "AVAX_USD",     binanceFutures: "AVAXUSDT", binanceSymbol: "AVAXUSDT"},
  // Pétrole : perp Hyperliquid du dex HIP-3 "xyz" (coin CL), comme dans index.html.
  // Il manquait jusqu'ici : une position OIL d'un trader inactif n'était jamais
  // marquée côté serveur.
  {symbol: "OIL",                                hyperliquid: {dex: "xyz", coin: "CL"}},
  // Forex (perps FX du dex xyz). invertBelow : prix inversé s'il arrive en
  // « dollar par yen/won » (< 1), pour rester en USD/JPY et USD/KRW comme le site.
  {symbol: "GBPUSD",                             hyperliquid: {dex: "xyz", coin: "GBP"}},
  {symbol: "USDJPY",                             hyperliquid: {dex: "xyz", coin: "JPY", invertBelow: 1}},
  {symbol: "USDKRW",                             hyperliquid: {dex: "xyz", coin: "KRW", invertBelow: 1}},
  {symbol: "DXY",                                hyperliquid: {dex: "xyz", coin: "DXY"}},
];

/* ============================= PRICE FETCH (Binance primary, Crypto.com fallback) ============================= */
async function fetchJson(url, timeoutMs) {
  const res = await fetch(url, {signal: AbortSignal.timeout(timeoutMs)});
  if (!res.ok) throw new Error("http_" + res.status);
  return res.json();
}
async function fetchTickerBinance(binanceSymbol) {
  const raw = await fetchJson(BINANCE_API_BASE + "/ticker/24hr?symbol=" + encodeURIComponent(binanceSymbol), 8000);
  if (!raw || raw.lastPrice == null) throw new Error("bad_payload");
  const last = parseFloat(raw.lastPrice);
  return {last, bid: parseFloat(raw.bidPrice) || last, ask: parseFloat(raw.askPrice) || last};
}
async function fetchTickerCryptoCom(instrument) {
  const json = await fetchJson(CRYPTO_API_BASE + "/get-tickers?instrument_name=" + encodeURIComponent(instrument), 10000);
  const raw = (json && json.result && Array.isArray(json.result.data)) ? json.result.data[0]
    : (json && Array.isArray(json.data)) ? json.data[0] : null;
  if (!raw) throw new Error("bad_payload");
  const last = parseFloat(raw.a);
  return {last, bid: parseFloat(raw.b) || last, ask: parseFloat(raw.k) || last};
}
// Hyperliquid : un seul appel metaAndAssetCtxs par dex et par passage
// (mis en cache dans `hlCache`, partagé par tous les marchés de ce dex).
async function fetchHyperliquidCtxs(dex) {
  const res = await fetch("https://api.hyperliquid.xyz/info", {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify(dex ? {type: "metaAndAssetCtxs", dex} : {type: "metaAndAssetCtxs"}),
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error("http_" + res.status);
  return res.json();
}
async function fetchTickerHyperliquid(hl, hlCache) {
  const key = hl.dex || "_main";
  if (!hlCache[key]) hlCache[key] = fetchHyperliquidCtxs(hl.dex).catch(() => null);
  const data = await hlCache[key];
  if (!Array.isArray(data)) throw new Error("bad_payload");
  const [meta, ctxs] = data;
  const prefixed = hl.dex ? hl.dex + ":" + hl.coin : hl.coin;
  const idx = (meta && Array.isArray(meta.universe)) ? meta.universe.findIndex((u) => u && (u.name === hl.coin || u.name === prefixed)) : -1;
  const ctx = (idx >= 0 && Array.isArray(ctxs)) ? ctxs[idx] : null;
  let last = ctx ? parseFloat(ctx.markPx) : NaN;
  if (!isFinite(last) || last <= 0) throw new Error("bad_payload");
  if (hl.invertBelow && last < hl.invertBelow) last = 1 / last;
  return {last, bid: last, ask: last};
}
async function fetchTickerBinanceFutures(sym) {
  const raw = await fetchJson("https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol=" + encodeURIComponent(sym), 8000);
  const bid = parseFloat(raw && raw.bidPrice), ask = parseFloat(raw && raw.askPrice);
  if (!isFinite(bid) || !isFinite(ask)) throw new Error("bad_payload");
  return {last: (bid + ask) / 2, bid, ask};
}
// Sources dans le même ordre que priceSources() d'index.html. indexRatio
// (ETF → indice) ne s'applique qu'au secours Crypto.com de SPY/QQQ.
async function fetchOne(asset, hlCache) {
  const tries = [];
  if (asset.hyperliquid) tries.push(() => fetchTickerHyperliquid(asset.hyperliquid, hlCache));
  if (asset.binanceFutures) tries.push(() => fetchTickerBinanceFutures(asset.binanceFutures));
  if (asset.binanceSymbol) tries.push(() => fetchTickerBinance(asset.binanceSymbol));
  if (asset.instrument) tries.push(async () => {
    const r = await fetchTickerCryptoCom(asset.instrument);
    const k = asset.indexRatio || 1;
    return {last: r.last * k, bid: r.bid * k, ask: r.ask * k};
  });
  if (asset.hlFallback) tries.push(() => fetchTickerHyperliquid(asset.hlFallback, hlCache));
  for (const t of tries) {
    try {
      const r = await t();
      if (r && isFinite(r.last)) return r;
    } catch (e) { /* source suivante */ }
  }
  return null;
}
async function buildPriceSnapshot() {
  const hlCache = {};
  const entries = await Promise.all(ASSETS.map(async (a) => [a.symbol, await fetchOne(a, hlCache)]));
  return Object.fromEntries(entries);
}

/* ============================= TRADING ENGINE (mirrors index.html's client-side logic) =============================
 * Every function below operates on a plain "book context" object
 * ({cash, marginUsed, positions, pendingOrders, history, stats, badges}) —
 * the server-side equivalent of what index.html keeps on `state.*` for
 * whichever book is currently "live" there. Unlike the client, there's no
 * need for a withParkedBook()-style swap trick here: this file just builds
 * one ctx per book and calls the same functions on each, in turn.
 */
function liqPriceFor(entry, side, lev) {
  const buf = (1 / lev) - MMR;
  return side === "long" ? entry * (1 - buf) : entry * (1 + buf);
}
function exitFillFor(prices, symbol, side) {
  const p = prices[symbol];
  if (!p) return null;
  return side === "long" ? p.bid : p.ask;
}
function srvId() {
  return "srv" + Math.random().toString(36).slice(2) + Date.now().toString(36);
}
function updatePerfStats(stats, pnl) {
  stats.tradesClosed = (stats.tradesClosed || 0) + 1;
  if (pnl === 0) { /* v3720 : break-even, ni gagnant ni perdant (comme le site) */ }
  else if (pnl > 0) {
    stats.wins = (stats.wins || 0) + 1;
    stats.grossProfit = (stats.grossProfit || 0) + pnl;
    stats.bestTrade = Math.max(stats.bestTrade || 0, pnl);
    if (stats.streakType === "win") { stats.currentStreak = (stats.currentStreak || 0) + 1; }
    else { stats.streakType = "win"; stats.currentStreak = 1; }
    stats.bestWinStreak = Math.max(stats.bestWinStreak || 0, stats.currentStreak);
  } else {
    stats.losses = (stats.losses || 0) + 1;
    stats.grossLoss = (stats.grossLoss || 0) + pnl;
    stats.worstTrade = Math.min(stats.worstTrade || 0, pnl);
    if (stats.streakType === "loss") { stats.currentStreak = (stats.currentStreak || 0) + 1; }
    else { stats.streakType = "loss"; stats.currentStreak = 1; }
    stats.bestLossStreak = Math.max(stats.bestLossStreak || 0, stats.currentStreak);
  }
}
// badges live in a SHARED object passed into every ctx (index.html keeps a
// single state.badges regardless of which book is active) — a badge earned
// via a prop-book fill and one earned via a normal-book fill land in the
// same place, exactly like the client.
function awardBadge(badges, id, nowIso) {
  if (badges[id]) return;
  badges[id] = {earnedAt: nowIso};
}
function pushHistory(ctx, entry) {
  ctx.history.unshift(entry);
  // v3710 : les trades au-delà des 200 derniers partent dans l'archive
  // traders/{id}/tradeArchive/{AAAA-MM} (compte normal), au lieu d'être perdus.
  if (ctx.history.length > 200 && ctx.archive) ctx.archive.push(...ctx.history.slice(200));
  ctx.history = ctx.history.slice(0, 200);
}
function openPositionCore(ctx, o) {
  const sizeUnits = o.notional / o.entry;
  const existing = ctx.positions.find((p) => p.symbol === o.symbol && p.side === o.side && p.mode === o.mode);
  if (existing) {
    const totalNotional = existing.notional + o.notional;
    const totalSizeUnits = existing.sizeUnits + sizeUnits;
    const totalMargin = existing.initMargin + o.initMargin;
    existing.entryPrice = totalNotional / totalSizeUnits;
    existing.sizeUnits = totalSizeUnits;
    existing.notional = totalNotional;
    existing.initMargin = totalMargin;
    existing.leverage = totalNotional / totalMargin;
    existing.maintMargin = totalNotional * MMR;
    existing.liqPrice = liqPriceFor(existing.entryPrice, o.side, existing.leverage);
    existing.entryFee = (existing.entryFee || 0) + o.entryFee;
    existing.feesPaid = (existing.feesPaid || 0) + o.entryFee;
    if (o.tpVal != null) existing.takeProfit = o.tpVal;
    if (o.slVal != null) existing.stopLoss = o.slVal;
  } else {
    const maintMargin = o.notional * MMR;
    const liq = liqPriceFor(o.entry, o.side, o.leverage);
    ctx.positions.push({
      id: srvId(), symbol: o.symbol, side: o.side, sizeUnits, notional: o.notional,
      entryPrice: o.entry, leverage: o.leverage, mode: o.mode, initMargin: o.initMargin, maintMargin, liqPrice: liq,
      entryFee: o.entryFee, feesPaid: o.entryFee, takeProfit: o.tpVal != null ? o.tpVal : null, stopLoss: o.slVal != null ? o.slVal : null,
      openedAt: new Date().toISOString(),
    });
  }
  ctx.stats.tradesOpened = (ctx.stats.tradesOpened || 0) + 1;
}
function closePositionFull(ctx, prices, id, reason, nowIso) {
  const idx = ctx.positions.findIndex((p) => p.id === id);
  if (idx < 0) return;
  const p = ctx.positions[idx];
  let pnl, exitFee, exitPrice;
  if (reason === "liquidated") {
    // v3560 — même règle que closePosition() du site : liquidation = toute la
    // marge perdue (isolée) ; en cross, perte au prix du marché + marge de
    // maintenance. Avant : marge + 0,5 % du notionnel, soit PLUS que la marge.
    if (p.mode === "isolated") {
      exitPrice = p.liqPrice;
      const gross = p.side === "long" ? (exitPrice - p.entryPrice) * p.sizeUnits : (p.entryPrice - exitPrice) * p.sizeUnits;
      pnl = -p.initMargin;
      exitFee = Math.max(0, p.initMargin + gross);
    } else {
      const ex = exitFillFor(prices, p.symbol, p.side);
      exitPrice = ex === null ? p.liqPrice : ex;
      const gross = p.side === "long" ? (exitPrice - p.entryPrice) * p.sizeUnits : (p.entryPrice - exitPrice) * p.sizeUnits;
      exitFee = p.notional * MMR;
      pnl = gross - exitFee;
    }
  } else {
    const exit = exitFillFor(prices, p.symbol, p.side);
    if (exit === null) return;
    const gross = p.side === "long" ? (exit - p.entryPrice) * p.sizeUnits : (p.entryPrice - exit) * p.sizeUnits;
    exitFee = p.notional * TAKER_FEE_RATE;
    pnl = gross - exitFee;
    exitPrice = exit;
  }
  ctx.cash = Math.max(0, ctx.cash + pnl);
  ctx.marginUsed = Math.max(0, ctx.marginUsed - p.initMargin);
  ctx.positions.splice(idx, 1);
  const feesPaid = (p.entryFee || 0) + exitFee;
  // Same shape as the client's closePosition() in index.html — in
  // particular `openedAt`/`entryPrice`/`notional` are what the admin
  // hedging detector (adminHedgeReportHtml()) needs to reconstruct a
  // trade's time window; without them a trade closed server-side while
  // idle would silently be invisible to that check.
  pushHistory(ctx, {
    id: srvId(), symbol: p.symbol, side: p.side, pnl, reason, exitFee, feesPaid, closedAt: nowIso,
    sizeUnits: p.sizeUnits, leverage: p.leverage, mode: p.mode, entryPrice: p.entryPrice, exitPrice, notional: p.notional, openedAt: p.openedAt,
    stopLoss: typeof p.stopLoss === "number" ? p.stopLoss : null, takeProfit: typeof p.takeProfit === "number" ? p.takeProfit : null,
  });
  updatePerfStats(ctx.stats, pnl);
  if (reason === "liquidated") awardBadge(ctx.badges, "first_liquidation", nowIso);
  else if (pnl >= 500) awardBadge(ctx.badges, "big_win", nowIso);
}
function closePositionPartial(ctx, prices, id, fraction, reason, nowIso) {
  const idx = ctx.positions.findIndex((p) => p.id === id);
  if (idx < 0) return;
  const p = ctx.positions[idx];
  const f = Math.min(1, Math.max(0.0001, fraction));
  if (f >= 0.9999) { closePositionFull(ctx, prices, id, reason, nowIso); return; }
  const exit = exitFillFor(prices, p.symbol, p.side);
  if (exit === null) return;
  const closedSizeUnits = p.sizeUnits * f;
  const closedNotional = p.notional * f;
  const closedMargin = p.initMargin * f;
  const closedEntryFee = (p.entryFee || 0) * f;
  const gross = p.side === "long" ? (exit - p.entryPrice) * closedSizeUnits : (p.entryPrice - exit) * closedSizeUnits;
  const exitFee = closedNotional * TAKER_FEE_RATE;
  const pnl = gross - exitFee;
  ctx.cash = Math.max(0, ctx.cash + pnl);
  ctx.marginUsed = Math.max(0, ctx.marginUsed - closedMargin);
  p.sizeUnits -= closedSizeUnits;
  p.notional -= closedNotional;
  p.initMargin -= closedMargin;
  p.maintMargin = p.notional * MMR;
  p.entryFee = (p.entryFee || 0) - closedEntryFee;
  const feesPaid = closedEntryFee + exitFee;
  pushHistory(ctx, {
    id: srvId(), symbol: p.symbol, side: p.side, pnl, reason, exitFee, feesPaid, closedAt: nowIso, partial: true, closedPct: Math.round(f * 100),
    sizeUnits: closedSizeUnits, leverage: p.leverage, mode: p.mode, entryPrice: p.entryPrice, exitPrice: exit, notional: closedNotional, openedAt: p.openedAt,
    stopLoss: typeof p.stopLoss === "number" ? p.stopLoss : null, takeProfit: typeof p.takeProfit === "number" ? p.takeProfit : null,
  });
  updatePerfStats(ctx.stats, pnl);
  if (pnl >= 500) awardBadge(ctx.badges, "big_win", nowIso);
}
function checkPendingOrders(ctx, prices, nowIso) {
  [...ctx.pendingOrders].forEach((o) => {
    const p = prices[o.symbol];
    if (!p) return;
    const cross = o.side === "long" ? p.ask : p.bid;
    const triggered = o.side === "long" ? cross <= o.limitPrice : cross >= o.limitPrice;
    if (!triggered) return;
    const idx = ctx.pendingOrders.findIndex((x) => x.id === o.id);
    if (idx < 0) return;
    ctx.pendingOrders.splice(idx, 1);
    openPositionCore(ctx, {
      symbol: o.symbol, side: o.side, mode: o.mode, leverage: o.leverage, entry: o.limitPrice,
      notional: o.notional, initMargin: o.initMargin, entryFee: o.entryFeeEst, tpVal: o.takeProfit, slVal: o.stopLoss,
    });
    ctx.cash -= o.entryFeeEst;
    awardBadge(ctx.badges, "first_trade", nowIso);
    if ((ctx.stats.tradesOpened || 0) >= 10) awardBadge(ctx.badges, "ten_trades", nowIso);
  });
}
function triggerTpSl(ctx, prices, p, which, nowIso) {
  const pct = which === "tp" ? (p.takeProfitPct != null ? p.takeProfitPct : 100) : (p.stopLossPct != null ? p.stopLossPct : 100);
  const frac = Math.min(100, Math.max(1, pct)) / 100;
  if (frac >= 0.9999) {
    closePositionFull(ctx, prices, p.id, which, nowIso);
  } else {
    closePositionPartial(ctx, prices, p.id, frac, which, nowIso);
    if (which === "tp") p.takeProfit = null; else p.stopLoss = null;
  }
}
function checkTPSL(ctx, prices, nowIso) {
  [...ctx.positions].forEach((p) => {
    if (p.takeProfit == null && p.stopLoss == null) return;
    const exit = exitFillFor(prices, p.symbol, p.side);
    if (exit === null) return;
    if (p.side === "long") {
      if (p.takeProfit != null && exit >= p.takeProfit) { triggerTpSl(ctx, prices, p, "tp", nowIso); return; }
      if (p.stopLoss != null && exit <= p.stopLoss) { triggerTpSl(ctx, prices, p, "sl", nowIso); return; }
    } else {
      if (p.takeProfit != null && exit <= p.takeProfit) { triggerTpSl(ctx, prices, p, "tp", nowIso); return; }
      if (p.stopLoss != null && exit >= p.stopLoss) { triggerTpSl(ctx, prices, p, "sl", nowIso); return; }
    }
  });
}
function checkLiquidations(ctx, prices, nowIso) {
  [...ctx.positions].forEach((p) => {
    if (p.mode !== "isolated") return;
    const exit = exitFillFor(prices, p.symbol, p.side);
    if (exit === null) return;
    const hit = p.side === "long" ? exit <= p.liqPrice : exit >= p.liqPrice;
    if (hit) closePositionFull(ctx, prices, p.id, "liquidated", nowIso);
  });
  const crossPos = ctx.positions.filter((p) => p.mode === "cross");
  if (crossPos.length) {
    const crossUnrl = crossPos.reduce((s, p) => {
      const exit = exitFillFor(prices, p.symbol, p.side);
      if (exit === null) return s;
      return s + (p.side === "long" ? (exit - p.entryPrice) * p.sizeUnits : (p.entryPrice - exit) * p.sizeUnits);
    }, 0);
    const crossMaint = crossPos.reduce((s, p) => s + p.maintMargin, 0);
    if (ctx.cash + crossUnrl <= crossMaint) {
      [...crossPos].forEach((p) => closePositionFull(ctx, prices, p.id, "liquidated", nowIso));
    }
  }
}
function computeEquity(ctx, prices) {
  const unrl = ctx.positions.reduce((s, p) => {
    const exit = exitFillFor(prices, p.symbol, p.side);
    if (exit === null) return s;
    return s + (p.side === "long" ? (exit - p.entryPrice) * p.sizeUnits : (p.entryPrice - exit) * p.sizeUnits);
  }, 0);
  return ctx.cash + unrl;
}
// Builds a mutable book context from a raw Firestore book object (either the
// trader doc itself, for the normal book, or its `propBook` field) — same
// shape/defaults hydrateStateFromDoc() applies client-side. `badges` is
// passed in BY REFERENCE and shared between the normal and prop ctx for one
// trader, exactly like the single state.badges object on the client.
function makeBookCtx(bookData, badges) {
  const d = bookData || {};
  return {
    cash: typeof d.cash === "number" ? d.cash : 100000,
    marginUsed: typeof d.marginUsed === "number" ? d.marginUsed : 0,
    positions: Array.isArray(d.positions) ? d.positions.map((p) => ({...p})) : [],
    pendingOrders: Array.isArray(d.pendingOrders) ? d.pendingOrders.map((o) => ({...o})) : [],
    history: Array.isArray(d.history) ? [...d.history] : [],
    stats: d.stats && typeof d.stats === "object" ? {...d.stats} : {},
    badges,
  };
}
function processBook(ctx, prices, nowIso) {
  checkPendingOrders(ctx, prices, nowIso);
  checkTPSL(ctx, prices, nowIso);
  checkLiquidations(ctx, prices, nowIso);
}

/* ============================= PROP CHALLENGE — FAIL CHECK ONLY =============================
 * Deliberately narrow: mirrors ONLY the failing half of updateChallengeStatus()
 * in index.html (daily-loss floor + overall-drawdown floor -> phase:"failed").
 * Passing a step / getting funded is intentionally left to the client (see
 * the file-level comment for why) — this function never advances `phase`
 * forward, only ever to "failed". `dayKey`/`dailyStartEquity` ARE rolled
 * forward here exactly like the client does, because skipping that would
 * make the daily-loss check itself wrong (comparing today's equity against
 * a stale prior day's starting point) the moment a day boundary is crossed
 * while a trader is offline — this isn't scope creep, it's required for the
 * one check this function does perform to be correct.
 */
function todayKeyUTC() {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD — same as index.html's todayKey() (Date#toISOString is always UTC)
}
function evaluateChallengeFail(challenge, eq, nowIso) {
  if (!challenge || challenge.phase === "failed" || eq == null) return challenge;
  const c = {...challenge};
  const key = todayKeyUTC();
  if (c.dayKey !== key) { c.dayKey = key; c.dailyStartEquity = eq; }
  const dailyFloor = c.dailyStartEquity * (1 - CHALLENGE_RULES.maxDailyLossPct / 100);
  const overallFloor = c.startCash * (1 - CHALLENGE_RULES.maxDrawdownPct / 100);
  if (eq <= dailyFloor || eq <= overallFloor) {
    c.phase = "failed";
    c.failReason = eq <= overallFloor ? "drawdown" : "daily";
    c.failedAt = nowIso;
  }
  return c;
}

/* ============================= PER-TRADER PROCESSING ============================= */
// Écriture conditionnelle (precondition lastUpdateTime) : elle n'aboutit que
// si le document n'a pas bougé depuis la lecture de la requête. Si le
// navigateur du trader a sauvegardé entre-temps (fermeture d'une position,
// retour sur l'onglet…), Firestore refuse l'écriture et on ne touche à rien.
// Avant, une écriture basée sur une lecture un peu ancienne pouvait
// « ressusciter » une position que l'élève venait de fermer. Contrairement à
// une transaction, ça ne coûte aucune lecture supplémentaire.
async function processTrader(doc, prices, nowIso) {
  const update = processTraderData(doc.data(), prices, nowIso);
  try {
    await doc.ref.update(update, {lastUpdateTime: doc.updateTime});
  } catch (e) {
    if (e && (e.code === 9 || e.code === "failed-precondition" || /FAILED_PRECONDITION/i.test(String(e.message)))) return; // modifié entre-temps : on laisse la main au client
    throw e;
  }
  const arch = update.__archive;
  if (arch && arch.length) {
    const byMonth = {};
    arch.forEach((t) => { const k = String(t.closedAt || "").slice(0, 7) || "inconnu"; (byMonth[k] = byMonth[k] || []).push(t); });
    await Promise.all(Object.keys(byMonth).map((k) => doc.ref.collection("tradeArchive").doc(k).set({trades: FieldValue.arrayUnion(...byMonth[k])}, {merge: true})));
  }
}
function processTraderData(d, prices, nowIso) {
  const badges = d.badges && typeof d.badges === "object" ? {...d.badges} : {};

  const normalCtx = makeBookCtx(d, badges);
  normalCtx.archive = [];
  const openedBefore = (normalCtx.stats.tradesOpened || 0);
  processBook(normalCtx, prices, nowIso);
  const normalEquity = computeEquity(normalCtx, prices);

  const hasPropBook = d.propBook && typeof d.propBook === "object";
  let propCtx = null;
  let propEquityVal = null;
  if (hasPropBook) {
    propCtx = makeBookCtx(d.propBook, badges);
    var propOpenedBefore = (propCtx.stats.tradesOpened || 0);
    processBook(propCtx, prices, nowIso);
    propEquityVal = computeEquity(propCtx, prices);
  }

  let challenge = d.challenge && typeof d.challenge === "object" ? d.challenge : null;
  if (challenge && propEquityVal != null) {
    challenge = evaluateChallengeFail(challenge, propEquityVal, nowIso);
  }

  // Re-derive the read-cost-saving flag from what's ACTUALLY left open after
  // this run (fills/TP-SL/liquidations above may have just closed the last
  // one) — see the query in paperTapeMark below and persistAccount() in
  // index.html, which is the other writer of this field. Keeping it correct
  // here means a trader whose last position gets liquidated while they're
  // offline drops out of next month's scan on their own, without needing
  // their browser to reopen first.
  const stillHasActivity = normalCtx.positions.length > 0 || normalCtx.pendingOrders.length > 0 ||
    (hasPropBook && (propCtx.positions.length > 0 || propCtx.pendingOrders.length > 0));

  // Weekly leaderboard: mirrors persistAccount()'s client-side formula
  // exactly (weeklyPnl = current equity - the snapshot weeklyLeaderboardReset
  // took at the start of this week) so an idle trader's weekly ranking keeps
  // moving even while their browser is closed, instead of freezing the
  // moment they went idle.
  const weekStartEquity = typeof d.weekStartEquity === "number" ? d.weekStartEquity : normalEquity;

  const update = {
    cash: normalCtx.cash,
    marginUsed: normalCtx.marginUsed,
    positions: normalCtx.positions,
    pendingOrders: normalCtx.pendingOrders,
    history: normalCtx.history,
    stats: normalCtx.stats,
    equity: normalEquity,
    weeklyPnl: normalEquity - weekStartEquity,
    badges,
    serverMarkedAt: nowIso,
    hasOpenActivity: stillHasActivity,
    // Numéro de version (voir persistAccount dans index.html) : un navigateur
    // resté sur une version antérieure verra sa sauvegarde refusée et
    // reprendra cet état au lieu de l'écraser.
    rev: (typeof d.rev === "number" ? d.rev : 0) + 1,
  };
  if (hasPropBook) {
    update.propBook = {
      cash: propCtx.cash, marginUsed: propCtx.marginUsed,
      positions: propCtx.positions, pendingOrders: propCtx.pendingOrders,
      history: propCtx.history, stats: propCtx.stats,
    };
    update.propEquity = propEquityVal;
  }
  if (challenge) update.challenge = challenge;

  // Compteur à vie des positions ouvertes (tous comptes confondus, voir
  // lifetimeTradesOpened() dans index.html) : on y ajoute les ordres limites
  // exécutés ici, et le badge « Trader régulier » se juge sur ce total.
  const openedNow = ((normalCtx.stats.tradesOpened || 0) - openedBefore) +
    (hasPropBook ? ((propCtx.stats.tradesOpened || 0) - propOpenedBefore) : 0);
  const statsSum = (normalCtx.stats.tradesOpened || 0) + (hasPropBook ? (propCtx.stats.tradesOpened || 0) : 0);
  const lifetime = Math.max((typeof d.tradesOpenedTotal === "number" ? d.tradesOpenedTotal : 0) + openedNow, statsSum);
  if (openedNow > 0) update.tradesOpenedTotal = lifetime;
  if (lifetime >= 1) awardBadge(badges, "first_trade", nowIso);
  if (lifetime >= 10) awardBadge(badges, "ten_trades", nowIso);

  if (normalCtx.archive.length) Object.defineProperty(update, "__archive", {value: normalCtx.archive, enumerable: false});
  return update;
}

/* ============================= SCHEDULED ENTRY POINT ============================= */
exports.paperTapeMark = onSchedule(
  {schedule: "every 1 minutes", timeoutSeconds: 120, memory: "256MiB"},
  async () => {
    const prices = await buildPriceSnapshot();
    const now = Date.now();
    const nowIso = new Date(now).toISOString();
    // Was `db.collection("traders").get()` — an unbounded read of EVERY
    // trader doc, every minute, regardless of activity. `hasOpenActivity`
    // (written by persistAccount() in index.html, and re-derived below after
    // every run) mirrors exactly the same "does either book have a position
    // or a pending order" check this loop used to do in memory AFTER paying
    // for the read — filtering it into the query itself means Firestore only
    // bills/returns the docs that could possibly need work, so an account
    // with nothing open costs ~0 reads instead of one every 60s forever.
    // NOTE (one-time transition caveat): a trader whose position was already
    // open before this field existed won't be picked up here until their own
    // browser calls persistAccount() again (which happens continuously while
    // the tab is open/active) and sets the flag for the first time.
    const snap = await db.collection("traders").where("hasOpenActivity", "==", true).get();
    const jobs = [];
    snap.forEach((doc) => {
      const d = doc.data();
      const lastActive = d.lastActiveAt ? Date.parse(d.lastActiveAt) : 0;
      if (now - lastActive <= IDLE_THRESHOLD_MS) return; // their own browser is live, don't race it
      jobs.push(
        processTrader(doc, prices, nowIso).catch((e) => console.error("processTrader failed for", doc.id, e)),
      );
    });
    await Promise.all(jobs);
    console.log(`paperTapeMark: processed ${jobs.length} idle trader(s) of ${snap.size} candidate(s) with open activity.`);
  },
);

/* ============================= WEEKLY LEADERBOARD RESET =============================
 * Unlike paperTapeMark above, this DOES need a full collection scan — the
 * whole point is to snapshot every trader's starting-of-week equity at the
 * same instant, including traders who are offline right now (an idle
 * trader's weekly PnL has to reset too, or the weekly leaderboard would
 * quietly favor whoever happens to be online at the reset moment). That's
 * fine cost-wise: this only runs once a week (52 full scans/year), a rounding
 * error next to what paperTapeMark used to cost every single day before the
 * hasOpenActivity fix above.
 */
// Monday-anchored week key ("2026-09-15", the Monday date of that week) in
// Europe/Paris — only needs to change once a week and never collide across
// weeks; doesn't need to match any client-side format.
function isoWeekKeyParis(date) {
  const parisNow = new Date(date.toLocaleString("en-US", {timeZone: "Europe/Paris"}));
  const dayIdx = (parisNow.getDay() + 6) % 7; // Monday=0 .. Sunday=6
  const monday = new Date(parisNow);
  monday.setDate(parisNow.getDate() - dayIdx);
  return monday.toISOString().slice(0, 10);
}
exports.weeklyLeaderboardReset = onSchedule(
  {schedule: "0 0 * * 1", timeZone: "Europe/Paris", timeoutSeconds: 300, memory: "256MiB"},
  async () => {
    const weekKey = isoWeekKeyParis(new Date());
    const snap = await db.collection("traders").get();
    let batch = db.batch();
    let batchCount = 0;
    let totalWrites = 0;
    for (const doc of snap.docs) {
      const d = doc.data();
      const equity = typeof d.equity === "number" ? d.equity : 100000;
      batch.update(doc.ref, {weekStartEquity: equity, weekKey, weeklyPnl: 0});
      batchCount++;
      totalWrites++;
      if (batchCount >= 400) { // stay comfortably under Firestore's 500-write batch cap
        await batch.commit();
        batch = db.batch();
        batchCount = 0;
      }
    }
    if (batchCount > 0) await batch.commit();
    console.log(`weeklyLeaderboardReset: reset ${totalWrites} trader(s) for week of ${weekKey}.`);
  },
);

/* ============================= COURS EUROPÉENS (onglet Investissement) ============================= */
// Les actions/ETF européens (LVMH, Orange, CAC 40, MSCI World…) n'ont pas de
// flux public utilisable directement depuis le navigateur (CORS). Cette
// fonction les récupère côté serveur toutes les 5 minutes pendant la séance
// et les écrit dans UN document Firestore : marketData/euQuotes. Chaque
// navigateur l'écoute (1 lecture par mise à jour, seulement sur l'onglet
// Investir ou si le portefeuille contient ces titres).
// Doit rester alignée avec les `src:{eu:"…"}` de INVEST_ASSETS dans index.html.
//
// v3 : Yahoo (429) et Google Finance (pas de prix) refusent les serveurs
// Google. Source principale désormais : Tradegate Exchange (Berlin), dont la
// page publique de chaque titre s'appuie sur un petit JSON par code ISIN
// (dernier prix, clôture de la veille, variation %). Ces titres y cotent en
// euros, en continu de 8h à 22h, à des prix alignés sur Euronext / Xetra.
// Yahoo reste en secours si Tradegate ne répond pas pour un titre.
const EU_QUOTE_ISINS = {
  "CAC.PA": "FR0007052782",   // ETF CAC 40
  "CW8.PA": "LU1681043599",   // ETF MSCI World
  "MEUD.PA": "LU0908500753",  // ETF Stoxx Europe 600
  "PAEEM.PA": "FR0013412020", // ETF Marchés émergents (PEA)
  "MC.PA": "FR0000121014",    // LVMH
  "RMS.PA": "FR0000052292",   // Hermès
  "OR.PA": "FR0000120321",    // L'Oréal
  "TTE.PA": "FR0000120271",   // TotalEnergies
  "AIR.PA": "NL0000235190",   // Airbus
  "SAF.PA": "FR0000073272",   // Safran
  "ORA.PA": "FR0000133308",   // Orange
  "BNP.PA": "FR0000131104",   // BNP Paribas
  "GLE.PA": "FR0000130809",   // Société Générale
  "SAN.PA": "FR0000120578",   // Sanofi
  "SU.PA": "FR0000121972",    // Schneider Electric
  "AI.PA": "FR0000120073",    // Air Liquide
  "BN.PA": "FR0000120644",    // Danone
  "KER.PA": "FR0000121485",   // Kering
  "CAP.PA": "FR0000125338",   // Capgemini
  "DSY.PA": "FR0014003TT8",   // Dassault Systèmes
  "ASML.AS": "NL0010273215",  // ASML
  "SAP.DE": "DE0007164600",   // SAP
  "SIE.DE": "DE0007236101",   // Siemens
  "EL.PA": "FR0000121667",    // EssilorLuxottica
  "STMPA.PA": "NL0000226223", // STMicroelectronics
  "SOI.PA": "FR0013227113",   // Soitec
  "IFX.DE": "DE0006231004",   // Infineon
  "ASM.AS": "NL0000334118",   // ASM International
  "BESI.AS": "NL0012866412",  // BE Semiconductor Industries
  // ETF obligataires (compte-titres uniquement, v3260)
  "EUNH.DE": "IE00B4WXJJ64",  // iShares Core € Govt Bond
  "IBGS.AS": "IE00B14X4Q57",  // iShares € Govt Bond 1-3yr
  "EUN5.DE": "IE00B3F81R35",  // iShares Core € Corp Bond
  "SXRM.DE": "IE00B3VWN518",  // iShares $ Treasury Bond 7-10yr (Acc)
};
const EU_QUOTE_SYMBOLS = Object.keys(EU_QUOTE_ISINS);
const BROWSER_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
  "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
};
// Tradegate mixes plain numbers (400.05) and German-formatted strings
// ("1 381,50", "400,20") in the same object.
function tgNum(v) {
  if (typeof v === "number") return isFinite(v) ? v : null;
  if (typeof v !== "string") return null;
  const n = parseFloat(v.replace(/[\s  ]/g, "").replace(/\.(?=\d{3}(\D|$))/g, "").replace(",", "."));
  return isFinite(n) ? n : null;
}
async function tradegateQuote(sym, log) {
  const isin = EU_QUOTE_ISINS[sym];
  if (!isin) return null;
  try {
    const res = await fetch("https://www.tradegatebsx.com/refresh.php?isin=" + isin, {
      headers: {...BROWSER_HEADERS, "Accept": "application/json, text/javascript, */*; q=0.01", "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.tradegatebsx.com/orderbuch.php?isin=" + isin},
      signal: AbortSignal.timeout(7000),
    });
    if (!res.ok) { log.push("tradegate " + sym + " " + res.status); return null; }
    const j = await res.json();
    let px = tgNum(j.last);
    const bid = tgNum(j.bid), ask = tgNum(j.ask);
    if (!(px > 0) && bid > 0 && ask > 0) px = (bid + ask) / 2; // no trade yet today: use the mid
    if (!(px > 0)) { log.push("tradegate " + sym + " no price"); return null; }
    return {px, prev: tgNum(j.close), cur: "EUR", t: Date.now(), src: "tradegate"};
  } catch (e) { log.push("tradegate " + sym + " error " + (e && e.name)); return null; }
}
async function yahooChart(sym, log) {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/" + encodeURIComponent(sym) + "?range=1d&interval=5m";
    const res = await fetch(url, {headers: {...BROWSER_HEADERS, Accept: "application/json"}, signal: AbortSignal.timeout(6000)});
    if (!res.ok) { log.push("yahoo " + sym + " " + res.status); return null; }
    const meta = (((await res.json()).chart || {}).result || [])[0]?.meta;
    const px = meta && Number(meta.regularMarketPrice);
    if (!px || !isFinite(px)) return null;
    return {px, prev: Number(meta.chartPreviousClose || meta.previousClose) || null, cur: meta.currency || "EUR",
      t: meta.regularMarketTime ? meta.regularMarketTime * 1000 : Date.now(), src: "yahoo"};
  } catch (e) { log.push("yahoo " + sym + " error"); return null; }
}
function parisClock(d) {
  const p = new Intl.DateTimeFormat("en-GB", {timeZone: "Europe/Paris", weekday: "short", hour: "2-digit", minute: "2-digit", hour12: false})
    .formatToParts(d).reduce((a, x) => { a[x.type] = x.value; return a; }, {});
  return {wd: p.weekday, mins: (+p.hour) * 60 + (+p.minute)};
}
exports.euQuotesRefresh = onSchedule(
  {schedule: "every 5 minutes", timeoutSeconds: 180, memory: "256MiB"},
  async () => {
    try { await ratesRefresh(); } catch (e) { console.warn("rates", e && e.message); }
    const ref = db.collection("marketData").doc("euQuotes");
    const {wd, mins} = parisClock(new Date());
    const inSession = wd !== "Sat" && wd !== "Sun" && mins >= 8 * 60 + 55 && mins <= 17 * 60 + 50;
    const snap = await ref.get();
    const prevDoc = snap.exists ? snap.data() : null;
    if (!inSession) {
      // Outside market hours prices don't move: only fill the doc if it's
      // missing, empty or very old (first deploy, long outage…).
      const last = prevDoc && prevDoc.updatedAt ? prevDoc.updatedAt.toMillis() : 0;
      if (prevDoc && (prevDoc.okCount || 0) > 0 && Date.now() - last < 12 * 3600 * 1000) return;
    }
    const log = [];
    const quotes = {};
    let yahooFails = 0;
    for (const sym of EU_QUOTE_SYMBOLS) {
      let q = await tradegateQuote(sym, log);
      if (!q && yahooFails < 2) { q = await yahooChart(sym, log); if (!q) yahooFails++; }
      if (q) quotes[sym] = q;
      await new Promise(r => setTimeout(r, 150)); // be gentle with the sources
    }
    const ok = Object.keys(quotes).length;
    const bySource = Object.values(quotes).reduce((a, q) => { a[q.src] = (a[q.src] || 0) + 1; return a; }, {});
    console.log("euQuotesRefresh: " + ok + "/" + EU_QUOTE_SYMBOLS.length + " quotes " + JSON.stringify(bySource) + (log.length ? " | " + log.slice(0, 8).join(" ; ") : ""));
    if (!ok) return;
    // merge:true keeps the previous value of any symbol that failed this time.
    await ref.set({quotes, updatedAt: FieldValue.serverTimestamp(), okCount: ok, sources: bySource}, {merge: true});
    try { await euHistoryAppend(quotes); } catch (e) { console.warn("euHistory", e && e.message); }
  }
);
// Historique quotidien des titres européens (graphique de la fiche actif,
// page Investir) : un point {d:"AAAA-MM-JJ", p:cours en €} par jour de Bourse
// et par titre, 400 jours max, dans marketData/euHistory. Écrit au plus toutes
// les 30 min (le dernier point du jour finit donc proche de la clôture).
// Les titres avec moins de 20 points sont complétés une fois par jour avec
// l'historique 1 an de Yahoo, si la source répond.
async function yahooDaily(sym) {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/" + encodeURIComponent(sym) + "?range=1y&interval=1d";
    const res = await fetch(url, {headers: {...BROWSER_HEADERS, Accept: "application/json"}, signal: AbortSignal.timeout(6000)});
    if (!res.ok) return null;
    const r = (((await res.json()).chart || {}).result || [])[0];
    const ts = r && r.timestamp, cl = r && r.indicators && r.indicators.quote && r.indicators.quote[0] && r.indicators.quote[0].close;
    if (!Array.isArray(ts) || !Array.isArray(cl)) return null;
    const fmt = new Intl.DateTimeFormat("en-CA", {timeZone: "Europe/Paris"});
    return ts.map((t, i) => ({d: fmt.format(new Date(t * 1000)), p: Number(cl[i])})).filter(x => x.p > 0 && isFinite(x.p));
  } catch (e) { return null; }
}
// Courbes des taux (page Investir, obligations en direct) : marketData/rates.
//  - euro : courbe des rendements au pair de TOUS les États de la zone euro
//    publiée chaque jour par la BCE (série YC … G_N_C … PY_xY), API publique ;
//  - dollar : courbe officielle du Trésor américain (CSV quotidien), avec
//    Yahoo (^IRX ^FVX ^TNX ^TYX) en secours.
// Rafraîchi au plus toutes les 3 h ; historique quotidien de 300 jours (le
// graphique de la fiche obligation est recalculé à partir de ces taux).
const ECB_TENORS = ["1", "2", "3", "5", "7", "10", "15", "20", "30"];
function parseCsv(text) {
  const rows = []; let row = [], cur = "", q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += c; }
    else if (c === '"') q = true;
    else if (c === ",") { row.push(cur); cur = ""; }
    else if (c === "\n" || c === "\r") { if (c === "\r" && text[i + 1] === "\n") i++; row.push(cur); cur = ""; if (row.some(x => x !== "")) rows.push(row); row = []; }
    else cur += c;
  }
  if (cur !== "" || row.length) { row.push(cur); rows.push(row); }
  return rows;
}
async function ecbCurve(n, log) {
  const byDay = {};
  for (const t of ECB_TENORS) {
    try {
      const url = "https://data-api.ecb.europa.eu/service/data/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_" + t + "Y?lastNObservations=" + n + "&format=csvdata";
      const res = await fetch(url, {headers: {Accept: "text/csv"}, signal: AbortSignal.timeout(10000)});
      if (!res.ok) { log.push("ecb " + t + " " + res.status); continue; }
      const rows = parseCsv(await res.text());
      const hd = rows[0] || [], iT = hd.indexOf("TIME_PERIOD"), iV = hd.indexOf("OBS_VALUE");
      if (iT < 0 || iV < 0) { log.push("ecb " + t + " format"); continue; }
      rows.slice(1).forEach(r => { const v = parseFloat(r[iV]); if (r[iT] && isFinite(v)) (byDay[r[iT]] = byDay[r[iT]] || {})[t] = Math.round(v * 10000) / 10000; });
    } catch (e) { log.push("ecb " + t + " error"); }
  }
  return Object.keys(byDay).sort().map(d => ({d, y: byDay[d]})).filter(r => Object.keys(r.y).length >= 5);
}
async function treasuryCurve(years, log) {
  const out = [];
  for (const Y of years) {
    try {
      const url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/" + Y + "/all?type=daily_treasury_yield_curve&field_tdr_date_value=" + Y + "&page&_format=csv";
      const res = await fetch(url, {headers: BROWSER_HEADERS, signal: AbortSignal.timeout(10000)});
      if (!res.ok) { log.push("treasury " + Y + " " + res.status); continue; }
      const rows = parseCsv(await res.text()), hd = rows[0] || [];
      const cols = hd.map(h => { const m = /^([\d.]+)\s*(Mo|Month|Yr)/i.exec(h.trim()); return m ? String(Math.round(parseFloat(m[1]) / (/^y/i.test(m[2]) ? 1 : 12) * 10000) / 10000) : null; });
      rows.slice(1).forEach(r => {
        const m = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec((r[0] || "").trim()); if (!m) return;
        const y = {}; cols.forEach((k, i) => { const v = parseFloat(r[i]); if (k && isFinite(v)) y[k] = v; });
        if (Object.keys(y).length >= 5) out.push({d: m[3] + "-" + m[1] + "-" + m[2], y});
      });
    } catch (e) { log.push("treasury " + Y + " error"); }
  }
  return out.sort((a, b) => a.d < b.d ? -1 : 1);
}
async function yahooUsCurve(log) {
  const map = {"^IRX": "0.25", "^FVX": "5", "^TNX": "10", "^TYX": "30"}, y = {};
  for (const sym of Object.keys(map)) { const q = await yahooChart(sym, log); if (q && q.px > 0) y[map[sym]] = q.px; }
  if (Object.keys(y).length < 3) return [];
  return [{d: new Intl.DateTimeFormat("en-CA", {timeZone: "America/New_York"}).format(new Date()), y}];
}
async function ratesRefresh() {
  const ref = db.collection("marketData").doc("rates");
  const snap = await ref.get();
  const R = snap.exists ? snap.data() : {};
  const last = R.updatedAt ? R.updatedAt.toMillis() : 0;
  if (Date.now() - last < 3 * 3600 * 1000) return;
  const hist = R.hist || {eur: [], usd: []}, log = [];
  const merge = (arr, rows) => { rows.forEach(r => { const i = arr.findIndex(x => x.d === r.d); if (i >= 0) arr[i] = r; else arr.push(r); }); return arr.sort((a, b) => a.d < b.d ? -1 : 1).slice(-300); };
  const eur = await ecbCurve((hist.eur || []).length < 200 ? 300 : 5, log);
  const Y = new Date().getFullYear();
  let usd = await treasuryCurve((hist.usd || []).length < 200 ? [Y - 1, Y] : [Y], log);
  if (!usd.length) usd = await yahooUsCurve(log);
  hist.eur = merge(hist.eur || [], eur);
  hist.usd = merge(hist.usd || [], usd);
  console.log("ratesRefresh: eur +" + eur.length + " usd +" + usd.length + (log.length ? " | " + log.slice(0, 6).join(" ; ") : ""));
  if (!hist.eur.length && !hist.usd.length) return;
  await ref.set({eur: hist.eur[hist.eur.length - 1] || null, usd: hist.usd[hist.usd.length - 1] || null, hist, updatedAt: FieldValue.serverTimestamp()});
}
async function euHistoryAppend(quotes) {
  const ref = db.collection("marketData").doc("euHistory");
  const day = new Intl.DateTimeFormat("en-CA", {timeZone: "Europe/Paris"}).format(new Date());
  const snap = await ref.get();
  const H = snap.exists ? snap.data() : {};
  const series = H.series || {};
  const last = H.updatedAt ? H.updatedAt.toMillis() : 0;
  const needBackfill = H.backfillDay !== day && EU_QUOTE_SYMBOLS.some(s => (series[s] || []).length < 20);
  if (Date.now() - last < 30 * 60 * 1000 && !needBackfill) return;
  const upsert = (sym, d, p) => {
    const arr = series[sym] || (series[sym] = []);
    const i = arr.findIndex(x => x.d === d);
    if (i >= 0) arr[i] = {d, p}; else arr.push({d, p});
  };
  if (needBackfill) {
    let fails = 0;
    for (const sym of EU_QUOTE_SYMBOLS) {
      if ((series[sym] || []).length >= 20 || fails >= 3) continue;
      const pts = await yahooDaily(sym);
      if (!pts) { fails++; continue; }
      pts.forEach(x => upsert(sym, x.d, Math.round(x.p * 10000) / 10000));
      await new Promise(r => setTimeout(r, 150));
    }
  }
  Object.keys(quotes).forEach(sym => upsert(sym, day, Math.round(quotes[sym].px * 10000) / 10000));
  Object.keys(series).forEach(sym => { series[sym] = series[sym].sort((a, b) => a.d < b.d ? -1 : 1).slice(-400); });
  await ref.set({series, updatedAt: FieldValue.serverTimestamp(), backfillDay: needBackfill ? day : (H.backfillDay || null)});
}

/* ============================= PAIEMENT EDB+ (STRIPE) =============================
 * Stripe appelle cette fonction (webhook) quand un paiement de l'adhésion
 * EDB+ est validé sur le lien de paiement (« Payment Link ») configuré dans
 * index.html (EDB_PLUS_STRIPE_LINK). La fonction :
 *  1. vérifie la signature Stripe (secret STRIPE_WEBHOOK_SECRET, stocké dans
 *     Secret Manager — jamais dans le code) ;
 *  2. retrouve le compte : client_reference_id (= identifiant du compte, ajouté
 *     au lien par l'appli), sinon l'e-mail du payeur s'il correspond à un seul
 *     compte ;
 *  3. active EDB+ pour un an (prolonge d'un an une adhésion encore en cours) ;
 *  4. enregistre le paiement dans la collection « payments » (une seule fois
 *     par session Stripe, même si Stripe renvoie l'événement) — un paiement
 *     qu'on n'a pas pu rattacher y apparaît avec matched:false, pour le bureau.
 * Voir SERVER_SETUP.md, section « Paiement EDB+ par Stripe ».
 */
const {onRequest} = require("firebase-functions/v2/https");
const {defineSecret} = require("firebase-functions/params");
const crypto = require("crypto");
const STRIPE_WEBHOOK_SECRET = defineSecret("STRIPE_WEBHOOK_SECRET");
const EDB_PLUS_MIN_CENTS = 2500; // 25 €

function verifyStripeSignature(rawBody, header, secret, toleranceSec = 300) {
  if (!rawBody || !header || !secret) return false;
  let t = null;
  const sigs = [];
  String(header).split(",").forEach((part) => {
    const i = part.indexOf("=");
    if (i < 0) return;
    const k = part.slice(0, i).trim(), v = part.slice(i + 1).trim();
    if (k === "t") t = v;
    else if (k === "v1") sigs.push(v);
  });
  if (!t || !sigs.length) return false;
  if (Math.abs(Date.now() / 1000 - Number(t)) > toleranceSec) return false;
  const expected = crypto.createHmac("sha256", secret).update(t + "." + rawBody.toString("utf8")).digest("hex");
  const exp = Buffer.from(expected, "utf8");
  return sigs.some((s) => {
    const b = Buffer.from(s, "utf8");
    return b.length === exp.length && crypto.timingSafeEqual(b, exp);
  });
}

async function findTraderForSession(tx, s) {
  const ref = s.client_reference_id ? db.collection("traders").doc(String(s.client_reference_id)) : null;
  if (ref) {
    const snap = await tx.get(ref);
    if (snap.exists) return snap;
  }
  const email = ((s.customer_details && s.customer_details.email) || s.customer_email || "").trim();
  if (!email) return null;
  for (const e of [...new Set([email, email.toLowerCase()])]) {
    const q = await tx.get(db.collection("traders").where("email", "==", e).limit(2));
    if (q.size === 1) return q.docs[0];
  }
  return null;
}

exports.stripeWebhook = onRequest({secrets: [STRIPE_WEBHOOK_SECRET], memory: "256MiB", timeoutSeconds: 30}, async (req, res) => {
  if (req.method !== "POST") { res.status(405).send("Method not allowed"); return; }
  if (!verifyStripeSignature(req.rawBody, req.get("stripe-signature"), STRIPE_WEBHOOK_SECRET.value())) {
    res.status(400).send("Bad signature"); return;
  }
  let event;
  try { event = JSON.parse(req.rawBody.toString("utf8")); } catch (e) { res.status(400).send("Bad payload"); return; }
  const type = event && event.type;
  if (type !== "checkout.session.completed" && type !== "checkout.session.async_payment_succeeded") {
    res.status(200).send("ignored"); return;
  }
  const s = (event.data && event.data.object) || {};
  if (s.payment_status !== "paid") { res.status(200).send("not paid yet"); return; }
  const amountOk = s.currency === "eur" && (s.amount_total || 0) >= EDB_PLUS_MIN_CENTS;
  const payRef = db.collection("payments").doc(String(s.id));
  try {
    await db.runTransaction(async (tx) => {
      const already = await tx.get(payRef);
      if (already.exists) return; // événement renvoyé par Stripe : déjà traité
      const trader = amountOk ? await findTraderForSession(tx, s) : null;
      const now = new Date();
      let until = null;
      if (trader) {
        const d = trader.data() || {};
        const cur = d.edbPlus && d.edbPlusUntil ? new Date(d.edbPlusUntil) : null;
        const base = cur && !isNaN(cur.getTime()) && cur > now ? cur : now; // renouvellement : prolonge
        until = new Date(base.getTime());
        until.setFullYear(until.getFullYear() + 1);
        tx.update(trader.ref, {
          edbPlus: true,
          edbPlusRequested: false,
          edbPlusUntil: until.toISOString(),
          // Même date au format Timestamp : c'est elle que lisent les règles
          // Firestore (isPlus) pour couper l'accès au contenu EDB+ à l'échéance.
          edbPlusUntilTs: Timestamp.fromDate(until),
          edbPlusPaidAt: now.toISOString(),
          edbPlusSource: "stripe",
          updatedAt: now.toISOString(),
        });
      }
      tx.set(payRef, {
        provider: "stripe",
        sessionId: String(s.id),
        traderId: trader ? trader.id : null,
        clientReferenceId: s.client_reference_id || null,
        email: (s.customer_details && s.customer_details.email) || s.customer_email || null,
        amount: (s.amount_total || 0) / 100,
        currency: s.currency || null,
        matched: !!trader,
        amountOk,
        until: until ? until.toISOString() : null,
        livemode: !!event.livemode,
        createdAt: now.toISOString(),
      });
    });
  } catch (e) {
    console.error("stripeWebhook failed", e);
    res.status(500).send("error"); return; // Stripe réessaiera
  }
  res.status(200).send("ok");
});

/* ============================= ACTUALITÉS (onglet News) =============================
 * Lit les flux RSS côté serveur toutes les 15 minutes et écrit le résultat
 * dans UN document Firestore : marketData/news ({intl:[…], fr:[…]}). Avant,
 * chaque navigateur passait par le service gratuit rss2json, qui finit par
 * refuser les requêtes (quota, blocage) : l'onglet affichait « indisponible ».
 * Doit rester alignée avec NEWS_FEEDS / NEWS_FEEDS_FR dans index.html.
 * Lecture autorisée par la règle existante match /marketData/{docId}. */
const NEWS_FEEDS_SRV = {
  intl: [
    {name: "CoinDesk", url: "https://www.coindesk.com/arc/outboundfeeds/rss/", tag: "Crypto"},
    {name: "Cointelegraph", url: "https://cointelegraph.com/rss", tag: "Crypto"},
    {name: "Yahoo Finance", url: "https://finance.yahoo.com/news/rssindex", tag: "Marchés"},
    {name: "BBC World", url: "https://feeds.bbci.co.uk/news/world/rss.xml", tag: "Géopolitique"},
    {name: "BBC Business", url: "https://feeds.bbci.co.uk/news/business/rss.xml", tag: "Économie"},
  ],
  fr: [
    {name: "Cryptoast", url: "https://cryptoast.fr/feed/", tag: "Crypto"},
    {name: "Cointribune", url: "https://www.cointribune.com/feed/", tag: "Crypto"},
    {name: "Coin Academy", url: "https://coinacademy.fr/feed/", tag: "Marchés"},
    {name: "Le Monde International", url: "https://www.lemonde.fr/international/rss_full.xml", tag: "Géopolitique"},
    {name: "Le Monde Économie", url: "https://www.lemonde.fr/economie/rss_full.xml", tag: "Économie"},
  ],
};
function rssDecode(s) {
  return String(s || "")
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/&#x([0-9a-f]+);/gi, (m, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (m, d) => String.fromCodePoint(+d))
    .replace(/&quot;/g, '"').replace(/&apos;|&#39;/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&nbsp;/g, " ").replace(/&rsquo;/g, "’").replace(/&lsquo;/g, "‘").replace(/&amp;/g, "&")
    .trim();
}
function rssTag(block, name) {
  const m = block.match(new RegExp("<" + name + "(?:\\s[^>]*)?>([\\s\\S]*?)</" + name + ">", "i"));
  return m ? rssDecode(m[1]) : "";
}
function rssAttr(block, tag, attr) {
  const re = new RegExp("<" + tag + "\\b[^>]*\\b" + attr + "=[\"']([^\"']+)[\"']", "i");
  const m = block.match(re);
  return m ? rssDecode(m[1]) : "";
}
function rssExcerpt(html) {
  const t = rssDecode(String(html).replace(/<[^>]*>/g, " ")).replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  return t.length > 220 ? t.slice(0, 217).replace(/\s+\S*$/, "") + "…" : t;
}
function rssParse(xml, feed) {
  const blocks = xml.match(/<item[\s>][\s\S]*?<\/item>/gi) || xml.match(/<entry[\s>][\s\S]*?<\/entry>/gi) || [];
  return blocks.slice(0, 25).map(b => {
    const title = rssExcerpt(rssTag(b, "title"));
    let link = rssTag(b, "link");
    if (!/^https?:\/\//i.test(link)) link = rssAttr(b, "link", "href");
    const when = rssTag(b, "pubDate") || rssTag(b, "dc:date") || rssTag(b, "published") || rssTag(b, "updated");
    const d = new Date(when);
    const desc = rssTag(b, "description") || rssTag(b, "content:encoded") || rssTag(b, "summary") || rssTag(b, "content");
    const imgInDesc = (desc.match(/<img[^>]+src=["']([^"']+)["']/i) || [])[1] || "";
    const img = [rssAttr(b, "media:content", "url"), rssAttr(b, "media:thumbnail", "url"), rssAttr(b, "enclosure", "url"), imgInDesc]
      .find(u => /^https:\/\//i.test(u) && !/\.(mp3|mp4|m4a)(\?|$)/i.test(u)) || "";
    return {title, link, pubDate: isNaN(d.getTime()) ? "" : d.toISOString(), source: feed.name, tag: feed.tag, img, excerpt: rssExcerpt(desc)};
  }).filter(it => it.title && /^https?:\/\//i.test(it.link));
}
async function rssFetch(feed, log) {
  try {
    const res = await fetch(feed.url, {
      headers: {...BROWSER_HEADERS, Accept: "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5"},
      redirect: "follow", signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) { log.push(feed.name + " " + res.status); return []; }
    const items = rssParse(await res.text(), feed);
    if (!items.length) log.push(feed.name + " vide");
    return items;
  } catch (e) { log.push(feed.name + " erreur " + (e && e.name)); return []; }
}
exports.newsRefresh = onSchedule(
  {schedule: "every 15 minutes", timeoutSeconds: 120, memory: "256MiB"},
  async () => {
    const ref = db.collection("marketData").doc("news");
    const log = [];
    const out = {};
    for (const lang of ["intl", "fr"]) {
      const lists = await Promise.all(NEWS_FEEDS_SRV[lang].map(f => rssFetch(f, log)));
      out[lang] = [].concat(...lists)
        .sort((a, b) => (Date.parse(b.pubDate) || 0) - (Date.parse(a.pubDate) || 0))
        .slice(0, 60);
    }
    console.log("newsRefresh: intl " + out.intl.length + ", fr " + out.fr.length + (log.length ? " | " + log.join(" ; ") : ""));
    const patch = {updatedAt: FieldValue.serverTimestamp()};
    if (out.intl.length) patch.intl = out.intl; // une langue sans résultat garde ses articles précédents
    if (out.fr.length) patch.fr = out.fr;
    if (!out.intl.length && !out.fr.length) return;
    await ref.set(patch, {merge: true});
  }
);

// Footprint exact (v3500) : fpRecorder + fpBackfill, voir functions/fpRecorder.js.
Object.assign(exports, require("./fpRecorder")({ASSETS, onSchedule, db}));
