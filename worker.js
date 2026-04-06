// ============================================================
// TradeBot AI — Cloudflare Worker v6.0
// Changes vs v5:
//   - Batch KV saves (12 writes → 2 per cycle)
//   - Claude AI as decision engine (replaces pure math score)
//   - $100 default capital per pair
//   - 1h/4h kline caching (reduces Binance calls ~60%)
//   - Momentum runs every 2 cycles
// ============================================================

const CRYPTOS = [
  { sym: 'BTCUSDT', name: 'Bitcoin',  short: 'BTC/USDT' },
  { sym: 'ETHUSDT', name: 'Ethereum', short: 'ETH/USDT' },
  { sym: 'BNBUSDT', name: 'BNB',      short: 'BNB/USDT' },
  { sym: 'SOLUSDT', name: 'Solana',   short: 'SOL/USDT' },
  { sym: 'XRPUSDT', name: 'XRP',      short: 'XRP/USDT' },
  { sym: 'ADAUSDT', name: 'Cardano',  short: 'ADA/USDT' },
];

const BINANCE_MIRRORS = [
  'https://api.binance.com',
  'https://api1.binance.com',
  'https://api2.binance.com',
  'https://api3.binance.com',
];

const COINGECKO_IDS = {
  BTCUSDT: 'bitcoin', ETHUSDT: 'ethereum', BNBUSDT: 'binancecoin',
  SOLUSDT: 'solana',  XRPUSDT: 'ripple',   ADAUSDT: 'cardano'
};

// ── Binance fetch con fallback entre mirrors ─────────────────
async function fetchBinance(path) {
  for (const base of BINANCE_MIRRORS) {
    try {
      const r = await fetch(base + path, {
        cf: { cacheTtl: 0, scrapeShield: false },
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' },
        signal: AbortSignal.timeout(5000)
      });
      if (!r.ok) continue;
      const d = await r.json();
      if (d.code && d.code < 0) continue;
      return d;
    } catch (e) { /* siguiente mirror */ }
  }
  throw new Error('Binance no disponible');
}

async function fetchPrice(sym) {
  try {
    const d = await fetchBinance(`/api/v3/ticker/price?symbol=${sym}`);
    const p = parseFloat(d.price);
    if (p > 0) return p;
  } catch (e) {}
  try {
    const id = COINGECKO_IDS[sym];
    const r = await fetch(`https://api.coingecko.com/api/v3/simple/price?ids=${id}&vs_currencies=usd`);
    const d = await r.json();
    const p = d[id]?.usd;
    if (p > 0) return p;
  } catch (e) {}
  return null;
}

// ── Klines con caché KV (1h cached 60min, 4h cached 4h) ─────
async function fetchKlinesCached(env, sym, interval, limit) {
  const ttlSec = interval === '4h' ? 14400 : interval === '1h' ? 3600 : 0;
  if (ttlSec > 0) {
    try {
      const key = `kl_${sym}_${interval}`;
      const cached = await env['bot kv'].get(key, 'json');
      if (cached && (Date.now() - cached.ts) < ttlSec * 1000) return cached.data;
      const data = await fetchBinance(`/api/v3/klines?symbol=${sym}&interval=${interval}&limit=${limit}`);
      await env['bot kv'].put(key, JSON.stringify({ ts: Date.now(), data }), { expirationTtl: ttlSec });
      return data;
    } catch (e) {}
  }
  return fetchBinance(`/api/v3/klines?symbol=${sym}&interval=${interval}&limit=${limit}`);
}

// ── KV Batch — todos los estados en 1 key, todos los precios en 1 key ──
async function getAllStates(env) {
  try {
    const raw = await env['bot kv'].get('states_all');
    return raw ? JSON.parse(raw) : {};
  } catch (e) { return {}; }
}

async function saveAllStates(env, states) {
  await env['bot kv'].put('states_all', JSON.stringify(states));
}

async function getAllPrices(env) {
  try {
    const raw = await env['bot kv'].get('prices_all');
    return raw ? JSON.parse(raw) : {};
  } catch (e) { return {}; }
}

async function saveAllPrices(env, prices) {
  await env['bot kv'].put('prices_all', JSON.stringify(prices));
}

function defaultState() {
  return {
    fase: 'BUSCANDO', cash: 100, qty: 0, entryPrice: 0, entryTime: null, startingCapital: 100,
    faseShort: 'BUSCANDO', cashShort: 100, shortQty: 0, shortEntryPrice: 0, shortEntryTime: null, startingCapitalShort: 100,
    trades: [], priceHistory: [], botRunCount: 0, lastUpdate: null,
    config: { stopLoss: 6, takeProfit: 8, minScore: 55, tgEnabled: true }
  };
}

function parseState(s) {
  const d = defaultState();
  if (!s) return d;
  return {
    fase: s.fase ?? d.fase,
    cash: parseFloat(s.cash) || d.cash,
    qty: parseFloat(s.qty) || 0,
    entryPrice: parseFloat(s.entryPrice) || 0,
    entryTime: s.entryTime || null,
    startingCapital: parseFloat(s.startingCapital) || d.startingCapital,
    faseShort: s.faseShort ?? d.faseShort,
    cashShort: parseFloat(s.cashShort) || d.cashShort,
    shortQty: parseFloat(s.shortQty) || 0,
    shortEntryPrice: parseFloat(s.shortEntryPrice) || 0,
    shortEntryTime: s.shortEntryTime || null,
    startingCapitalShort: parseFloat(s.startingCapitalShort) || d.startingCapitalShort,
    trades: Array.isArray(s.trades) ? s.trades : [],
    priceHistory: Array.isArray(s.priceHistory) ? s.priceHistory : [],
    botRunCount: parseInt(s.botRunCount) || 0,
    lastUpdate: s.lastUpdate || null,
    config: { ...d.config, ...(s.config || {}) }
  };
}

// ════════════════════════════════════════════════════════════
// INDICADORES TÉCNICOS
// ════════════════════════════════════════════════════════════

function calcRSI(closes, period = 14) {
  if (closes.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    if (d > 0) gains += d; else losses -= d;
  }
  if (losses === 0) return 100;
  return 100 - (100 / (1 + gains / losses));
}

function calcEMA(closes, period) {
  const k = 2 / (period + 1);
  let ema = closes[0];
  for (let i = 1; i < closes.length; i++) ema = closes[i] * k + ema * (1 - k);
  return ema;
}

function calcMACD(closes) {
  return calcEMA(closes, 12) - calcEMA(closes, 26);
}

function calcBollingerBands(closes, period = 20) {
  const slice = closes.slice(-period);
  const mean = slice.reduce((a, b) => a + b, 0) / period;
  const std = Math.sqrt(slice.reduce((a, b) => a + (b - mean) ** 2, 0) / period);
  return { upper: mean + 2 * std, middle: mean, lower: mean - 2 * std };
}

function calcATR(highs, lows, closes, period = 14) {
  const trs = [];
  for (let i = 1; i < closes.length; i++) {
    trs.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i-1]), Math.abs(lows[i] - closes[i-1])));
  }
  return trs.slice(-period).reduce((a, b) => a + b, 0) / period;
}

// Score matemático 0-100 (pre-filtro para Claude)
function calcScore(data15m, data1h, data4h, direction = 'LONG') {
  const { closes: c15, highs: h15, lows: l15, volumes: v15 } = data15m;
  const { closes: c1h } = data1h;
  const { closes: c4h } = data4h;
  const price = c15[c15.length - 1];

  const rsi15 = calcRSI(c15), rsi1h = calcRSI(c1h), rsi4h = calcRSI(c4h);
  const rsiAvg = (rsi15 + rsi1h + rsi4h) / 3;

  let rsiScore;
  if (direction === 'LONG') {
    if (rsi15 < 30 && rsiAvg < 45) rsiScore = 25;
    else if (rsi15 < 45 && rsiAvg < 55) rsiScore = 20;
    else if (rsi15 < 55 && rsiAvg < 65) rsiScore = 14;
    else if (rsi15 > 70 || rsiAvg > 72) rsiScore = 0;
    else rsiScore = 8;
  } else {
    if (rsi15 > 75 && rsiAvg > 68) rsiScore = 25;
    else if (rsi15 > 68 && rsiAvg > 62) rsiScore = 20;
    else if (rsi15 > 60 && rsiAvg > 58) rsiScore = 14;
    else if (rsi15 < 45 || rsiAvg < 40) rsiScore = 0;
    else rsiScore = 8;
  }

  const ema20_15 = calcEMA(c15, 20), ema50_15 = calcEMA(c15, 50);
  const ema20_1h = calcEMA(c1h, 20), ema20_4h = calcEMA(c4h, 20);
  const bull15 = price > ema20_15, bull1h = c1h[c1h.length-1] > ema20_1h, bull4h = c4h[c4h.length-1] > ema20_4h;
  const bullCount = [bull15, bull1h, bull4h].filter(Boolean).length;
  let trendScore;
  if (direction === 'LONG') {
    trendScore = bullCount === 3 ? (ema20_15 > ema50_15 ? 20 : 16) : bullCount === 2 ? 10 : bullCount === 1 ? 4 : 0;
  } else {
    const bearCount = 3 - bullCount;
    trendScore = bearCount === 3 ? (ema20_15 < ema50_15 ? 20 : 16) : bearCount === 2 ? 10 : bearCount === 1 ? 4 : 0;
  }

  const macd15 = calcMACD(c15), macd1h = calcMACD(c1h);
  let macdScore;
  if (direction === 'LONG') {
    if (macd1h > 0 && macd15 > 0) macdScore = 15;
    else if (macd15 > 0) macdScore = 6;
    else macdScore = 0;
  } else {
    if (macd1h < 0 && macd15 < 0) macdScore = 15;
    else if (macd15 < 0) macdScore = 6;
    else macdScore = 0;
  }

  const bb = calcBollingerBands(c15);
  const bbPos = (price - bb.lower) / (bb.upper - bb.lower);
  let bbScore;
  if (direction === 'LONG') {
    bbScore = bbPos < 0.2 ? 15 : bbPos < 0.4 ? 10 : bbPos < 0.55 ? 6 : bbPos > 0.85 ? 0 : 3;
  } else {
    bbScore = bbPos > 0.85 ? 15 : bbPos > 0.7 ? 10 : bbPos > 0.55 ? 6 : bbPos < 0.2 ? 0 : 3;
  }

  const volAvg = v15.slice(-20).reduce((a, b) => a + b, 0) / 20;
  const volRatio = v15[v15.length-1] / (volAvg || 1);
  const volScore = volRatio > 2 ? 15 : volRatio > 1.5 ? 12 : volRatio > 1.2 ? 8 : volRatio > 0.8 ? 5 : 0;

  const atr = calcATR(h15, l15, c15);
  const atrPct = (atr / price) * 100;
  const atrScore = (atrPct >= 0.3 && atrPct <= 1.2) ? 10 : (atrPct >= 0.15 && atrPct <= 2) ? 6 : atrPct < 0.15 ? 2 : 0;

  return {
    total: rsiScore + trendScore + macdScore + bbScore + volScore + atrScore,
    rsi15: rsi15.toFixed(1), rsi1h: rsi1h.toFixed(1), rsi4h: rsi4h.toFixed(1),
    macd15: macd15.toFixed(4), macd1h: macd1h.toFixed(4),
    bbPos: bbPos.toFixed(2), volRatio: volRatio.toFixed(2), atrPct: atrPct.toFixed(3),
    bb: { upper: bb.upper.toFixed(4), lower: bb.lower.toFixed(4) }
  };
}

// ════════════════════════════════════════════════════════════
// CLAUDE AI — Motor de decisiones
// Usa Haiku (más barato) con prompt compacto
// Solo se llama cuando hay señal matemática relevante
// ════════════════════════════════════════════════════════════

async function claudeDecide(env, crypto, price, change24h, scoreLong, scoreShort, state) {
  if (!env.ANTHROPIC_API_KEY) return { action: 'HOLD', confidence: 0, reason: 'No API key' };

  const posLong = state.fase === 'EN_POSICION'
    ? `ABIERTO en $${state.entryPrice.toFixed(4)} (P&L: ${((price - state.entryPrice) / state.entryPrice * 100).toFixed(2)}%)`
    : 'sin posición';
  const posShort = state.faseShort === 'EN_SHORT'
    ? `ABIERTO en $${state.shortEntryPrice.toFixed(4)} (P&L: ${((state.shortEntryPrice - price) / state.shortEntryPrice * 100).toFixed(2)}%)`
    : 'sin posición';

  const recentTrades = (state.trades || [])
    .filter(t => t.estado !== 'ABIERTA')
    .slice(0, 3)
    .map(t => `${t.tipo} ${t.estado} ${t.pnlPct ? t.pnlPct + '%' : ''}`)
    .join(', ') || 'sin historial';

  const prompt = `Eres un bot de trading crypto (paper trading, capital ficticio $100/par).
Par: ${crypto.short} | Precio: $${price.toFixed(4)} | 24h: ${change24h.toFixed(2)}%

Indicadores técnicos:
- RSI 15m/1h/4h: ${scoreLong.rsi15}/${scoreLong.rsi1h}/${scoreLong.rsi4h}
- MACD 15m: ${scoreLong.macd15} | 1h: ${scoreLong.macd1h}
- Bollinger position: ${(parseFloat(scoreLong.bbPos)*100).toFixed(0)}% (0=sobrevendido, 100=sobrecomprado)
- Volumen vs promedio: ${scoreLong.volRatio}x
- ATR%: ${scoreLong.atrPct}%
- Score matemático LONG: ${scoreLong.total}/100
- Score matemático SHORT: ${scoreShort.total}/100

Estado actual:
- LONG: ${posLong}
- SHORT: ${posShort}
- Últimos trades: ${recentTrades}

Acciones posibles:
- BUY_LONG: abrir posición alcista (solo si LONG = sin posición)
- SELL_LONG: cerrar posición alcista (solo si LONG = ABIERTO)
- BUY_SHORT: abrir posición bajista (solo si SHORT = sin posición)
- SELL_SHORT: cerrar posición bajista (solo si SHORT = ABIERTO)
- HOLD: no hacer nada

Responde ÚNICAMENTE con JSON válido, sin texto adicional:
{"action":"BUY_LONG|SELL_LONG|BUY_SHORT|SELL_SHORT|HOLD","confidence":0-100,"reason":"máximo 15 palabras"}`;

  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 120,
        messages: [{ role: 'user', content: prompt }]
      }),
      signal: AbortSignal.timeout(8000)
    });
    const data = await r.json();
    const text = data.content?.[0]?.text?.trim() || '{}';
    // Extraer JSON aunque venga con texto extra
    const match = text.match(/\{[^}]+\}/);
    if (!match) return { action: 'HOLD', confidence: 0, reason: 'Parse error' };
    return JSON.parse(match[0]);
  } catch (e) {
    console.error('Claude error:', e.message);
    return { action: 'HOLD', confidence: 0, reason: 'Error: ' + e.message };
  }
}

// ════════════════════════════════════════════════════════════
// FETCH DATOS DE MERCADO
// ════════════════════════════════════════════════════════════

async function fetchMarketData(env, sym) {
  const [k15, k1h, k4h, ticker24] = await Promise.all([
    fetchBinance(`/api/v3/klines?symbol=${sym}&interval=15m&limit=100`),
    fetchKlinesCached(env, sym, '1h', 100),
    fetchKlinesCached(env, sym, '4h', 60),
    fetchBinance(`/api/v3/ticker/24hr?symbol=${sym}`),
  ]);

  const parse = klines => ({
    closes:  klines.map(k => parseFloat(k[4])),
    highs:   klines.map(k => parseFloat(k[2])),
    lows:    klines.map(k => parseFloat(k[3])),
    volumes: klines.map(k => parseFloat(k[5])),
  });

  const data15m = parse(k15);
  return {
    data15m,
    data1h: parse(k1h),
    data4h: parse(k4h),
    price: data15m.closes[data15m.closes.length - 1],
    volume24h: parseFloat(ticker24.quoteVolume),
    change24h: parseFloat(ticker24.priceChangePercent),
  };
}

// ════════════════════════════════════════════════════════════
// LÓGICA DEL BOT — Claude toma decisiones
// ════════════════════════════════════════════════════════════

async function processCrypto(env, crypto, allStates, prices) {
  const state = parseState(allStates[crypto.sym]);
  state.botRunCount++;

  let market;
  try {
    market = await fetchMarketData(env, crypto.sym);
  } catch (e) {
    console.error(`${crypto.sym}: fetch falló — ${e.message}`);
    return state;
  }

  const { price, data15m, data1h, data4h, change24h } = market;
  prices[crypto.sym] = price;
  state.priceHistory = [...(state.priceHistory || []), price].slice(-200);
  state.lastUpdate = new Date().toISOString();

  const cfg = state.config;
  const SL = cfg.stopLoss / 100;
  const TP = cfg.takeProfit / 100;

  // ── Calcular scores matemáticos (pre-filtro) ─────────────
  const scoreLong  = calcScore(data15m, data1h, data4h, 'LONG');
  const scoreShort = calcScore(data15m, data1h, data4h, 'SHORT');

  // ── Gestión de posición LONG abierta ─────────────────────
  if (state.fase === 'EN_POSICION') {
    const pnlPct = (price - state.entryPrice) / state.entryPrice;

    if (pnlPct <= -SL) {
      // Stop Loss automático (sin consultar Claude)
      const pnl = state.qty * (price - state.entryPrice);
      state.cash += state.qty * price;
      const t = state.trades.find(t => t.tipo === 'LONG' && t.estado === 'ABIERTA');
      if (t) { t.precioSalida = price; t.fechaSalida = new Date().toISOString(); t.pnl = pnl; t.pnlPct = (pnlPct*100).toFixed(2); t.estado = 'PÉRDIDA'; t.razonSalida = `Stop Loss (${(pnlPct*100).toFixed(2)}%)`; }
      state.qty = 0; state.entryPrice = 0; state.entryTime = null; state.fase = 'BUSCANDO';
      await telegram(env, `🔴 LONG STOP LOSS — ${crypto.short}`, `Precio: $${price.toFixed(4)}\nP&L: ${(pnlPct*100).toFixed(2)}%`);

    } else if (pnlPct >= TP) {
      // Take Profit automático
      const pnl = state.qty * (price - state.entryPrice);
      state.cash += state.qty * price;
      const t = state.trades.find(t => t.tipo === 'LONG' && t.estado === 'ABIERTA');
      if (t) { t.precioSalida = price; t.fechaSalida = new Date().toISOString(); t.pnl = pnl; t.pnlPct = (pnlPct*100).toFixed(2); t.estado = 'GANANCIA'; t.razonSalida = `Take Profit (+${(pnlPct*100).toFixed(2)}%)`; }
      state.qty = 0; state.entryPrice = 0; state.entryTime = null; state.fase = 'BUSCANDO';
      await telegram(env, `🟢 LONG TAKE PROFIT — ${crypto.short}`, `Precio: $${price.toFixed(4)}\nP&L: +${(pnlPct*100).toFixed(2)}%`);

    } else {
      // Claude decide si cerrar anticipadamente
      const decision = await claudeDecide(env, crypto, price, change24h, scoreLong, scoreShort, state);
      console.log(`${crypto.sym} LONG abierto — Claude: ${decision.action} (${decision.confidence}%) — ${decision.reason}`);
      if (decision.action === 'SELL_LONG' && decision.confidence >= 60) {
        const pnl = state.qty * (price - state.entryPrice);
        state.cash += state.qty * price;
        const t = state.trades.find(t => t.tipo === 'LONG' && t.estado === 'ABIERTA');
        if (t) { t.precioSalida = price; t.fechaSalida = new Date().toISOString(); t.pnl = pnl; t.pnlPct = (pnlPct*100).toFixed(2); t.estado = pnl >= 0 ? 'GANANCIA' : 'PÉRDIDA'; t.razonSalida = `Claude cierre: ${decision.reason}`; }
        state.qty = 0; state.entryPrice = 0; state.entryTime = null; state.fase = 'BUSCANDO';
        await telegram(env, `🤖 LONG CERRADO (Claude) — ${crypto.short}`, `${decision.reason}\nPrecio: $${price.toFixed(4)}\nP&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)}`);
      }
    }

  } else if (state.fase === 'BUSCANDO') {
    // Solo preguntar a Claude si hay señal matemática mínima (ahorra tokens)
    if (scoreLong.total >= 40 || scoreShort.total >= 45) {
      const decision = await claudeDecide(env, crypto, price, change24h, scoreLong, scoreShort, state);
      console.log(`${crypto.sym} buscando — Claude: ${decision.action} (${decision.confidence}%) — ${decision.reason}`);

      if (decision.action === 'BUY_LONG' && decision.confidence >= 65) {
        const capitalFraction = Math.min(1, (scoreLong.total - 40) / 40);
        const capital = state.cash * (0.3 + capitalFraction * 0.5);
        const qty = capital / price;
        state.qty = qty; state.entryPrice = price; state.entryTime = new Date().toISOString();
        state.cash -= capital; state.fase = 'EN_POSICION';
        state.trades.unshift({
          id: Date.now(), tipo: 'LONG', par: crypto.short,
          precioEntrada: price, precioSalida: null, qty, capital,
          fechaEntrada: new Date().toISOString(), fechaSalida: null,
          pnl: null, pnlPct: null, estado: 'ABIERTA',
          razonEntrada: `🤖 Claude (conf:${decision.confidence}%): ${decision.reason} | Score:${scoreLong.total}`,
          razonSalida: null
        });
        await telegram(env, `🤖🟢 LONG ENTRADA — ${crypto.short}`, `Claude: ${decision.reason}\nScore: ${scoreLong.total}/100 | Capital: $${capital.toFixed(2)}\nRSI 15m: ${scoreLong.rsi15}`);
      }
    }
  }

  // ── Gestión de posición SHORT abierta ────────────────────
  if (state.faseShort === 'EN_SHORT') {
    const pnlPct = (state.shortEntryPrice - price) / state.shortEntryPrice;

    if (pnlPct <= -SL) {
      const pnl = state.shortQty * (state.shortEntryPrice - price);
      state.cashShort += state.shortQty * state.shortEntryPrice + pnl;
      const t = state.trades.find(t => t.tipo === 'SHORT' && t.estado === 'ABIERTA');
      if (t) { t.precioSalida = price; t.fechaSalida = new Date().toISOString(); t.pnl = pnl; t.pnlPct = (pnlPct*100).toFixed(2); t.estado = 'PÉRDIDA'; t.razonSalida = `Stop Loss Short (${(pnlPct*100).toFixed(2)}%)`; }
      state.shortQty = 0; state.shortEntryPrice = 0; state.shortEntryTime = null; state.faseShort = 'BUSCANDO';
      await telegram(env, `🔴 SHORT STOP LOSS — ${crypto.short}`, `Precio: $${price.toFixed(4)}\nP&L: ${(pnlPct*100).toFixed(2)}%`);

    } else if (pnlPct >= TP) {
      const pnl = state.shortQty * (state.shortEntryPrice - price);
      state.cashShort += state.shortQty * state.shortEntryPrice + pnl;
      const t = state.trades.find(t => t.tipo === 'SHORT' && t.estado === 'ABIERTA');
      if (t) { t.precioSalida = price; t.fechaSalida = new Date().toISOString(); t.pnl = pnl; t.pnlPct = (pnlPct*100).toFixed(2); t.estado = 'GANANCIA'; t.razonSalida = `Take Profit Short (+${(pnlPct*100).toFixed(2)}%)`; }
      state.shortQty = 0; state.shortEntryPrice = 0; state.shortEntryTime = null; state.faseShort = 'BUSCANDO';
      await telegram(env, `🟢 SHORT TAKE PROFIT — ${crypto.short}`, `Precio: $${price.toFixed(4)}\nP&L: +${(pnlPct*100).toFixed(2)}%`);

    } else {
      const decision = await claudeDecide(env, crypto, price, change24h, scoreLong, scoreShort, state);
      if (decision.action === 'SELL_SHORT' && decision.confidence >= 60) {
        const pnl = state.shortQty * (state.shortEntryPrice - price);
        state.cashShort += state.shortQty * state.shortEntryPrice + pnl;
        const t = state.trades.find(t => t.tipo === 'SHORT' && t.estado === 'ABIERTA');
        if (t) { t.precioSalida = price; t.fechaSalida = new Date().toISOString(); t.pnl = pnl; t.pnlPct = (pnlPct*100).toFixed(2); t.estado = pnl >= 0 ? 'GANANCIA' : 'PÉRDIDA'; t.razonSalida = `Claude cierre short: ${decision.reason}`; }
        state.shortQty = 0; state.shortEntryPrice = 0; state.shortEntryTime = null; state.faseShort = 'BUSCANDO';
        await telegram(env, `🤖 SHORT CERRADO (Claude) — ${crypto.short}`, `${decision.reason}\nP&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)}`);
      }
    }

  } else if (state.faseShort === 'BUSCANDO') {
    if (scoreShort.total >= 50) {
      const decision = await claudeDecide(env, crypto, price, change24h, scoreLong, scoreShort, state);
      if (decision.action === 'BUY_SHORT' && decision.confidence >= 70) {
        const capitalFraction = Math.min(1, (scoreShort.total - 50) / 35);
        const capital = state.cashShort * (0.25 + capitalFraction * 0.45);
        const qty = capital / price;
        state.shortQty = qty; state.shortEntryPrice = price; state.shortEntryTime = new Date().toISOString();
        state.cashShort -= capital; state.faseShort = 'EN_SHORT';
        state.trades.unshift({
          id: Date.now()+1, tipo: 'SHORT', par: crypto.short,
          precioEntrada: price, precioSalida: null, qty, capital,
          fechaEntrada: new Date().toISOString(), fechaSalida: null,
          pnl: null, pnlPct: null, estado: 'ABIERTA',
          razonEntrada: `🤖 Claude (conf:${decision.confidence}%): ${decision.reason} | Score:${scoreShort.total}`,
          razonSalida: null
        });
        await telegram(env, `🤖🔻 SHORT ENTRADA — ${crypto.short}`, `Claude: ${decision.reason}\nScore: ${scoreShort.total}/100 | Capital: $${capital.toFixed(2)}\nRSI 15m: ${scoreShort.rsi15}`);
      }
    }
  }

  state.trades = state.trades.slice(0, 100);
  return state;
}

// ════════════════════════════════════════════════════════════
// MOMENTUM — cada 2 ciclos
// ════════════════════════════════════════════════════════════

async function runMomentum(env) {
  try {
    const raw = await fetchBinance('/api/v3/ticker/24hr');
    const tickers = Array.isArray(raw) ? raw : Object.values(raw);
    const candidates = tickers
      .filter(t => t.symbol?.endsWith('USDT') &&
        !['USDC','BUSD','DAI','TUSD','FDUSD'].some(s => t.symbol.includes(s)) &&
        parseFloat(t.priceChangePercent) > 8 &&
        parseFloat(t.quoteVolume) > 5_000_000)
      .sort((a, b) => parseFloat(b.priceChangePercent) - parseFloat(a.priceChangePercent))
      .slice(0, 5);

    const raw2 = await env['bot kv'].get('momentum_v5');
    const mState = raw2 ? JSON.parse(raw2) : { positions: [], trades: [], cash: 50 };
    mState.positions = mState.positions || [];
    mState.trades = mState.trades || [];

    for (let i = mState.positions.length - 1; i >= 0; i--) {
      const pos = mState.positions[i];
      try {
        const data = await fetchBinance(`/api/v3/ticker/price?symbol=${pos.sym}`);
        const price = parseFloat(data.price);
        const pnlPct = (price - pos.entryPrice) / pos.entryPrice;
        if (price > pos.maxPrice) pos.maxPrice = price;
        const hours = (Date.now() - new Date(pos.entryTime).getTime()) / 3600000;
        if ((pos.maxPrice > 0 && (pos.maxPrice - price) / pos.maxPrice > 0.05) || pnlPct <= -0.08 || hours > 48) {
          const pnl = pos.qty * (price - pos.entryPrice);
          mState.cash += pos.qty * price;
          const t = mState.trades.find(t => t.id === pos.id);
          if (t) { t.sellPrice = price; t.sellTime = new Date().toISOString(); t.pnl = pnl; t.pnlPct = (pnlPct*100).toFixed(2); t.status = pnl >= 0 ? 'GANANCIA' : 'PÉRDIDA'; t.sellReason = `Trailing ${pnlPct>=0?'+':''}${(pnlPct*100).toFixed(2)}%`; }
          mState.positions.splice(i, 1);
        }
      } catch (e) {}
    }

    for (const c of candidates) {
      if (mState.positions.find(p => p.sym === c.symbol) || mState.cash < 10) continue;
      try {
        const klines = await fetchBinance(`/api/v3/klines?symbol=${c.symbol}&interval=15m&limit=50`);
        const closes = klines.map(k => parseFloat(k[4]));
        const volumes = klines.map(k => parseFloat(k[5]));
        const rsi = calcRSI(closes);
        const volAvg = volumes.slice(-20).reduce((a, b) => a + b, 0) / 20;
        const volSpike = volumes[volumes.length-1] / (volAvg || 1);
        if (rsi > 80 || volSpike < 1.5) continue;
        const price = closes[closes.length-1];
        const capital = Math.min(20, mState.cash * 0.2);
        const qty = capital / price;
        const posId = Date.now();
        mState.positions.push({ id: posId, sym: c.symbol, entryPrice: price, entryTime: new Date().toISOString(), qty, capital, maxPrice: price });
        mState.trades.unshift({ id: posId, sym: c.symbol, entryPrice: price, entryTime: new Date().toISOString(), qty, capital, sellPrice: null, sellTime: null, pnl: null, pnlPct: null, status: 'ABIERTA', buyReason: `Spike ${parseFloat(c.priceChangePercent).toFixed(1)}% RSI:${rsi.toFixed(1)} Vol:${volSpike.toFixed(1)}x` });
        mState.cash -= capital;
      } catch (e) {}
    }

    mState.trades = mState.trades.slice(0, 50);
    await env['bot kv'].put('momentum_v5', JSON.stringify(mState));
  } catch (e) {
    console.error('Momentum error:', e.message);
  }
}

// ════════════════════════════════════════════════════════════
// TELEGRAM
// ════════════════════════════════════════════════════════════

async function telegram(env, title, body) {
  try {
    if (!env.TELEGRAM_TOKEN || !env.TELEGRAM_CHAT_ID) return;
    await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text: `<b>${title}</b>\n${body}`, parse_mode: 'HTML' })
    });
  } catch (e) {}
}

// ════════════════════════════════════════════════════════════
// HEALTH CHECK
// ════════════════════════════════════════════════════════════

async function healthCheck(env) {
  const start = Date.now();
  const [cfResult, binanceResult, binanceDataResult, kvResult] = await Promise.all([
    Promise.resolve({ ok: true, ms: 0, detail: 'Worker respondiendo' }),
    (async () => {
      const t = Date.now();
      try {
        const r = await fetch('https://api.binance.com/api/v3/ping', { signal: AbortSignal.timeout(3000) });
        return { ok: r.ok, ms: Date.now()-t, detail: r.ok ? 'api.binance.com OK' : 'Error' };
      } catch (e) {
        return { ok: false, ms: Date.now()-t, detail: e.name === 'AbortError' ? 'Timeout' : e.message };
      }
    })(),
    (async () => {
      const t = Date.now();
      try {
        const cached = await env['bot kv'].get('prices_all');
        if (cached) {
          const prices = JSON.parse(cached);
          const btc = prices.BTCUSDT;
          return { ok: true, ms: Date.now()-t, detail: btc ? `BTC: $${btc.toLocaleString()} (KV cache)` : 'Sin precio' };
        }
        const price = await fetchPrice('BTCUSDT');
        return { ok: !!price, ms: Date.now()-t, detail: price ? `BTC: $${price.toLocaleString()}` : 'Sin precio' };
      } catch (e) {
        return { ok: false, ms: Date.now()-t, detail: e.message };
      }
    })(),
    (async () => {
      const t = Date.now();
      try {
        await env['bot kv'].get('health_check');
        return { ok: true, ms: Date.now()-t, detail: 'Lectura OK' };
      } catch (e) {
        return { ok: false, ms: Date.now()-t, detail: 'Error de acceso' };
      }
    })(),
  ]);

  return {
    cloudflare: cfResult, binance: binanceResult, binanceData: binanceDataResult, kv: kvResult,
    anthropic: {
      ok: !!(env.ANTHROPIC_API_KEY && env.ANTHROPIC_API_KEY.length > 10),
      ms: 0,
      detail: env.ANTHROPIC_API_KEY ? 'API key configurada ✓' : 'API key no encontrada'
    },
    telegram: {
      ok: !!(env.TELEGRAM_TOKEN && env.TELEGRAM_CHAT_ID),
      ms: 0,
      detail: (env.TELEGRAM_TOKEN && env.TELEGRAM_CHAT_ID) ? 'Configurado ✓' : 'Sin configurar'
    },
    totalMs: Date.now() - start,
    timestamp: new Date().toISOString()
  };
}

// ════════════════════════════════════════════════════════════
// HTTP HANDLER
// ════════════════════════════════════════════════════════════

export default {
  async fetch(request, env) {
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Content-Type': 'application/json'
    };
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });

    const url = new URL(request.url);
    let body = {};
    if (request.method === 'POST') {
      try { body = await request.json(); } catch (e) {}
    }

    if (url.pathname === '/health') {
      const h = await healthCheck(env);
      return new Response(JSON.stringify({ ok: true, ...h }), { headers: cors });
    }

    if (url.pathname === '/status-all') {
      // Solo 2 KV reads en vez de 13
      const [allStates, prices] = await Promise.all([
        getAllStates(env),
        getAllPrices(env)
      ]);
      const states = CRYPTOS.map(c => {
        const s = parseState(allStates[c.sym]);
        return { sym: c.sym, short: c.short, name: c.name, price: prices[c.sym] || null, ...s };
      });
      const mRaw = await env['bot kv'].get('momentum_v5');
      const momentum = mRaw ? JSON.parse(mRaw) : { positions: [], trades: [], cash: 50 };
      return new Response(JSON.stringify({ ok: true, states, momentum }), { headers: cors });
    }

    if (url.pathname === '/reset') {
      const { sym, startingCapital = 100 } = body;
      const allStates = await getAllStates(env);
      const targets = sym ? [sym] : CRYPTOS.map(c => c.sym);
      for (const s of targets) {
        const st = defaultState();
        st.startingCapital = startingCapital;
        st.startingCapitalShort = startingCapital;
        st.cash = startingCapital;
        st.cashShort = startingCapital;
        allStates[s] = st;
      }
      await saveAllStates(env, allStates);
      return new Response(JSON.stringify({ ok: true, msg: `Reset OK — $${startingCapital} por par` }), { headers: cors });
    }

    if (url.pathname === '/config') {
      const { sym, config } = body;
      const allStates = await getAllStates(env);
      const targets = sym ? [sym] : CRYPTOS.map(c => c.sym);
      for (const s of targets) {
        const st = parseState(allStates[s]);
        st.config = { ...st.config, ...config };
        allStates[s] = st;
      }
      await saveAllStates(env, allStates);
      return new Response(JSON.stringify({ ok: true }), { headers: cors });
    }

    if (url.pathname === '/manual-trade') {
      const { sym, action, capitalPct = 50 } = body;
      if (!sym || !action) return new Response(JSON.stringify({ ok: false, error: 'sym y action requeridos' }), { headers: cors });

      const [allStates, allPrices] = await Promise.all([getAllStates(env), getAllPrices(env)]);
      const s = parseState(allStates[sym]);
      let price = allPrices[sym];
      const freshPrice = await fetchPrice(sym);
      if (freshPrice) { price = freshPrice; allPrices[sym] = price; }
      if (!price || price <= 0) return new Response(JSON.stringify({ ok: false, error: 'Sin precio disponible' }), { headers: cors });

      const pct = Math.max(10, Math.min(100, parseFloat(capitalPct))) / 100;

      if (action === 'BUY_LONG') {
        if (s.fase === 'EN_POSICION') return new Response(JSON.stringify({ ok: false, error: 'Ya hay un LONG abierto' }), { headers: cors });
        const capital = s.cash * pct;
        if (capital < 0.5) return new Response(JSON.stringify({ ok: false, error: 'Sin capital' }), { headers: cors });
        s.qty = capital / price; s.entryPrice = price; s.entryTime = new Date().toISOString();
        s.cash -= capital; s.fase = 'EN_POSICION';
        s.trades.unshift({ id: Date.now(), tipo: 'LONG', par: sym.replace('USDT','/USDT'), precioEntrada: price, precioSalida: null, qty: s.qty, capital, fechaEntrada: new Date().toISOString(), fechaSalida: null, pnl: null, pnlPct: null, estado: 'ABIERTA', razonEntrada: `🖐 Manual (${(pct*100).toFixed(0)}%)`, razonSalida: null });
      } else if (action === 'SELL_LONG') {
        if (s.fase !== 'EN_POSICION') return new Response(JSON.stringify({ ok: false, error: 'No hay LONG abierto' }), { headers: cors });
        const pnl = s.qty * (price - s.entryPrice);
        const pnlPct = ((price - s.entryPrice) / s.entryPrice * 100).toFixed(2);
        s.cash += s.qty * price;
        const t = s.trades.find(t => t.tipo === 'LONG' && t.estado === 'ABIERTA');
        if (t) { t.precioSalida = price; t.fechaSalida = new Date().toISOString(); t.pnl = pnl; t.pnlPct = pnlPct; t.estado = pnl >= 0 ? 'GANANCIA' : 'PÉRDIDA'; t.razonSalida = '🖐 Venta manual'; }
        s.qty = 0; s.entryPrice = 0; s.entryTime = null; s.fase = 'BUSCANDO';
        allStates[sym] = s; allPrices[sym] = price;
        await Promise.all([saveAllStates(env, allStates), saveAllPrices(env, allPrices)]);
        await telegram(env, `🖐 LONG CERRADO — ${sym}`, `P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pnlPct}%)`);
        return new Response(JSON.stringify({ ok: true, msg: `LONG cerrado`, pnl: pnl.toFixed(2), pnlPct }), { headers: cors });
      } else if (action === 'BUY_SHORT') {
        if (s.faseShort === 'EN_SHORT') return new Response(JSON.stringify({ ok: false, error: 'Ya hay SHORT abierto' }), { headers: cors });
        const capital = s.cashShort * pct;
        s.shortQty = capital / price; s.shortEntryPrice = price; s.shortEntryTime = new Date().toISOString();
        s.cashShort -= capital; s.faseShort = 'EN_SHORT';
        s.trades.unshift({ id: Date.now()+1, tipo: 'SHORT', par: sym.replace('USDT','/USDT'), precioEntrada: price, precioSalida: null, qty: s.shortQty, capital, fechaEntrada: new Date().toISOString(), fechaSalida: null, pnl: null, pnlPct: null, estado: 'ABIERTA', razonEntrada: `🖐 Short manual (${(pct*100).toFixed(0)}%)`, razonSalida: null });
      } else if (action === 'SELL_SHORT') {
        if (s.faseShort !== 'EN_SHORT') return new Response(JSON.stringify({ ok: false, error: 'No hay SHORT abierto' }), { headers: cors });
        const pnl = s.shortQty * (s.shortEntryPrice - price);
        const pnlPct = ((s.shortEntryPrice - price) / s.shortEntryPrice * 100).toFixed(2);
        s.cashShort += s.shortQty * s.shortEntryPrice + pnl;
        const t = s.trades.find(t => t.tipo === 'SHORT' && t.estado === 'ABIERTA');
        if (t) { t.precioSalida = price; t.fechaSalida = new Date().toISOString(); t.pnl = pnl; t.pnlPct = pnlPct; t.estado = pnl >= 0 ? 'GANANCIA' : 'PÉRDIDA'; t.razonSalida = '🖐 Cierre short manual'; }
        s.shortQty = 0; s.shortEntryPrice = 0; s.shortEntryTime = null; s.faseShort = 'BUSCANDO';
        allStates[sym] = s; allPrices[sym] = price;
        await Promise.all([saveAllStates(env, allStates), saveAllPrices(env, allPrices)]);
        await telegram(env, `🖐 SHORT CERRADO — ${sym}`, `P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pnlPct}%)`);
        return new Response(JSON.stringify({ ok: true, msg: `SHORT cerrado`, pnl: pnl.toFixed(2), pnlPct }), { headers: cors });
      }

      allStates[sym] = s; allPrices[sym] = price;
      await Promise.all([saveAllStates(env, allStates), saveAllPrices(env, allPrices)]);
      await telegram(env, `🖐 ${action} — ${sym}`, `Precio: $${price.toLocaleString()}`);
      return new Response(JSON.stringify({ ok: true, msg: `${action} ejecutado`, price }), { headers: cors });
    }

    if (url.pathname === '/toggle-bot') {
      const current = await env['bot kv'].get('bot_enabled');
      const nowOn = current === 'false';
      await env['bot kv'].put('bot_enabled', nowOn ? 'true' : 'false');
      await telegram(env, nowOn ? '🟢 Bot ENCENDIDO' : '🔴 Bot APAGADO', nowOn ? 'Retomará operaciones en el próximo ciclo.' : 'No abrirá nuevas posiciones.');
      return new Response(JSON.stringify({ ok: true, enabled: nowOn }), { headers: cors });
    }

    if (url.pathname === '/bot-status') {
      const v = await env['bot kv'].get('bot_enabled');
      return new Response(JSON.stringify({ ok: true, enabled: v !== 'false' }), { headers: cors });
    }

    if (url.pathname === '/test-report') {
      const [allStates, prices] = await Promise.all([getAllStates(env), getAllPrices(env)]);
      const botOn = (await env['bot kv'].get('bot_enabled')) !== 'false';
      let totalCap = 0, totalStart = 0, wins = 0, losses = 0, openLong = 0, openShort = 0;
      const lines = [];
      for (const c of CRYPTOS) {
        const s = parseState(allStates[c.sym]);
        const price = prices[c.sym] || 0;
        const capL = s.cash + s.qty * price;
        const start = s.startingCapital || 100;
        const pnlL = capL - start;
        totalCap += capL + s.cashShort;
        totalStart += start + (s.startingCapitalShort || 100);
        const closed = (s.trades || []).filter(t => t.estado !== 'ABIERTA');
        wins += closed.filter(t => t.estado === 'GANANCIA').length;
        losses += closed.filter(t => t.estado === 'PÉRDIDA').length;
        if (s.fase === 'EN_POSICION') openLong++;
        if (s.faseShort === 'EN_SHORT') openShort++;
        lines.push(`${pnlL >= 0 ? '🟢' : '🔴'} ${c.short.replace('/USDT','')} — P&L: ${pnlL>=0?'+':''}$${pnlL.toFixed(2)} | ${s.fase==='EN_POSICION'?'▲LONG':s.faseShort==='EN_SHORT'?'▼SHORT':'IDLE'}`);
      }
      const pnl = totalCap - totalStart;
      const pct = totalStart > 0 ? (pnl / totalStart * 100).toFixed(2) : '0.00';
      const tot = wins + losses;
      const wr = tot > 0 ? (wins / tot * 100).toFixed(0) : '--';
      const msg = `🤖 <b>TradeBot AI — Reporte</b>\n━━━━━━━━━━━━━━\n● Bot: ${botOn?'🟢 ON':'🔴 OFF'}\n● Posiciones: ${openLong}L / ${openShort}S\n\n💼 <b>Portfolio ($100/par)</b>\nCapital: $${totalCap.toFixed(2)} / $${totalStart.toFixed(0)}\nP&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pct}%)\nWin rate: ${wr}% (${tot} trades)\n\n📊 <b>Por par</b>\n${lines.join('\n')}\n━━━━━━━━━━━━━━\n🕐 ${new Date().toLocaleString('es-MX',{timeZone:'America/Mexico_City'})}`;
      try {
        await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text: msg, parse_mode: 'HTML' })
        });
      } catch(e) {}
      return new Response(JSON.stringify({ ok: true, pnl: pnl.toFixed(2), winRate: wr }), { headers: cors });
    }

    if (url.pathname === '/telegram') {
      await telegram(env, body.title || 'TradeBot', body.message || 'Test');
      return new Response(JSON.stringify({ ok: true }), { headers: cors });
    }

    // Proxy Claude API (para uso futuro desde el frontend)
    try {
      const r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify(body)
      });
      return new Response(JSON.stringify(await r.json()), { headers: cors });
    } catch (e) {
      return new Response(JSON.stringify({ ok: false, error: e.message }), { headers: cors });
    }
  },

  // ── Cron: cada 15 minutos (era 10) ──────────────────────
  // En wrangler.toml: crons = ["*/15 * * * *"]
  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      const enabled = await env['bot kv'].get('bot_enabled');
      if (enabled === 'false') { console.log('🔴 Bot apagado'); return; }

      const cycleRaw = await env['bot kv'].get('cycle_count');
      const cycle = (parseInt(cycleRaw) || 0) + 1;
      await env['bot kv'].put('cycle_count', String(cycle));
      console.log(`🤖 Ciclo #${cycle}`);

      // Cargar todos los estados y precios de una vez (2 KV reads)
      const [allStates, prices] = await Promise.all([
        getAllStates(env),
        getAllPrices(env)
      ]);

      // Procesar cada cripto (Claude decide)
      const results = await Promise.allSettled(
        CRYPTOS.map(c => processCrypto(env, c, allStates, prices))
      );

      // Actualizar estados con resultados
      results.forEach((r, i) => {
        if (r.status === 'fulfilled') allStates[CRYPTOS[i].sym] = r.value;
      });

      // Guardar todo en 2 KV writes (en vez de 12)
      await Promise.all([
        saveAllStates(env, allStates),
        saveAllPrices(env, prices)
      ]);

      // Momentum solo cada 2 ciclos (30 min)
      if (cycle % 2 === 0) await runMomentum(env);

      console.log(`✅ Ciclo #${cycle} completado`);
    })());
  }
};
