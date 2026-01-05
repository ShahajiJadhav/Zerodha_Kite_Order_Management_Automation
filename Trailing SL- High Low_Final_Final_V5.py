# hft_bot_kite_local_complete_v2.py
"""
Local (verbose) HFT bot for Kite — COMPLETE version (force-update SL every cycle)

Key behaviors:
- MIS only; entries & SL actions only before 15:00 IST
- SL = previous *completed* 5m candle extreme (LOW for BUY, HIGH for SELL)
- Immediate SL placement *right after entry*
- Immediate breach check at entry (if crossed, exit at market)
- Trailing every 5 minutes at HH:(..06|11|16|21|26|31|36|41|46|51|56):30
- 5× leverage universe (scraped from Zerodha) — ONLY trade these symbols
- Orphan cleanup: cancel open SL/SL-M orders without an active MIS position
- Strict max-position enforcement with in-memory pending reservation
- **Force update**: On every scheduled cycle, SL is updated to the previous candle's extreme

Dependencies:
  pip install kiteconnect selenium webdriver-manager beautifulsoup4 pandas pytz requests

Env vars required:
  KITE_API_KEY, KITE_ACCESS_TOKEN, CHARTINK_COOKIE, CHARTINK_CSRF_TOKEN
"""

import os
import time
import random
import requests
import threading
import csv
from datetime import datetime, timedelta
from collections import defaultdict
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import pytz
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()
from kiteconnect import KiteConnect

# ==================== Config ==================== #
india_tz = pytz.timezone("Asia/Kolkata")
SIMULATION_MODE = False
DEBUG = True

MAX_POSITIONS = 10
COOLDOWN_MINUTES = 30
MAIN_SLEEP_MIN, MAIN_SLEEP_MAX = 10, 14

CUTOFF_ENTRY_H = 15
CUTOFF_ENTRY_M = 0
CUTOFF_EXIT_H  = 15
CUTOFF_EXIT_M  = 0

TRAIL_OFFSET_SECONDS = 90  # 1m30s after each 5m boundary

KITE_API_KEY       = os.getenv("KITE_API_KEY", "")
KITE_ACCESS_TOKEN  = os.getenv("KITE_ACCESS_TOKEN", "")
CHARTINK_COOKIE    = os.getenv("CHARTINK_COOKIE", "")
CHARTINK_CSRF      = os.getenv("CHARTINK_CSRF_TOKEN", "")
FIVEX_CACHE_CSV    = os.path.join(os.path.dirname(__file__), "5x_cache.csv")

# ==================== State ==================== #
broker_positions_today = {}  # {symbol: {"qty","direction","avg_price"}}
active_slm_orders = defaultdict(list)  # {symbol: [order dicts]}  # diagnostics only
recent_exits = {}  # {symbol: datetime}

_all_instruments = None
symbol_to_token = {}
symbol_to_tick = defaultdict(lambda: 0.05)
allowed_symbols = set()

# capacity guard
entry_lock = threading.Lock()
pending_entries = {}  # {symbol: datetime_added}
PENDING_TTL_SEC = 180

# ==================== Common helpers ==================== #
def log(msg: str):
    t = datetime.now(india_tz).strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

def now_ist() -> datetime:
    return datetime.now(india_tz)

def before_3pm() -> bool:
    n = now_ist()
    return (n.hour, n.minute) < (CUTOFF_ENTRY_H, CUTOFF_ENTRY_M)

def exits_allowed_now() -> bool:
    n = now_ist()
    return (n.hour, n.minute) < (CUTOFF_EXIT_H, CUTOFF_EXIT_M)

def is_trade_allowed(symbol: str) -> bool:
    last = recent_exits.get(symbol.upper())
    if not last:
        return True
    return (now_ist() - last) >= timedelta(minutes=COOLDOWN_MINUTES)

def round_to_tick(price: float, symbol: str) -> float:
    tick = float(symbol_to_tick.get(symbol.upper(), 0.05) or 0.05)
    steps = round(price / tick)
    return round(steps * tick, 2)

# ==================== Kite ==================== #
kite = KiteConnect(api_key=KITE_API_KEY)
if KITE_ACCESS_TOKEN:
    kite.set_access_token(KITE_ACCESS_TOKEN)

# ==================== Hardened HTTP + Kite retry wrapper ==================== #
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Single hardened session for direct requests (historical candles, scraping, etc.)
HARDENED_HTTP = requests.Session()
retry_cfg = Retry(
    total=5,
    backoff_factor=0.4,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)
HARDENED_HTTP.mount("https://", HTTPAdapter(max_retries=retry_cfg))
HARDENED_HTTP.headers.update({"Connection": "close"})  # avoid reusing half-dead sockets

# Serialize & retry Kite SDK calls; protects against SSLEOF/keep-alive issues
kite_http_lock = threading.Lock()

def _retry_jitter(attempt: int) -> float:
    base = min(0.5 * (2 ** attempt), 4.0)  # 0.5,1,2,4 capped
    return base + random.uniform(0.0, 0.25)

def call_kite(fn, *args, **kwargs):
    for attempt in range(5):
        try:
            with kite_http_lock:
                return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            transient = (
                "SSLEOFError" in msg
                or "UNEXPECTED_EOF_WHILE_READING" in msg
                or "Max retries exceeded" in msg
                or "Read timed out" in msg
                or "Connection aborted" in msg
                or "RemoteDisconnected" in msg
            )
            if transient and attempt < 4:
                sleep_s = _retry_jitter(attempt)
                log(f"[kite-retry] {fn.__name__} transient: {msg} → retry {attempt+1}/5 in {sleep_s:.2f}s")
                time.sleep(sleep_s)
                continue
            log(f"[kite-retry] {fn.__name__} failed: {msg}")
            raise


def get_ltp(symbol: str) -> float:
    try:
        q = kite.quote(f"NSE:{symbol}")
        return float(q[f"NSE:{symbol}"]["last_price"])
    except Exception as e:
        log(f"[get_ltp] {symbol} {e}")
        return 0.0


def capital_per_position() -> float:
    try:
        bal = kite.margins()["equity"]["available"]["live_balance"]
        # bal = 2000  # placeholder for dev/testing; replace with live_balance if desired
        return bal // MAX_POSITIONS if MAX_POSITIONS else 0
    except Exception as e:
        log(f"[capital_per_position] {e}")
        return 0

# ==================== Instruments ==================== #

def load_instruments_once():
    global _all_instruments
    if _all_instruments is None:
        log("[init] Loading NSE instruments…")
        try:
            _all_instruments = kite.instruments("NSE")
            for inst in _all_instruments:
                sym = inst["tradingsymbol"].upper()
                symbol_to_token[sym] = inst["instrument_token"]
                symbol_to_tick[sym]  = float(inst.get("tick_size", 0.05) or 0.05)
            log(f"[init] Loaded {len(_all_instruments)} instruments; ticks ready.")
        except Exception as e:
            log(f"[init] instruments load failed: {e}")
            _all_instruments = []


def ensure_instrument_token(symbol: str):
    sym = symbol.upper()
    tok = symbol_to_token.get(sym)
    if not tok and _all_instruments is None:
        load_instruments_once()
        tok = symbol_to_token.get(sym)
    if not tok:
        log(f"[ensure_instrument_token] Not found: {sym}")
    return tok

# ==================== Broker sync & reconciliation ==================== #

def fetch_active_positions():
    out = {}
    try:
        data = call_kite(kite.positions).get("day", []) or []
        for p in data:
            if p.get("product") != "MIS":
                continue
            qty = int(p.get("quantity", 0))
            if qty == 0:
                continue
            sym = p.get("tradingsymbol", "").upper()
            side = "BUY" if qty > 0 else "SELL"
            out[sym] = {
                "qty": abs(qty),
                "direction": side,
                "avg_price": float(p.get("average_price", 0.0)),
            }
    except Exception as e:
        log(f"[fetch_active_positions] {e}")
    return out


def _cleanup_pending():
    now = now_ist()
    to_del = []
    for sym, t0 in list(pending_entries.items()):
        if sym in broker_positions_today:
            to_del.append(sym)
        elif (now - t0).total_seconds() > PENDING_TTL_SEC:
            to_del.append(sym)
    for sym in to_del:
        pending_entries.pop(sym, None)
        if DEBUG:
            log(f"[pending] cleared: {sym}")


def capacity_left() -> int:
    _cleanup_pending()
    return max(0, MAX_POSITIONS - (len(broker_positions_today) + len(pending_entries)))


def sync_broker_positions_today():
    broker_positions_today.clear()
    broker_positions_today.update(fetch_active_positions())
    log(f"[sync] positions: {list(broker_positions_today.keys()) or 'none'}")
    _cleanup_pending()


def sync_active_orders_cache():
    # Diagnostics only; trail logic fetches fresh state per symbol.
    active_slm_orders.clear()
    try:
        for o in call_kite(kite.orders) or []:
            if o.get("product") != "MIS":
                continue
            if o.get("variety") != "regular":
                continue
            if o.get("status") not in ("TRIGGER PENDING", "OPEN"):
                continue
            if o.get("order_type") != "SL-M":
                continue
            sym = o.get("tradingsymbol", "").upper()
            active_slm_orders[sym].append(
                {
                    "order_id": o.get("order_id"),
                    "trigger_price": float(o.get("trigger_price", 0.0) or 0),
                    "status": o.get("status"),
                    "transaction_type": o.get("transaction_type"),
                }
            )
        if DEBUG:
            log("[sync] slms: {sym:len} -> " + str({k: len(v) for k, v in active_slm_orders.items()}))
    except Exception as e:
        log(f"[sync_active_orders_cache] {e}")


def reconcile_orphan_orders():
    """Cancel open SL/SL-M orders that do not have a corresponding active MIS position."""
    try:
        pos_syms = set(broker_positions_today.keys())
        for o in call_kite(kite.orders) or []:
            if o.get("product") != "MIS":
                continue
            if o.get("variety") != "regular":
                continue
            if o.get("status") not in ("TRIGGER PENDING", "OPEN"):
                continue
            if o.get("order_type") not in ("SL", "SL-M"):
                continue
            sym = o.get("tradingsymbol", "").upper()
            if sym not in pos_syms:
                try:
                    call_kite(kite.cancel_order, variety="regular", order_id=o["order_id"])
                    log(f"[reconcile] orphan order canceled {sym} oid={o.get('order_id')}")
                except Exception as ce:
                    log(f"[reconcile] cancel failed {sym}: {ce}")
    except Exception as e:
        log(f"[reconcile] error: {e}")

# ==================== Chartink (signals) ==================== #

def fetch_chartink_signals(scan_type: str, payload: dict):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-CSRF-Token": CHARTINK_CSRF,
        "Cookie": CHARTINK_COOKIE,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    try:
        r = requests.post(
            "https://chartink.com/screener/process",
            headers=headers,
            data=payload,
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
        syms = [d.get("nsecode", "").upper() for d in data.get("data", []) if d.get("nsecode")]
        return [{"symbol": s, "side": scan_type.upper()} for s in syms]
    except Exception as e:
        log(f"[chartink {scan_type}] {e}")
        return []


def gather_signals():
    # Keep your existing scan clauses here (trimmed for brevity)
    buy_payload  = {
        "scan_clause": '( {1339018} ( [0] 1 hour volume >= 300000 and [0] 5 minute close > 1 day ago close * 1.013 and( {cash} ( [0] 5 minute close > [-1] 5 minute max( 36 , [-1] 5 minute close ) * 1.004 or [-1] 5 minute close > [-2] 5 minute max( 36 , [-2] 5 minute close ) * 1.003 or( {cash} ( [0] 5 minute close < [-1] 5 minute max( 36 , [-1] 5 minute close ) * 1.025 and [0] 5 minute close > [-1] 5 minute max( 36 , [-1] 5 minute close ) * 0.98 and abs( [0] 10 minute close - [0] 10 minute open ) > [0] 10 minute open * 0.01 and [-1] 5 minute close > [-1] 5 minute open ) ) or( {cash} ( [0] 5 minute close > [-1] 5 minute max( 36 , [-1] 5 minute close ) * 1.0015 and [0] 5 minute open < [-1] 5 minute max( 36 , [-1] 5 minute close ) * 1.005 ) ) ) ) and latest close > 20 and latest close < 3000 and [0] 5 minute close > [0] 5 minute open and abs( [0] 5 minute close - [0] 5 minute open ) > [0] 5 minute open * 0.002 and [=1] 5 minute close < 1 day ago close * 1.07 and [0] 5 minute close > [0] 5 minute supertrend( 18 , 1.2 ) and( {cash} ( ( {cash} ( latest close > 25 and latest close < 80 and( {cash} ( [0] 10 minute volume > 70000 or( {cash} ( [0] 5 minute volume > 30000 and [-1] 5 minute volume > 20000 ) ) ) ) ) ) or( {cash} ( latest close > 80 and latest close < 150 and( {cash} ( [0] 10 minute volume > 60000 or( {cash} ( [0] 5 minute volume > 25000 and [-1] 5 minute volume > 20000 ) ) ) ) ) ) or( {cash} ( latest close > 150 and latest close < 500 and( {cash} ( [0] 10 minute volume > 45000 and( {cash} ( [0] 5 minute volume > 28000 and [-1] 5 minute volume > 14000 ) ) ) ) ) ) or( {cash} ( latest close > 500 and( {cash} ( [0] 10 minute volume > 35000 or( {cash} ( [-1] 5 minute volume > 10000 and [0] 5 minute volume > 25000 ) ) ) ) ) ) ) ) ) )'
    }
    sell_payload = {
        "scan_clause": '( {1339018} ( [0] 1 hour volume >= 300000 and( {cash} ( [0] 5 minute close < [-1] 5 minute min( 36 , [-1] 5 minute close ) * 1.008 or [-1] 5 minute close < [-2] 5 minute max( 36 , [-2] 5 minute close ) * 1.0015 or( {cash} ( [0] 5 minute close < [-1] 5 minute min( 36 , [-1] 5 minute close ) * 1.03 and [0] 5 minute close > [-1] 5 minute min( 36 , [-1] 5 minute close ) * 0.97 and abs( [0] 10 minute close - [0] 10 minute open ) > [0] 10 minute open * 0.08 and [-1] 5 minute close < [-1] 5 minute open ) ) or( {cash} ( [0] 5 minute close < [-1] 5 minute min( 36 , [-1] 5 minute close ) * 0.998 and [0] 5 minute open > [-1] 5 minute min( 36 , [-1] 5 minute close ) * 0.993 ) ) ) ) and latest close > 20 and latest close < 2000 and [0] 5 minute close < [0] 5 minute open and abs( [0] 5 minute close - [0] 5 minute open ) > [0] 5 minute open * 0.0025 and [=1] 5 minute close > 1 day ago close * 0.94 and [0] 5 minute close < 1 day ago close * 0.987 and [0] 5 minute close < [0] 5 minute supertrend( 18 , 1.2 ) and( {cash} ( ( {cash} ( latest close > 25 and latest close < 80 and( {cash} ( [0] 10 minute volume > 70000 or( {cash} ( [0] 5 minute volume > 30000 and [-1] 5 minute volume > 20000 ) ) ) ) ) ) or( {cash} ( latest close > 80 and latest close < 150 and( {cash} ( [0] 10 minute volume > 60000 or( {cash} ( [0] 5 minute volume > 25000 and [-1] 5 minute volume > 20000 ) ) ) ) ) ) or( {cash} ( latest close > 150 and latest close < 500 and( {cash} ( [0] 10 minute volume > 45000 and( {cash} ( [0] 5 minute volume > 28000 and [-1] 5 minute volume > 14000 ) ) ) ) ) ) or( {cash} ( latest close > 500 and( {cash} ( [0] 10 minute volume > 35000 or( {cash} ( [-1] 5 minute volume > 10000 and [0] 5 minute volume > 25000 ) ) ) ) ) ) ) ) ) )'
    }

    out = []
    if buy_payload:
        out += fetch_chartink_signals("BUY", buy_payload)
    if sell_payload:
        out += fetch_chartink_signals("SELL", sell_payload)

    # de-dup
    seen, uniq = set(), []
    for s in out:
        key = (s["symbol"], s["side"])
        if key not in seen:
            seen.add(key)
            uniq.append(s)

    # restrict to allowed symbols
    if allowed_symbols:
        uniq = [s for s in uniq if s["symbol"].upper() in allowed_symbols]
    else:
        log("[signals] allowed_symbols empty — blocking all signals.")
        uniq = []

    if DEBUG:
        log(f"[signals] {uniq}")
    return uniq

# ==================== 5× leverage universe ==================== #

def _save_5x_cache(symbols: set):
    try:
        with open(FIVEX_CACHE_CSV, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Symbol"])
            for s in sorted(symbols):
                w.writerow([s])
        log(f"[5x cache] saved {len(symbols)} symbols")
    except Exception as e:
        log(f"[5x cache] save failed: {e}")


def _load_5x_cache() -> set:
    if not os.path.exists(FIVEX_CACHE_CSV):
        return set()
    try:
        df = pd.read_csv(FIVEX_CACHE_CSV)
        s = set(df["Symbol"].str.upper().dropna().tolist())
        log(f"[5x cache] loaded {len(s)} symbols")
        return s
    except Exception as e:
        log(f"[5x cache] load failed: {e}")
        return set()


def get_5x_leverage_stocks(timeout=25) -> set:
    url = "https://zerodha.com/margin-calculator/Equity/"
    symbols = set()
    driver = None
    log("[5x] scraping Zerodha Equity margin page…")

    try:
        chrome_opts = Options()
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument("--disable-gpu")
        chrome_opts.add_argument("--window-size=1280,800")
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_opts)
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        time.sleep(5)
        page = driver.page_source
    except Exception as e:
        log(f"[5x] scrape failed: {e}")
        page = ""
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass

    if not page:
        return set()

    try:
        soup = BeautifulSoup(page, "html.parser")
        rows = soup.select("table tbody tr")
        for row in rows:
            tds = row.find_all("td")
            if len(tds) >= 3:
                sym = (tds[0].get_text() or "").strip().upper()
                mis_lev = (tds[2].get_text() or "").strip().lower()
                try:
                    lev = float(mis_lev.replace("x", "").strip())
                    if lev == 5.0 and sym:
                        symbols.add(sym)
                except Exception:
                    continue
        log(f"[5x] scraped {len(symbols)} symbols with 5x leverage")
    except Exception as e:
        log(f"[5x] parse failed: {e}")
        return set()

    return symbols


def load_allowed_symbols_5x():
    global allowed_symbols
    live = get_5x_leverage_stocks()
    if live:
        allowed_symbols = live
        _save_5x_cache(live)
        sample = sorted(list(allowed_symbols))[:20]
        log(f"[5x] LIVE set applied ({len(live)}). Sample: {sample}…")
        return
    cached = _load_5x_cache()
    if cached:
        allowed_symbols = cached
        log(f"[5x] Using CACHE set ({len(cached)})")
        return
    allowed_symbols = set()
    log("[5x] EMPTY — entries blocked until list is available")

# ==================== 5m candle helpers ==================== #

def _five_minute_bounds_for_fetch(now=None, n_candles=5):
    if now is None:
        now = now_ist()
    minute_block = (now.minute // 5) * 5
    current_window_start = now.replace(minute=minute_block, second=0, microsecond=0)
    to_ts = current_window_start
    from_ts = to_ts - timedelta(minutes=5 * n_candles)
    return from_ts, to_ts


def get_last_n_five_min_candles(symbol: str, n: int = 5):
    symbol = symbol.upper()
    token = ensure_instrument_token(symbol)
    if not token:
        log(f"[get_last_n_five_min_candles] No token for {symbol}")
        return []

    from_ts, to_ts = _five_minute_bounds_for_fetch(now_ist(), n)
    url = f"https://api.kite.trade/instruments/historical/{token}/5minute"
    params = {"from": from_ts.strftime("%Y-%m-%d %H:%M:%S"), "to": to_ts.strftime("%Y-%m-%d %H:%M:%S"), "oi": 0}
    headers = {"X-Kite-Version": "3", "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}"}

    for attempt in range(2):
        try:
            r = HARDENED_HTTP.get(url, headers=headers, params=params, timeout=5)
            r.raise_for_status()
            candles = r.json().get("data", {}).get("candles", []) or []
            candles = candles[-max(1, n):]
            out = []
            for row in candles:
                ts, o, h, l, c, v = row
                out.append({"ts": ts, "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(v)})
            if DEBUG:
                log(f"[candles] {symbol} last{n}: {out}")
            return out
        except Exception as e:
            log(f"[get_last_n_five_min_candles] {symbol} attempt {attempt+1} {e}")
            time.sleep(0.8)
    return []


def get_last_completed_5m_candle(symbol: str):
    """Return the *previous* fully closed 5m candle (off-by-one safe)."""
    c = get_last_n_five_min_candles(symbol, n=2)
    if not c:
        return None
    if len(c) == 1:
        return c[0]
    return c[-1]


def compute_sl_from_prev_candle(symbol: str, direction: str):
    last = get_last_completed_5m_candle(symbol)
    if not last:
        return None
    raw = last["low"] if direction.upper() == "BUY" else last["high"]
    trig = round_to_tick(raw, symbol)
    if DEBUG:
        log(f"[sl-prev] {symbol} dir={direction} using {last['ts']} -> {trig}")
    return trig

# ==================== Orders ==================== #

def place_slm_order(symbol, direction, qty, trigger_price):
    if not exits_allowed_now():
        log(f"[place_slm_order] Skipped after 15:00 for {symbol}")
        return None
    symbol = symbol.upper()
    trigger_price = round_to_tick(trigger_price, symbol)
    if SIMULATION_MODE:
        log(f"[SIM SLM] {symbol} {direction} qty={qty} trig={trigger_price}")
        return f"SIM_SLM_{symbol}"
    try:
        trans = "SELL" if direction.upper() == "BUY" else "BUY"
        oid = call_kite(
            kite.place_order,
            tradingsymbol=symbol,
            exchange="NSE",
            transaction_type=trans,
            quantity=qty,
            order_type="SL-M",
            trigger_price=trigger_price,
            price=0,
            product="MIS",
            variety="regular",
        )
        log(f"[place_slm_order] {symbol} oid={oid} trig={trigger_price}")
        return oid
    except Exception as e:
        log(f"[place_slm_order] {symbol} {e}")
        return None


def modify_sl_order(order_id, new_trigger, symbol):
    if not exits_allowed_now():
        log(f"[modify_sl_order] Skipped after 15:00 for {symbol}")
        return False
    new_trigger = round_to_tick(new_trigger, symbol)
    if SIMULATION_MODE:
        log(f"[SIM MODIFY] {symbol} -> {new_trigger}")
        return True
    try:
        call_kite(kite.modify_order, variety="regular", order_id=order_id, trigger_price=new_trigger)
        log(f"[modify_sl_order] {symbol} -> {new_trigger}")
        return True
    except Exception as e:
        log(f"[modify_sl_order] {symbol} {e}")
        return False


def cancel_all_open_orders_for_symbol(symbol):
    if not exits_allowed_now():
        log(f"[cancel_all_open_orders_for_symbol] Skipped after 15:00 for {symbol}")
        return
    try:
        for o in call_kite(kite.orders) or []:
            if o.get("product") != "MIS":
                continue
            if o.get("variety") != "regular":
                continue
            if o.get("status") not in ("TRIGGER PENDING", "OPEN"):
                continue
            if o.get("tradingsymbol", "").upper() != symbol.upper():
                continue
            try:
                call_kite(kite.cancel_order, variety="regular", order_id=o["order_id"])
                log(f"[cancel] {symbol} order {o.get('order_id')}")
            except Exception as e:
                log(f"[cancel] {symbol} {e}")
    except Exception as e:
        log(f"[cancel_all_open_orders_for_symbol] {e}")


def close_position_market(symbol: str, pos: dict):
    if not exits_allowed_now():
        log(f"[close_position_market] Skipped after 15:00 for {symbol}")
        return None
    symbol = symbol.upper()
    trans = "SELL" if pos["direction"].upper() == "BUY" else "BUY"
    qty = int(pos.get("qty", 0) or 0)
    if qty <= 0:
        return None
    if SIMULATION_MODE:
        log(f"[SIM EXIT] {symbol} {trans} x{qty} @ MARKET")
        cancel_all_open_orders_for_symbol(symbol)
        recent_exits[symbol] = now_ist()
        return "SIM_EXIT"
    try:
        oid = call_kite(
            kite.place_order,
            tradingsymbol=symbol,
            exchange="NSE",
            transaction_type=trans,
            quantity=qty,
            order_type="MARKET",
            product="MIS",
            variety="regular",
        )
        log(f"[exit] {symbol} market-exit placed: {oid}")
        cancel_all_open_orders_for_symbol(symbol)
        recent_exits[symbol] = now_ist()
        return oid
    except Exception as e:
        log(f"[close_position_market] {symbol} {e}")
        return None

# -------------------- NEW: robust SL-M fetch helpers -------------------- #

def side_to_exit_trans(position_direction: str) -> str:
    """For a BUY position you must have a SELL SL-M, and vice versa."""
    return "SELL" if position_direction.upper() == "BUY" else "BUY"


def fetch_symbol_slm_from_broker(symbol: str, expected_trans: str):
    """
    Pull a fresh view of the symbol's open/trigger-pending SL-M orders
    and return the most relevant one (matching transaction_type).
    """
    try:
        for o in call_kite(kite.orders) or []:
            if o.get("product") != "MIS":
                continue
            if o.get("variety") != "regular":
                continue
            if o.get("status") not in ("TRIGGER PENDING", "OPEN"):
                continue
            if o.get("order_type") != "SL-M":
                continue
            if o.get("tradingsymbol", "").upper() != symbol.upper():
                continue
            if o.get("transaction_type") != expected_trans:
                continue
            return {
                "order_id": o.get("order_id"),
                "trigger_price": float(o.get("trigger_price") or 0.0),
                "status": o.get("status"),
                "transaction_type": o.get("transaction_type"),
            }
    except Exception as e:
        log(f"[fetch_symbol_slm_from_broker] {symbol} {e}")
    return None

# ==================== Entries ==================== #

def place_market_entry(symbol, side):
    """Place a market entry, immediately place SL, then *immediately* check breach and exit if needed."""
    sym = symbol.upper()
    with entry_lock:
        if not before_3pm():
            log(f"[entry] Skipped after 15:00: {sym}")
            return None
        if not allowed_symbols or sym not in allowed_symbols:
            log(f"[entry] {sym} not in 5x universe — blocked.")
            return None
        if not is_trade_allowed(sym):
            log(f"[entry] {sym} on cooldown — blocked.")
            return None
        if sym in broker_positions_today or sym in pending_entries:
            log(f"[entry] {sym} already active/pending — skipped.")
            return None
        if capacity_left() <= 0:
            log(f"[entry] capacity reached ({MAX_POSITIONS}) — skipped.")
            return None

        ltp_before = get_ltp(sym)
        if ltp_before <= 0:
            log(f"[entry] {sym} LTP unavailable — skipped.")
            return None
        qty = int((capital_per_position() * 5) // ltp_before)
        if qty <= 0:
            log(f"[entry] {sym} qty calc <= 0 — skipped.")
            return None

        pending_entries[sym] = now_ist()  # reserve slot

    try:
        trans = "BUY" if side.upper() == "BUY" else "SELL"
        if SIMULATION_MODE:
            oid = "SIM_ORDER"
            log(f"[SIM ENTRY] {sym} {side} x{qty} @~{ltp_before}")
        else:
            oid = call_kite(
                kite.place_order,
                tradingsymbol=sym,
                exchange="NSE",
                transaction_type=trans,
                quantity=qty,
                order_type="MARKET",
                product="MIS",
                variety="regular",
            )
            log(f"[entry] {sym} {side} placed oid={oid}, qty={qty}, ltp~{ltp_before}")

        # 1) Immediate SL from previous closed candle
        trig = compute_sl_from_prev_candle(sym, side) or _fallback_sl(sym, side)
        if trig:
            place_slm_order(sym, side, qty, trig)

        # 2) Immediate breach check — if LTP already crossed extreme, exit now
        ltp_after = get_ltp(sym)
        if ltp_after > 0 and trig:
            breached = (side.upper() == "BUY" and ltp_after <= trig) or (side.upper() == "SELL" and ltp_after >= trig)
            if breached and exits_allowed_now():
                log(f"[entry] {sym} immediate BREACH (LTP={ltp_after}, trig={trig}) → exit")
                pseudo_pos = {"qty": qty, "direction": side}
                close_position_market(sym, pseudo_pos)
        return oid

    except Exception as e:
        log(f"[entry] {sym} {e}")
        with entry_lock:
            pending_entries.pop(sym, None)
        return None

# ==================== SL ensure & BREACH-or-UPDATE trailing ==================== #

def _fallback_sl(symbol, direction):
    ltp = get_ltp(symbol)
    if ltp <= 0:
        return None
    raw = ltp * (1 - 0.00003) if direction.upper() == "BUY" else ltp * (1 + 0.00003)
    trig = round_to_tick(raw, symbol)
    log(f"[fallback-sl] {symbol} -> {trig}")
    return trig


def ensure_slm_exists_for_position(symbol, pos):
    """
    Ensure there's exactly one SL-M for the position (matching side).
    If missing, create using previous closed 5m candle extreme (or fallback).
    """
    expected_trans = side_to_exit_trans(pos["direction"])
    existing = fetch_symbol_slm_from_broker(symbol, expected_trans)
    if existing:
        return  # already have a matching SL-M

    trig = compute_sl_from_prev_candle(symbol, pos["direction"]) or _fallback_sl(symbol, pos["direction"])
    if trig:
        log(f"[ensure-sl] creating SL-M {symbol} -> {trig}")
        place_slm_order(symbol, pos["direction"], pos["qty"], trig)


def trail_slm_positions():
    """
    For every active MIS position at the broker side:
      1) Compute new_trig from previous closed 5m candle (LOW for BUY, HIGH for SELL)
      2) If LTP has already breached new_trig → exit immediately
      3) Else, ensure SL-M exists (opposite side)
      4) ALWAYS modify SL-M to new_trig (even if looser or equal)
    Also, cancel orphan open orders that have no corresponding active position.
    """
    reconcile_orphan_orders()

    for symbol, pos in broker_positions_today.items():
        new_trig = compute_sl_from_prev_candle(symbol, pos["direction"]) or _fallback_sl(symbol, pos["direction"])
        if not new_trig:
            continue

        ltp = get_ltp(symbol)
        if ltp <= 0:
            log(f"[trail] {symbol} skip (no LTP)")
            continue

        # Breach check FIRST
        breached = (pos["direction"].upper() == "BUY" and ltp <= new_trig) or \
                   (pos["direction"].upper() == "SELL" and ltp >= new_trig)
        if breached and exits_allowed_now():
            log(f"[trail] {symbol} BREACH dir={pos['direction']} LTP={ltp} trig={new_trig} → EXIT")
            close_position_market(symbol, pos)
            continue

        if not exits_allowed_now():
            continue

        # Ensure SL-M exists
        expected_trans = side_to_exit_trans(pos["direction"])
        slm = fetch_symbol_slm_from_broker(symbol, expected_trans)
        if not slm:
            log(f"[trail] {symbol} no SL found → ensure first")
            ensure_slm_exists_for_position(symbol, pos)
            slm = fetch_symbol_slm_from_broker(symbol, expected_trans)
            if not slm:
                log(f"[trail] {symbol} still no SL after ensure; skip")
                continue

        old = float(slm["trigger_price"])
        log(f"[trail] {symbol} force-update {old} → {new_trig}")
        modify_sl_order(slm["order_id"], new_trig, symbol)

# ==================== Scheduler ==================== #

def _next_tick_time(now=None, offset_seconds=TRAIL_OFFSET_SECONDS):
    if now is None:
        now = now_ist()
    minute_block = (now.minute // 5) * 5
    base = now.replace(minute=minute_block, second=0, microsecond=0)
    candidate = base + timedelta(minutes=5, seconds=offset_seconds)
    while candidate <= now:
        candidate += timedelta(minutes=5)
    return candidate


def do_trailing_cycle(prev_snapshot):
    curr = fetch_active_positions()
    sync_broker_positions_today()
    sync_active_orders_cache()
    detect_manual_exits_and_cleanup(prev_snapshot if prev_snapshot else {}, curr)
    trail_slm_positions()
    return curr


_prev_snapshot = {}


def detect_manual_exits_and_cleanup(prev_snapshot, curr_snapshot):
    prev_syms = set(prev_snapshot.keys())
    curr_syms = set(curr_snapshot.keys())
    exited = prev_syms - curr_syms
    for sym in exited:
        if exits_allowed_now():
            cancel_all_open_orders_for_symbol(sym)
        recent_exits[sym] = now_ist()
        pending_entries.pop(sym, None)
        log(f"[cooldown] {sym} exit observed → cooldown starts")


def start_trailing_scheduler():
    def scheduler_loop():
        global _prev_snapshot
        log("[scheduler] immediate cycle start")
        _prev_snapshot = do_trailing_cycle(_prev_snapshot)
        while True:
            n = now_ist()
            if (n.hour, n.minute) >= (CUTOFF_EXIT_H, CUTOFF_EXIT_M):
                log("[scheduler] 15:00 reached — stopping scheduler.")
                break
            next_wake = _next_tick_time(n, offset_seconds=TRAIL_OFFSET_SECONDS)
            sleep_s = max(0, int((next_wake - n).total_seconds()))
            log(f"[scheduler] sleeping {sleep_s}s → next {next_wake.strftime('%H:%M:%S')}")
            time.sleep(sleep_s)

            log("[scheduler] wake → cycle")
            _prev_snapshot = do_trailing_cycle(_prev_snapshot)

    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    return t

# ==================== Main loop ==================== #

def main_loop():
    global _prev_snapshot
    log("[main] starting bot (local verbose)…")
    load_instruments_once()
    load_allowed_symbols_5x()

    _prev_snapshot = fetch_active_positions()
    sync_broker_positions_today()
    sync_active_orders_cache()

    start_trailing_scheduler()

    while True:
        n = now_ist()
        if (n.hour, n.minute) >= (CUTOFF_EXIT_H, CUTOFF_EXIT_M):
            log("[main] 15:00 reached — stopping bot.")
            break

        curr = fetch_active_positions()
        sync_broker_positions_today()
        sync_active_orders_cache()
        detect_manual_exits_and_cleanup(_prev_snapshot, curr)
        _prev_snapshot = curr

        if exits_allowed_now():
            for sym, pos in broker_positions_today.items():
                ensure_slm_exists_for_position(sym, pos)

        if before_3pm():
            sigs = gather_signals()
            for sig in sigs:
                if capacity_left() <= 0:
                    log("[entry] capacity full — skipping remaining signals this cycle")
                    break
                place_market_entry(sig["symbol"], sig["side"])

        sleep_for = random.randint(MAIN_SLEEP_MIN, MAIN_SLEEP_MAX)
        log(f"[main] sleep {sleep_for}s")
        time.sleep(sleep_for)


if __name__ == "__main__":
    main_loop()
