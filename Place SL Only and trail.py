#!/usr/bin/env python3
"""
ensure_and_trail_slm.py

Purpose:
 - Connect to Kite, fetch active MIS positions, check whether a matching SL-M exists for each position.
 - If SL-M is missing, create one using the previous completed 5-minute candle extreme
   (LOW for BUY positions, HIGH for SELL positions).
 - Periodically (every 5 minutes + small offset) recompute the previous 5m candle extreme and:
     * If price breached the new extreme -> exit position at market
     * Else modify existing SL-M to match the new extreme (or replace it if modify fails)
 - Cancel orphan SL / SL-M orders that have no corresponding MIS position.
 - Additionally: run a fast order-watcher every 8-13 seconds to detect missing SLs for active positions
   and place SLs immediately (helps low-latency enforcement while the 5-min trail loop runs).
 - Stops trailing after 15:00 IST (configurable)

Env vars required:
 - KITE_API_KEY
 - KITE_ACCESS_TOKEN
 - SIMULATION_MODE (optional, default false)

Usage:
 python3 ensure_and_trail_slm.py
"""
import os
import time
import random
import threading
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
import requests
from kiteconnect import KiteConnect
import platform
from dotenv import load_dotenv

load_dotenv()

# Prevent system sleep (Windows)
if platform.system() == "Windows":
    import ctypes
    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)

# ---------------- Config -----------------
india_tz = pytz.timezone('Asia/Kolkata')
SIMULATION_MODE = os.getenv('SIMULATION_MODE', 'false').lower() in ('1', 'true', 'yes')
KITE_API_KEY = os.getenv('KITE_API_KEY', '')
KITE_ACCESS_TOKEN = os.getenv('KITE_ACCESS_TOKEN', '')
TRAIL_OFFSET_SECONDS = 90  # 1m30s after 5m boundary
CUTOFF_EXIT_H, CUTOFF_EXIT_M = 15, 0
# Fast watcher will randomize between 8-13s
FAST_WATCHER_MIN_S = 8
FAST_WATCHER_MAX_S = 13
# ORDER_POLL_INTERVAL kept for backward compatibility but fast watcher randomizes between FAST_WATCHER_MIN_S..MAX_S
ORDER_POLL_INTERVAL = 15  # seconds for fallback/compat

# ---------------- Kite client + hardened session -----------------
kite = KiteConnect(api_key=KITE_API_KEY)
if KITE_ACCESS_TOKEN:
    kite.set_access_token(KITE_ACCESS_TOKEN)

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

HARDENED_HTTP = requests.Session()
retry_cfg = Retry(total=5, backoff_factor=0.4, status_forcelist=(429,500,502,503,504), allowed_methods=frozenset(["GET","POST"]))
HARDENED_HTTP.mount('https://', HTTPAdapter(max_retries=retry_cfg))

kite_lock = threading.Lock()


def call_kite(fn, *args, **kwargs):
    for attempt in range(5):
        try:
            with kite_lock:
                return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            transient = any(x in msg for x in ("SSLEOFError","UNEXPECTED_EOF_WHILE_READING","Max retries exceeded","Read timed out","Connection aborted","RemoteDisconnected"))
            if transient and attempt < 4:
                sleep_s = min(0.5 * (2 ** attempt), 4.0) + random.uniform(0,0.25)
                print(f"[kite-retry] transient {msg} -> retry in {sleep_s:.2f}s")
                time.sleep(sleep_s)
                continue
            print(f"[kite] call failed: {msg}")
            raise

# ---------------- Helpers -----------------

def now_ist():
    return datetime.now(india_tz)


def before_cutoff():
    n = now_ist()
    return (n.hour, n.minute) < (CUTOFF_EXIT_H, CUTOFF_EXIT_M)

# minimal tick helper (could be extended by loading instruments)
DEFAULT_TICK = 0.05

def round_to_tick(price, tick=DEFAULT_TICK):
    steps = round(price / tick)
    return round(steps * tick, 2)

# ---------------- Historical 5m candles via Kite REST -----------------

def _five_minute_bounds_for_fetch(now=None, n_candles=3):
    if now is None:
        now = now_ist()
    minute_block = (now.minute // 5) * 5
    current_window_start = now.replace(minute=minute_block, second=0, microsecond=0)
    to_ts = current_window_start
    from_ts = to_ts - timedelta(minutes=5 * n_candles)
    return from_ts, to_ts

# NOTE: this routine expects instrument_token; try to discover via instruments list
_all_instruments = None
symbol_to_token = {}

def load_instruments_once():
    global _all_instruments
    if _all_instruments is not None:
        return
    try:
        _all_instruments = call_kite(kite.instruments, 'NSE')
        for inst in _all_instruments:
            symbol_to_token[inst['tradingsymbol'].upper()] = inst.get('instrument_token')
        print(f"[init] loaded instruments: {len(symbol_to_token)} symbols")
    except Exception as e:
        print(f"[init] instruments load failed: {e}")
        _all_instruments = []


def ensure_token(sym):
    tok = symbol_to_token.get(sym.upper())
    if not tok:
        load_instruments_once()
        tok = symbol_to_token.get(sym.upper())
    if not tok:
        print(f"[token] not found for {sym}")
    return tok


def get_last_completed_5m_candle(symbol: str):
    tok = ensure_token(symbol)
    if not tok:
        return None
    from_ts, to_ts = _five_minute_bounds_for_fetch(now_ist(), n_candles=2)
    url = f"https://api.kite.trade/instruments/historical/{tok}/5minute"
    params = {"from": from_ts.strftime('%Y-%m-%d %H:%M:%S'), "to": to_ts.strftime('%Y-%m-%d %H:%M:%S'), "oi": 0}
    headers = {"X-Kite-Version": "3", "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}"}
    try:
        r = HARDENED_HTTP.get(url, headers=headers, params=params, timeout=8)
        r.raise_for_status()
        candles = r.json().get('data', {}).get('candles', []) or []
        if not candles:
            return None
        # return previous closed candle (off-by-one safe)
        if len(candles) == 1:
            return None
        prev = candles[-1]
        ts, o, h, l, c, v = prev
        return {"ts": ts, "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(v)}
    except Exception as e:
        print(f"[candles] {symbol} failed: {e}")
        return None

# ---------------- Broker fetch & SL helpers -----------------

def fetch_active_positions():
    out = {}
    try:
        res = call_kite(kite.positions)
        for p in (res.get('day', []) or []):
            if p.get('product') != 'MIS':
                continue
            qty = int(p.get('quantity', 0))
            if qty == 0:
                continue
            sym = p.get('tradingsymbol','').upper()
            side = 'BUY' if qty > 0 else 'SELL'
            out[sym] = { 'qty': abs(qty), 'direction': side, 'avg_price': float(p.get('average_price',0.0)) }
    except Exception as e:
        print(f"[fetch positions] {e}")
    return out


def fetch_symbol_slm_from_broker(symbol: str, expected_trans: str):
    ACCEPTABLE = {"OPEN","TRIGGER PENDING","PUT ORDER REQUEST RECEIVED","MODIFY_VALIDATION_PENDING","MODIFY REQUEST RECEIVED","VALIDATION PENDING","OPEN PENDING"}
    best = None
    try:
        for o in call_kite(kite.orders) or []:
            if o.get('product') != 'MIS':
                continue
            if o.get('variety') != 'regular':
                continue
            if o.get('order_type') != 'SL-M':
                continue
            if o.get('tradingsymbol','').upper() != symbol.upper():
                continue
            if o.get('transaction_type') != expected_trans:
                continue
            if o.get('status') not in ACCEPTABLE:
                continue
            cur = { 'order_id': o.get('order_id'), 'trigger_price': float(o.get('trigger_price') or 0.0), 'status': o.get('status'), 'transaction_type': o.get('transaction_type'), 'updated_at': o.get('updated_at') or o.get('order_timestamp') }
            if not best or str(cur.get('updated_at','')) > str(best.get('updated_at','')):
                best = cur
    except Exception as e:
        print(f"[fetch_slm] {symbol} {e}")
    return best


def place_slm_order(symbol, direction, qty, trigger_price):
    if SIMULATION_MODE:
        print(f"[SIM SLM] {symbol} {direction} qty={qty} trig={trigger_price}")
        return f"SIM_SLM_{symbol}"
    try:
        trans = 'SELL' if direction.upper() == 'BUY' else 'BUY'
        oid = call_kite(kite.place_order, tradingsymbol=symbol, exchange='NSE', transaction_type=trans, quantity=qty, order_type='SL-M', trigger_price=trigger_price, price=0, product='MIS', variety='regular')
        print(f"[place_slm] {symbol} oid={oid} trig={trigger_price}")
        return oid
    except Exception as e:
        print(f"[place_slm] {symbol} {e}")
        return None


def modify_sl_order(order_id, new_trigger, symbol, max_retries=3):
    if SIMULATION_MODE:
        print(f"[SIM modify] {symbol} -> {new_trigger}")
        return True
    for attempt in range(max_retries):
        try:
            call_kite(kite.modify_order, variety='regular', order_id=order_id, trigger_price=new_trigger)
            print(f"[modify_sl] {symbol} -> {new_trigger} (ok)")
            return True
        except Exception as e:
            sleep_s = min(0.5 * (2 ** attempt), 4.0) + random.uniform(0,0.25)
            print(f"[modify_sl] attempt {attempt+1} failed: {e} -> retry {sleep_s:.2f}s")
            time.sleep(sleep_s)
    print(f"[modify_sl] {symbol} giving up")
    return False

# robust update/replace similar to your HFT bot

def update_or_replace_slm(symbol: str, pos: dict, new_trig: float):
    if not before_cutoff():
        return 'SKIPPED-AFTER-CUTOFF'
    expected_trans = 'SELL' if pos['direction'].upper() == 'BUY' else 'BUY'
    slm = fetch_symbol_slm_from_broker(symbol, expected_trans)
    if slm:
        ok = modify_sl_order(slm['order_id'], new_trig, symbol)
        if ok:
            return 'MODIFIED'
    # cancel any stale matching SL/SL-M for side
    try:
        for o in call_kite(kite.orders) or []:
            if o.get('product') != 'MIS':
                continue
            if o.get('variety') != 'regular':
                continue
            if o.get('tradingsymbol','').upper() != symbol.upper():
                continue
            if o.get('order_type') not in ('SL','SL-M'):
                continue
            if o.get('transaction_type') != expected_trans:
                continue
            try:
                call_kite(kite.cancel_order, variety='regular', order_id=o['order_id'])
                print(f"[update_or_replace] canceled stale SL oid={o.get('order_id')}")
            except Exception as e:
                print(f"[update_or_replace] cancel failed: {e}")
    except Exception as e:
        print(f"[update_or_replace] list/cancel error: {e}")

    oid = place_slm_order(symbol, pos['direction'], pos['qty'], new_trig)
    return 'REPLACED' if oid else 'FAILED'

# ---------------- New: reconcile orphan SL/SL-M orders -----------------

def reconcile_orphan_orders(active_symbols: set):
    """Cancel SL / SL-M orders on broker that have no corresponding active MIS position.
    active_symbols: set of tradingsymbol strings (uppercase) representing current MIS positions.
    """
    if not before_cutoff():
        print('[reconcile] cutoff reached - skipping orphan cancellation')
        return
    try:
        for o in call_kite(kite.orders) or []:
            if o.get('product') != 'MIS':
                continue
            if o.get('variety') != 'regular':
                continue
            if o.get('order_type') not in ('SL', 'SL-M'):
                continue
            sym = o.get('tradingsymbol','').upper()
            if sym in active_symbols:
                continue
            # only cancel orders in pending/openish states
            if o.get('status') not in ('OPEN', 'TRIGGER PENDING', 'OPEN PENDING', 'PUT ORDER REQUEST RECEIVED', 'VALIDATION PENDING', 'MODIFY VALIDATION PENDING', 'MODIFY REQUEST RECEIVED'):
                continue
            try:
                call_kite(kite.cancel_order, variety='regular', order_id=o['order_id'])
                print(f"[reconcile] canceled orphan SL for {sym} oid={o.get('order_id')}")
            except Exception as e:
                print(f"[reconcile] cancel failed for {sym}: {e}")
    except Exception as e:
        print(f"[reconcile] listing orders failed: {e}")

# ---------------- Main trailing loop -----------------

def compute_sl_and_meta(symbol: str, direction: str):
    last = get_last_completed_5m_candle(symbol)
    if not last:
        return None, None
    raw = last['low'] if direction.upper() == 'BUY' else last['high']
    trig = round_to_tick(raw)
    return trig, last


def close_position_market(symbol: str, pos: dict):
    if SIMULATION_MODE:
        print(f"[SIM EXIT] {symbol} closing {pos}")
        return 'SIM_EXIT'
    trans = 'SELL' if pos['direction'].upper() == 'BUY' else 'BUY'
    qty = int(pos.get('qty',0) or 0)
    if qty <= 0:
        return None
    try:
        oid = call_kite(kite.place_order, tradingsymbol=symbol, exchange='NSE', transaction_type=trans, quantity=qty, order_type='MARKET', product='MIS', variety='regular')
        print(f"[exit] {symbol} market-exit placed: {oid}")
        return oid
    except Exception as e:
        print(f"[close_market] {symbol} {e}")
        return None

# ---------------- Fast order watcher (every ~8-13s) -----------------

def fast_order_watcher(stop_event: threading.Event):
    """
    Runs in background. Every ~8-13 seconds:
      - fetch current MIS positions
      - cancel SL / SL-M orders for symbols that no longer have MIS positions (orphans)
      - ensure each active MIS position has a matching SL-M; if missing, place one using the latest 5m extreme
    This watcher intentionally does NOT modify existing SL-M (modifications happen in the 5-min trail loop).
    """
    print('[watcher] fast watcher started (8-13s cadence)')
    while not stop_event.is_set() and before_cutoff():
        try:
            positions = fetch_active_positions()
            active_symbols = set(positions.keys())

            # 1) Cancel orphan SL/SL-M orders first
            try:
                ACCEPT_CANCEL = ('OPEN', 'TRIGGER PENDING', 'OPEN PENDING', 'PUT ORDER REQUEST RECEIVED', 'VALIDATION PENDING', 'MODIFY VALIDATION PENDING', 'MODIFY REQUEST RECEIVED')
                for o in call_kite(kite.orders) or []:
                    if o.get('product') != 'MIS':
                        continue
                    if o.get('variety') != 'regular':
                        continue
                    if o.get('order_type') not in ('SL', 'SL-M'):
                        continue
                    sym = o.get('tradingsymbol','').upper()
                    # If there is an active position for this symbol, skip cancellation (we only cancel orphans)
                    if sym in active_symbols:
                        continue
                    # only cancel orders in pending/openish states
                    if o.get('status') not in ACCEPT_CANCEL:
                        continue
                    try:
                        call_kite(kite.cancel_order, variety='regular', order_id=o['order_id'])
                        print(f"[watcher-reconcile] canceled orphan SL for {sym} oid={o.get('order_id')}")
                    except Exception as e:
                        print(f"[watcher-reconcile] cancel failed for {sym}: {e}")
            except Exception as e:
                print(f"[watcher-reconcile] listing/cancel orders failed: {e}")

            # 2) Ensure active MIS positions have an SL-M. If missing, place one using last 5m extreme.
            try:
                for sym, pos in positions.items():
                    if not before_cutoff():
                        break
                    expected_trans = 'SELL' if pos['direction'].upper() == 'BUY' else 'BUY'
                    existing_slm = fetch_symbol_slm_from_broker(sym, expected_trans)
                    if existing_slm:
                        # SL-M exists; do not modify here (trail loop handles modifications).
                        continue
                    # No SL-M found for this active position -> compute and place
                    new_trig, last = compute_sl_and_meta(sym, pos['direction'])
                    if not new_trig:
                        print(f"[watcher] cannot compute trig for {sym} (no candle) -> skipping placement")
                        continue
                    # quick LTP check: if price already breached, don't place SL-M, instead close market
                    try:
                        q = call_kite(kite.quote, f"NSE:{sym}")
                        ltp = float(q[f"NSE:{sym}"]['last_price'])
                    except Exception as e:
                        print(f"[watcher] {sym} ltp fetch failed: {e}")
                        ltp = None
                    breached = False
                    if ltp is not None:
                        breached = (pos['direction'].upper() == 'BUY' and ltp <= new_trig) or (pos['direction'].upper() == 'SELL' and ltp >= new_trig)
                    if breached:
                        print(f"[watcher] {sym} already breached (ltp={ltp} trig={new_trig}) -> market exit")
                        close_position_market(sym, pos)
                        continue
                    # place SL-M
                    oid = place_slm_order(sym, pos['direction'], pos['qty'], new_trig)
                    if oid:
                        print(f"[watcher] placed SL-M for {sym} trig={new_trig} oid={oid}")
                    else:
                        print(f"[watcher] failed to place SL-M for {sym}")
            except Exception as e:
                print(f"[watcher] ensure-SLM loop error: {e}")

        except Exception as e:
            print(f"[watcher] error: {e}")

        # randomize next poll between FAST_WATCHER_MIN_S and FAST_WATCHER_MAX_S seconds
        wait_s = random.randint(FAST_WATCHER_MIN_S, FAST_WATCHER_MAX_S)
        stop_event.wait(wait_s)
    print('[watcher] exiting')

# ---------------- Main trail loop (5-min cadence) -----------------
def trail_loop(poll_interval_seconds=300):
    print('[trail] start')
    while True:
        if not before_cutoff():
            print('[trail] cutoff reached — stopping')
            break
        positions = fetch_active_positions()
        active_syms = set(positions.keys())

        # Reconcile orphan SL / SL-M orders: cancel any SLs for symbols without active MIS positions
        reconcile_orphan_orders(active_syms)

        if not positions:
            print('[trail] no active MIS positions — sleeping')
        for symbol, pos in positions.items():
            direction = pos['direction']
            new_trig, last = compute_sl_and_meta(symbol, direction)
            last_ts = last['ts'] if last else 'NA'
            if not new_trig:
                print(f"[diag] {symbol} cannot compute new_trig — skipping")
                continue
            # check LTP
            try:
                q = call_kite(kite.quote, f"NSE:{symbol}")
                ltp = float(q[f"NSE:{symbol}"]['last_price'])
            except Exception as e:
                print(f"[diag] {symbol} ltp fetch failed: {e}")
                continue
            # breach check
            breached = (direction.upper() == 'BUY' and ltp <= new_trig) or (direction.upper() == 'SELL' and ltp >= new_trig)
            if breached:
                print(f"[diag] {symbol} breached (ltp={ltp}, trig={new_trig}) -> exit")
                close_position_market(symbol, pos)
                continue
            # ensure/slm modify
            act = update_or_replace_slm(symbol, pos, new_trig)
            print(f"[diag] {symbol} dir={direction} prev={last_ts} trig={new_trig} ltp={ltp} action={act}")
        # sleep until next 5-min boundary + small offset
        now = now_ist()
        minute_block = (now.minute // 5) * 5
        base = now.replace(minute=minute_block, second=0, microsecond=0)
        candidate = base + timedelta(minutes=5, seconds=TRAIL_OFFSET_SECONDS)
        if candidate <= now:
            candidate += timedelta(minutes=5)
        sleep_s = max(1, (candidate - now).total_seconds())
        print(f"[trail] sleeping {int(sleep_s)}s until next tick {candidate.strftime('%H:%M:%S')}")
        time.sleep(sleep_s)

if __name__ == '__main__':
    load_instruments_once()

    stop_evt = threading.Event()
    watcher_thread = threading.Thread(target=fast_order_watcher, args=(stop_evt,), daemon=True)
    watcher_thread.start()

    try:
        trail_loop()
    except KeyboardInterrupt:
        print('stopped by user')
    finally:
        stop_evt.set()
        watcher_thread.join(timeout=3)
        print('exiting')
