"""
Multi-SSID streaming manager.

Flow per slot:
  1. Connect with optional proxy.
  2. Seed historical candles via the paged get_candles_advanced walk.
  3. Subscribe to raw ticks.
  4. Wait for the next clock boundary (60s-aligned).
  5. Discard ticks until the boundary.
  6. Build candles live from that boundary onward.
  7. Every tick, recompute RSI, BB, EMA and expose them.
"""

import os
import asyncio
import threading
import time
import random
from collections import deque
from typing import Dict, List, Optional, Tuple

from BinaryOptionsToolsV2 import PocketOptionAsync
from BinaryOptionsToolsV2.config import Config

from indicators import recompute
from perf import SlotStats, AssetStats
from seeder import seed_history


TIMEFRAME_SECONDS = 10
MAX_CANDLES = 2400          # allow the full seed to live in memory
RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0
EMA_PERIOD = 6

SLOT_START_GAP_SECS = 8.0
MIN_MARGIN_BEFORE_BOUNDARY = 5.0


class AssetState:
    __slots__ = (
        "asset", "candles", "forming", "last_boundary",
        "rsi", "bb_upper", "bb_middle", "bb_lower",
        "bb_bandwidth", "bb_pct_to_upper", "bb_pct_to_lower",
        "ema", "ema_signal",
        "aligned_started", "aligned_at",
        "seeded_count",
    )

    def __init__(self, asset: str):
        self.asset = asset
        self.candles = deque(maxlen=MAX_CANDLES)
        self.forming = None
        self.last_boundary = None

        self.rsi = None
        self.bb_upper = None
        self.bb_middle = None
        self.bb_lower = None
        self.bb_bandwidth = None
        self.bb_pct_to_upper = None
        self.bb_pct_to_lower = None
        self.ema = None
        self.ema_signal = None

        self.aligned_started = False
        self.aligned_at = None
        self.seeded_count = 0

    def snapshot(self):
        return {
            "asset": self.asset,
            "aligned_started": self.aligned_started,
            "aligned_at": self.aligned_at,
            "seeded_count": self.seeded_count,
            "closed_count": len(self.candles),
            "candles": [
                {"time": t, "open": o, "high": h, "low": l, "close": c}
                for (t, o, h, l, c) in self.candles
            ],
            "forming": self.forming,
            "indicators": {
                "rsi": self.rsi,
                "rsi_period": RSI_PERIOD,
                "bb_upper": self.bb_upper,
                "bb_middle": self.bb_middle,
                "bb_lower": self.bb_lower,
                "bb_period": BB_PERIOD,
                "bb_std": BB_STD,
                "bb_bandwidth": self.bb_bandwidth,
                "bb_pct_to_upper": self.bb_pct_to_upper,
                "bb_pct_to_lower": self.bb_pct_to_lower,
                "ema": self.ema,
                "ema_period": EMA_PERIOD,
                "ema_signal": self.ema_signal,
            },
        }


class Slot:
    def __init__(self, slot_id, ssid, assets, mode="ticks", proxy_url: Optional[str] = None):
        self.slot_id = slot_id
        self.ssid = ssid
        self.assets = assets
        self.mode = mode
        self.proxy_url = proxy_url
        self.stats = SlotStats(slot_id, ssid, assets, mode=mode)
        if proxy_url:
            self.stats.proxy_endpoint = proxy_url.split("@")[-1] if "@" in proxy_url else proxy_url
        self.asset_state: Dict[str, AssetState] = {a: AssetState(a) for a in assets}
        self.state_lock = threading.Lock()
        self.thread = None
        self._stop = threading.Event()

    def start(self):
        self.thread = threading.Thread(
            target=self._run_thread, daemon=True, name=f"slot-{self.slot_id}",
        )
        self.thread.start()

    def _run_thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main())
        except Exception as e:
            self.stats.mark_error(f"slot fatal: {e}")
        finally:
            try:
                loop.close()
            except Exception:
                pass

    async def _main(self):
        await asyncio.sleep(random.uniform(0, 2))

        if not self.ssid:
            self.stats.mark_error("SSID not set")
            return

        # Build config with proxy if present
        cfg_kwargs = {"timeout_secs": 60, "terminal_logging": False}
        if self.proxy_url:
            cfg_kwargs["proxy"] = self.proxy_url
            print(f"[slot {self.slot_id}] using proxy {self.stats.proxy_endpoint}")
        config = Config(**cfg_kwargs)

        try:
            client = PocketOptionAsync(self.ssid, config=config)
            await client.wait_for_assets(timeout=90.0)
            balance = await client.balance()
        except Exception as e:
            self.stats.mark_auth_failure(str(e))
            print(f"[slot {self.slot_id}] connect failed: {e}")
            return

        print(
            f"[slot {self.slot_id}] connected fp={self.stats.ssid_fp} "
            f"balance={balance} assets={len(self.assets)}"
        )
        self.stats.mark_connected()

        # ---- Seed history for each asset ----
        for asset in self.assets:
            try:
                hist = await seed_history(
                    client, asset,
                    timeframe_secs=TIMEFRAME_SECONDS,
                    total_candles=MAX_CANDLES,
                    page_size=60,
                    log_prefix=f"[slot {self.slot_id}]",
                )
            except Exception as e:
                print(f"[slot {self.slot_id}] seed failed for {asset}: {e}")
                continue

            state = self.asset_state[asset]
            for c in hist:
                state.candles.append(c)
            state.seeded_count = len(hist)
            if state.candles:
                state.last_boundary = state.candles[-1][0]

            # Initial indicator calc from history
            closes = [c[4] for c in state.candles]
            if closes:
                self._apply_indicators(state, closes)

        # ---- Wait for the next clock boundary ----
        target_boundary = self._compute_target_boundary()
        wait_secs = target_boundary - time.time()
        print(f"[slot {self.slot_id}] waiting {wait_secs:.1f}s for boundary")

        # Subscribe to ticks BEFORE the boundary
        streams = {}
        for asset in self.assets:
            try:
                streams[asset] = await client.subscribe_symbol(asset)
            except Exception as e:
                self.stats.mark_error(f"subscribe {asset}: {e}")

        # Discard ticks until boundary
        discard_deadline = target_boundary

        async def discard_stream(stream):
            while time.time() < discard_deadline and not self._stop.is_set():
                try:
                    await asyncio.wait_for(stream.__anext__(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                except StopAsyncIteration:
                    return
                except Exception:
                    await asyncio.sleep(0.1)

        await asyncio.gather(
            *[asyncio.create_task(discard_stream(s)) for s in streams.values()],
            return_exceptions=True,
        )

        print(f"[slot {self.slot_id}] boundary crossed at {int(time.time())}")
        with self.state_lock:
            for st in self.asset_state.values():
                st.aligned_started = True
                st.aligned_at = int(time.time())
                st.forming = None

        # ---- Live ingest ----
        tasks = [
            asyncio.create_task(self._ingest_loop(asset, stream))
            for asset, stream in streams.items()
        ]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.stats.mark_disconnected("stream loop ended")
            try:
                await client.shutdown()
            except Exception:
                pass

    def _compute_target_boundary(self):
        now = time.time()
        next_boundary = (int(now) // TIMEFRAME_SECONDS + 1) * TIMEFRAME_SECONDS
        if next_boundary - now < MIN_MARGIN_BEFORE_BOUNDARY:
            next_boundary += TIMEFRAME_SECONDS
        return float(next_boundary)

    async def _ingest_loop(self, asset: str, stream):
        state = self.asset_state[asset]
        st = self.stats.get_asset(asset)
        last_ts = 0

        while not self._stop.is_set():
            try:
                tick = await stream.__anext__()
            except StopAsyncIteration:
                self.stats.mark_error(f"stream ended: {asset}")
                return
            except Exception as e:
                self.stats.mark_error(f"tick error {asset}: {e}")
                await asyncio.sleep(0.5)
                continue

            try:
                price = float(tick.get("close") or tick.get("price") or 0)
                ts = int(tick.get("timestamp") or tick.get("time") or 0)
                if price <= 0 or ts <= 0 or ts < last_ts:
                    continue
                last_ts = ts
                with self.state_lock:
                    if not state.aligned_started:
                        continue
                self._ingest_tick(state, st, price, ts)
            except Exception as e:
                self.stats.mark_error(f"ingest {asset}: {e}")

    def _ingest_tick(self, state, st, price, ts):
        bucket = (ts // TIMEFRAME_SECONDS) * TIMEFRAME_SECONDS

        with self.state_lock:
            if state.forming is None:
                state.forming = {
                    "time": bucket,
                    "open": price, "high": price, "low": price, "close": price,
                }
                state.last_boundary = bucket
            elif bucket > state.last_boundary:
                gap_buckets = (bucket - state.last_boundary) // TIMEFRAME_SECONDS - 1
                if gap_buckets > 0:
                    st.gaps += gap_buckets
                f = state.forming
                state.candles.append((f["time"], f["open"], f["high"], f["low"], f["close"]))
                st.record_candle()
                state.last_boundary = bucket
                state.forming = {
                    "time": bucket,
                    "open": price, "high": price, "low": price, "close": price,
                }
            else:
                state.forming["high"] = max(state.forming["high"], price)
                state.forming["low"] = min(state.forming["low"], price)
                state.forming["close"] = price

            closes = [c[4] for c in state.candles]
            closes.append(state.forming["close"])
            self._apply_indicators(state, closes)

        st.record_tick(ts, price)
        self.stats.record_slot_tick()

    def _apply_indicators(self, state, closes):
        ind = recompute(closes, RSI_PERIOD, BB_PERIOD, BB_STD, EMA_PERIOD)
        state.rsi = ind["rsi"]
        state.bb_upper = ind["bb_upper"]
        state.bb_middle = ind["bb_middle"]
        state.bb_lower = ind["bb_lower"]
        state.bb_bandwidth = ind["bb_bandwidth"]
        state.bb_pct_to_upper = ind["bb_pct_to_upper"]
        state.bb_pct_to_lower = ind["bb_pct_to_lower"]
        state.ema = ind["ema"]
        state.ema_signal = ind["ema_signal"]

    def snapshot(self):
        with self.state_lock:
            assets_snap = {a: s.snapshot() for a, s in self.asset_state.items()}
        return {
            "slot_id": self.slot_id,
            "mode": self.mode,
            "proxy": self.stats.proxy_endpoint,
            "assets": assets_snap,
            "stats": self.stats.snapshot(),
        }


class MultiStreamManager:
    def __init__(self, vm_role, slots_config):
        self.vm_role = vm_role
        self.slots = []
        for i, (ssid, assets, mode, proxy) in enumerate(slots_config, start=1):
            self.slots.append(Slot(slot_id=i, ssid=ssid or "", assets=assets,
                                   mode=mode, proxy_url=proxy or None))

    def start(self, startup_delay_secs=0.0):
        def _run():
            if startup_delay_secs > 0:
                print(f"[manager] startup delay {startup_delay_secs}s")
                time.sleep(startup_delay_secs)
            for i, slot in enumerate(self.slots):
                slot.start()
                if i < len(self.slots) - 1:
                    time.sleep(SLOT_START_GAP_SECS)
        threading.Thread(target=_run, daemon=True, name="manager-start").start()

    def snapshot_all(self):
        return {
            "vm_role": self.vm_role,
            "timeframe": TIMEFRAME_SECONDS,
            "slots": [s.snapshot() for s in self.slots],
        }

    def snapshot_stats(self):
        return {
            "vm_role": self.vm_role,
            "slots": [s.stats.snapshot() for s in self.slots],
        }

    def refresh_rates(self):
        for s in self.slots:
            s.stats.refresh_rates()
