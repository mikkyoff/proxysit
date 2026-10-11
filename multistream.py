"""
Multi-SSID streaming manager — boundary-first, live-tick-first, history-after.
"""

import asyncio
import threading
import time
import random
from collections import deque
from typing import Dict, List, Optional

from BinaryOptionsToolsV2 import PocketOptionAsync
from BinaryOptionsToolsV2.config import Config

from indicators import recompute
from perf import SlotStats, AssetStats


TIMEFRAME_SECONDS = 60
MAX_CANDLES = 500
HISTORY_CANDLES = 150

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
        "history_loaded", "history_count",
        "first_live_tick_seen",
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
        self.history_loaded = False
        self.history_count = 0
        self.first_live_tick_seen = False

    def snapshot(self):
        return {
            "asset": self.asset,
            "aligned_started": self.aligned_started,
            "aligned_at": self.aligned_at,
            "history_loaded": self.history_loaded,
            "history_count": self.history_count,
            "first_live_tick_seen": self.first_live_tick_seen,
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

        cfg_kwargs = {"timeout_secs": 60, "terminal_logging": False}
        if self.proxy_url:
            cfg_kwargs["proxy"] = self.proxy_url
            print(f"[slot {self.slot_id}] using proxy {self.stats.proxy_endpoint}")
        else:
            print(f"[slot {self.slot_id}] running direct")
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

        target_boundary = self._compute_target_boundary()
        wait_secs = target_boundary - time.time()
        print(f"[slot {self.slot_id}] waiting {wait_secs:.1f}s for boundary at {int(target_boundary)}")

        streams = {}
        for asset in self.assets:
            try:
                streams[asset] = await client.subscribe_symbol(asset)
            except Exception as e:
                self.stats.mark_error(f"subscribe {asset}: {e}")
                print(f"[slot {self.slot_id}] subscribe {asset} failed: {e}")

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

        now = int(time.time())
        print(f"[slot {self.slot_id}] boundary crossed at {now}")
        with self.state_lock:
            for st in self.asset_state.values():
                st.aligned_started = True
                st.aligned_at = now
                st.forming = None

        # Start ingest loops FIRST, then wait for the first live tick
        ingest_tasks = []
        for asset, stream in streams.items():
            ingest_tasks.append(
                asyncio.create_task(self._ingest_loop(asset, stream))
            )

        # Wait for each asset's first live tick (up to 20s) before pulling history
        for asset in self.assets:
            state = self.asset_state[asset]
            for _ in range(200):  # up to 20 seconds
                with self.state_lock:
                    if state.first_live_tick_seen:
                        break
                await asyncio.sleep(0.1)
            print(f"[slot {self.slot_id}] {asset}: first_live_tick_seen={state.first_live_tick_seen}")

        # Now pull history in parallel
        history_tasks = [
            asyncio.create_task(self._fetch_history(client, asset))
            for asset in self.assets
        ]

        try:
            await asyncio.gather(*ingest_tasks, *history_tasks, return_exceptions=True)
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

    async def _fetch_history(self, client, asset: str):
        state = self.asset_state[asset]
        log_prefix = f"[slot {self.slot_id}]"

        try:
            server_time = await client.get_server_time()
        except Exception as e:
            print(f"{log_prefix} history {asset}: server time failed: {e}")
            return

        print(f"{log_prefix} history {asset}: server_time={server_time}")

        total = []
        page_size = 30
        pages = (HISTORY_CANDLES + page_size - 1) // page_size

        for i in range(pages):
            anchor = server_time - (i + 1) * page_size * TIMEFRAME_SECONDS
            if anchor < 0:
                break
            try:
                page = await client.get_candles_advanced(
                    asset,
                    TIMEFRAME_SECONDS,
                    page_size * TIMEFRAME_SECONDS,
                    anchor,
                )
            except Exception as e:
                print(f"{log_prefix} history {asset}: page {i} failed: {e}")
                continue

            if not page:
                print(f"{log_prefix} history {asset}: page {i} empty")
                continue

            parsed = []
            for c in page:
                try:
                    parsed.append((
                        int(c["time"]),
                        float(c["open"]),
                        float(c["high"]),
                        float(c["low"]),
                        float(c["close"]),
                    ))
                except Exception:
                    continue
            total = parsed + total
            print(f"{log_prefix} history {asset}: page {i} got {len(parsed)}")
            await asyncio.sleep(0.2)

        seen = set()
        deduped = []
        for c in total:
            if c[0] not in seen:
                seen.add(c[0])
                deduped.append(c)
        deduped.sort(key=lambda x: x[0])
        deduped = deduped[-HISTORY_CANDLES:]

        print(f"{log_prefix} history {asset}: deduped={len(deduped)}")

        with self.state_lock:
            if not deduped:
                state.history_loaded = True
                print(f"{log_prefix} history {asset}: NO CANDLES")
                return

            current = list(state.candles)
            # The forming candle's bucket is the cutoff. Anything older than
            # the forming candle's start is valid history to prepend.
            if state.forming is not None:
                cutoff = state.forming["time"]
            elif current:
                cutoff = current[0][0]
            else:
                cutoff = server_time

            history_to_prepend = [c for c in deduped if c[0] < cutoff]

            combined = history_to_prepend + current
            combined = combined[-MAX_CANDLES:]
            state.candles = deque(combined, maxlen=MAX_CANDLES)
            state.history_count = len(history_to_prepend)
            state.history_loaded = True

            closes = [c[4] for c in state.candles]
            if state.forming:
                closes.append(state.forming["close"])
            if closes:
                self._apply_indicators(state, closes)

        print(
            f"{log_prefix} history {asset}: prepended {len(history_to_prepend)}, "
            f"cutoff={cutoff}, total={len(state.candles)}"
        )

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
                state.first_live_tick_seen = True
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
