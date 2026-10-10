"""
Pocket Option proxy test — single asset, single SSID, single proxy.

Slot config: (ssid, [assets], mode, proxy_url)
"""

import os
import time
import ctypes
import gc
import threading
from urllib.parse import quote
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv

from multistream import MultiStreamManager, TIMEFRAME_SECONDS
from perf import PerfHistory

load_dotenv()

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

VM_ROLE = os.getenv("VM_ROLE", "A").upper()
STARTUP_DELAY_SECS = float(os.getenv("STARTUP_DELAY_SECS", "0"))


# ---------------------------------------------------------------------
# Proxy loading
# ---------------------------------------------------------------------
def _build_proxy(host, port, user, pw):
    if not all([host, port, user, pw]):
        return ""
    return f"http://{quote(user.strip(), safe='')}:{quote(pw.strip(), safe='')}@{host.strip()}:{port.strip()}"


# Read a single proxy from individual env vars (PROXY_HOST/PORT/USER/PASS)
PROXY_URL = _build_proxy(
    os.getenv("PROXY_HOST", ""),
    os.getenv("PROXY_PORT", ""),
    os.getenv("PROXY_USER", ""),
    os.getenv("PROXY_PASS", ""),
)


# ---------------------------------------------------------------------
# Slot config — one slot, one asset
# ---------------------------------------------------------------------
SSID = os.getenv("SSID", "")
ASSET = os.getenv("ASSET", "EURUSD_otc")

SLOTS_CONFIG = [
    (SSID, [ASSET], "ticks", PROXY_URL),
]

manager = MultiStreamManager(VM_ROLE, SLOTS_CONFIG)
perf_history = PerfHistory(maxlen=20)


def _perf_sampler():
    while True:
        time.sleep(30)
        try:
            manager.refresh_rates()
            perf_history.append({"ts": int(time.time()), "data": manager.snapshot_stats()})
        except Exception as e:
            print(f"[perf] sampler error: {e}")


def _memory_maintenance():
    time.sleep(600)
    while True:
        try:
            gc.collect()
            try:
                libc = ctypes.CDLL("libc.so.6")
                libc.malloc_trim(0)
            except Exception:
                pass
        except Exception:
            pass
        time.sleep(1800)


def _startup():
    manager.start(startup_delay_secs=STARTUP_DELAY_SECS)
    threading.Thread(target=_perf_sampler, daemon=True).start()
    threading.Thread(target=_memory_maintenance, daemon=True).start()


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/candles")
def get_candles():
    return jsonify({
        "vm_role": VM_ROLE,
        "timeframe": TIMEFRAME_SECONDS,
        "server_time": int(time.time()),
        "data": manager.snapshot_all(),
    })


@app.route("/api/perf")
def get_perf():
    manager.refresh_rates()
    return jsonify({
        "vm_role": VM_ROLE,
        "uptime_seconds": int(time.time() - _START_TS),
        "data": manager.snapshot_stats(),
    })


@app.route("/api/debug/proxy")
def debug_proxy():
    def mask(v):
        if not v:
            return None
        if len(v) <= 4:
            return "****"
        return v[:2] + "****" + v[-2:]
    return jsonify({
        "vm_role": VM_ROLE,
        "proxy_env": {
            "PROXY_HOST": mask(os.getenv("PROXY_HOST", "")),
            "PROXY_PORT": mask(os.getenv("PROXY_PORT", "")),
            "PROXY_USER": mask(os.getenv("PROXY_USER", "")),
            "PROXY_PASS": mask(os.getenv("PROXY_PASS", "")),
        },
        "proxy_url_resolved": bool(PROXY_URL),
        "proxy_endpoint": PROXY_URL.split("@")[-1] if PROXY_URL else None,
        "ssid_set": bool(SSID),
        "asset": ASSET,
    })


@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "vm_role": VM_ROLE,
        "slot_count": len(SLOTS_CONFIG),
        "proxy_configured": bool(PROXY_URL),
    })


_START_TS = time.time()
_startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
