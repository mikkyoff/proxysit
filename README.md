# Pocket Option Proxy Test

Single asset (EURUSD_otc), single SSID, single proxy, 1-minute candles,
2400 historical candles seeded via paged `get_candles_advanced` walk.

## Endpoints

- `/` — dashboard
- `/api/candles` — candles + indicators
- `/api/perf` — perf stats
- `/api/debug/proxy` — what proxy env vars the container actually sees
- `/api/health`

## Env vars
