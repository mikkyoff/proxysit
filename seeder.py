"""
Historical candle seeder — walks backward through get_candles_advanced
in pages and prepends each page.

Adapted from the developer's 1000candles.py example.
"""

import asyncio


async def seed_history(
    client,
    asset: str,
    timeframe_secs: int = 60,
    total_candles: int = 2400,
    page_size: int = 60,
    log_prefix: str = "[seed]",
):
    """
    Fetch `total_candles` closed candles ending at now-ish.
    Returns a list of tuples: (time, open, high, low, close).
    Oldest first.
    """
    try:
        server_time = await client.get_server_time()
    except Exception as e:
        print(f"{log_prefix} could not get server time: {e}")
        return []

    print(f"{log_prefix} server time: {server_time}")

    total = []
    pages = total_candles // page_size
    for i in range(pages):
        server_time -= page_size * timeframe_secs
        if server_time < 0:
            break
        try:
            page = await client.get_candles_advanced(
                asset,
                timeframe_secs,
                page_size * timeframe_secs,
                server_time,
            )
        except Exception as e:
            print(f"{log_prefix} page {i} failed: {e}")
            continue

        if not page:
            print(f"{log_prefix} page {i} empty — stopping")
            break

        # Normalize to tuples and prepend
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
        print(f"{log_prefix} page {i}: +{len(parsed)} candles (total {len(total)})")

        # Small pause to be polite to the server
        await asyncio.sleep(0.3)

    # Deduplicate by timestamp
    seen = set()
    deduped = []
    for c in total:
        if c[0] not in seen:
            seen.add(c[0])
            deduped.append(c)
    deduped.sort(key=lambda x: x[0])

    print(f"{log_prefix} final: {len(deduped)} unique closed candles")
    return deduped
