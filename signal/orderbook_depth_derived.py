from typing import Any

from signal_common import run_signal_entrypoint

_SOURCE_KIND = "orderbook_depth"


def _walk_slippage(levels: list[dict], target_usd: float, ascending: bool) -> float | None:
    """Walk order book levels to estimate slippage for a given spend.

    Returns average_fill_price - best_price (i.e. slippage cost).
    Returns None if there is insufficient liquidity.
    """
    sorted_levels = sorted(levels, key=lambda x: x["price"], reverse=not ascending)
    if not sorted_levels:
        return None
    best_price = sorted_levels[0]["price"]
    total_cost = 0.0
    total_shares = 0.0
    for level in sorted_levels:
        price = level["price"]
        size = level["size"]
        level_cost = price * size
        remaining = target_usd - total_cost
        if level_cost >= remaining:
            shares_here = remaining / price
            total_shares += shares_here
            total_cost = target_usd
            break
        total_cost += level_cost
        total_shares += size
    if total_cost < target_usd or total_shares == 0:
        return None
    avg_fill = total_cost / total_shares
    return avg_fill - best_price


def _build_orderbook_depth_derived(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    if event.get("signal_kind") != _SOURCE_KIND:
        return None, "skip"
    if event.get("status") != "ok":
        return None, "source_signal_unavailable"

    bids = [b for b in (event.get("bids") or []) if b.get("price") is not None and b.get("size") is not None]
    asks = [a for a in (event.get("asks") or []) if a.get("price") is not None and a.get("size") is not None]

    if not bids and not asks:
        return None, "empty_book"

    total_bid_size = sum(b["size"] for b in bids)
    total_ask_size = sum(a["size"] for a in asks)
    total_size = total_bid_size + total_ask_size

    imbalance = (total_bid_size - total_ask_size) / total_size if total_size > 0 else 0.0

    bid_value = sum(b["price"] * b["size"] for b in bids)
    ask_value = sum(a["price"] * a["size"] for a in asks)
    weighted_mid = (bid_value + ask_value) / total_size if total_size > 0 else None

    best_bid = max((b["price"] for b in bids), default=None)
    best_ask = min((a["price"] for a in asks), default=None)

    # Slippage: cost to buy $100 of YES tokens (walk ask side ascending)
    slippage_at_100 = _walk_slippage(asks, target_usd=100.0, ascending=True)

    sentiment_side = "YES" if imbalance >= 0 else "NO"
    sentiment_confidence = 0.5 + abs(imbalance) * 0.5

    return {
        "sentiment_side": sentiment_side,
        "sentiment_confidence": sentiment_confidence,
        "token_id": event.get("token_id"),
        "total_bid_size": total_bid_size,
        "total_ask_size": total_ask_size,
        "imbalance": imbalance,
        "weighted_mid": weighted_mid,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "slippage_at_100": slippage_at_100,
        "meta": {
            "source_signal_kind": _SOURCE_KIND,
            "source_signal_id": event.get("signal_id"),
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_orderbook_depth_derived",
        default_signal_id="signal-orderbook-depth-derived",
        default_signal_kind="orderbook_depth_derived",
        builder=_build_orderbook_depth_derived,
    )


if __name__ == "__main__":
    main()
