from typing import Any

from signal_common import run_signal_entrypoint

_SOURCE_KIND = "trade_flow"


def _vwap(trades: list[dict]) -> float | None:
    total_value = sum((t.get("size") or 0.0) * (t.get("price") or 0.0) for t in trades)
    total_size = sum(t.get("size") or 0.0 for t in trades)
    return total_value / total_size if total_size > 0 else None


def _build_trade_flow_derived(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    if event.get("signal_kind") != _SOURCE_KIND:
        return None, "skip"
    if event.get("status") != "ok":
        return None, "source_signal_unavailable"

    trades = [t for t in (event.get("trades") or []) if isinstance(t, dict)]
    if not trades:
        return None, "no_trades"

    buys = [t for t in trades if t.get("side") == "BUY"]
    sells = [t for t in trades if t.get("side") == "SELL"]

    buy_size = sum(t.get("size") or 0.0 for t in buys)
    sell_size = sum(t.get("size") or 0.0 for t in sells)
    net_flow = buy_size - sell_size
    total_flow = buy_size + sell_size

    buy_sell_ratio = buy_size / sell_size if sell_size > 0 else None

    vwap_buy = _vwap(buys)
    vwap_sell = _vwap(sells)
    price_impact = (vwap_buy - vwap_sell) if vwap_buy is not None and vwap_sell is not None else None

    timestamps = [t.get("timestamp") for t in trades if t.get("timestamp") is not None]
    trade_velocity = None
    if len(timestamps) >= 2:
        duration_hours = (max(timestamps) - min(timestamps)) / 3600.0
        if duration_hours > 0:
            trade_velocity = len(trades) / duration_hours

    flow_ratio = net_flow / total_flow if total_flow > 0 else 0.0
    sentiment_side = "YES" if net_flow >= 0 else "NO"
    sentiment_confidence = 0.5 + abs(flow_ratio) * 0.5

    return {
        "sentiment_side": sentiment_side,
        "sentiment_confidence": sentiment_confidence,
        "condition_id": event.get("condition_id"),
        "buy_count": len(buys),
        "sell_count": len(sells),
        "buy_size": buy_size,
        "sell_size": sell_size,
        "net_flow": net_flow,
        "buy_sell_ratio": buy_sell_ratio,
        "vwap_buy": vwap_buy,
        "vwap_sell": vwap_sell,
        "price_impact": price_impact,
        "trade_velocity": trade_velocity,
        "meta": {
            "source_signal_kind": _SOURCE_KIND,
            "source_signal_id": event.get("signal_id"),
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_trade_flow_derived",
        default_signal_id="signal-trade-flow-derived",
        default_signal_kind="trade_flow_derived",
        builder=_build_trade_flow_derived,
    )


if __name__ == "__main__":
    main()
