from signal_common import _extract_payload, _pick, run_signal_entrypoint


def _build_market_metadata(event: dict[str, object]) -> tuple[dict[str, object] | None, str]:
    payload = _extract_payload(event)
    event_id = payload.get("eventId") or payload.get("event_id")
    if event_id is None:
        event_id = event.get("event_id")

    metadata = {
        "event_id": event_id,
        "slug": event.get("slug"),
        "start_date_raw": event.get("start_date_raw"),
        "end_date_raw": event.get("end_date_raw"),
        "active": event.get("active"),
        "closed": event.get("closed"),
        "accepting_orders": event.get("accepting_orders"),
        "volume": event.get("volume"),
        "liquidity": event.get("liquidity"),
        "condition_id": _pick(payload, ["conditionId", "condition_id"]),
        "question_id": _pick(payload, ["questionID", "questionId", "question_id"]),
        "enable_order_book": _pick(payload, ["enableOrderBook", "enable_order_book"]),
        "meta": {
            "source_event": "market.new_question.v1",
            "source_fields": [
                "event_id",
                "slug",
                "start_date_raw",
                "end_date_raw",
                "active",
                "closed",
                "accepting_orders",
                "volume",
                "liquidity",
                "payload.conditionId",
                "payload.questionID",
                "payload.enableOrderBook",
            ],
        },
    }

    return metadata, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_market_metadata",
        default_signal_id="signal-market-metadata",
        default_signal_kind="market_metadata",
        builder=_build_market_metadata,
    )


if __name__ == "__main__":
    main()
