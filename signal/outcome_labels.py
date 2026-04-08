from typing import Any

from signal_common import _extract_payload, _parse_json_array, run_signal_entrypoint


def _build_outcome_labels(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    payload = _extract_payload(event)
    outcomes = _parse_json_array(payload.get("outcomes"))
    if not outcomes:
        return None, "missing_outcomes"

    normalized = [str(item) for item in outcomes]
    return {
        "sentiment_side": "YES",
        "sentiment_confidence": 0.5,
        "outcomes": normalized,
        "outcome_count": len(normalized),
        "meta": {
            "source_event": "market.new_question.v1",
            "source_field": "payload.outcomes",
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_outcome_labels",
        default_signal_id="signal-outcome-labels",
        default_signal_kind="outcome_labels",
        builder=_build_outcome_labels,
    )


if __name__ == "__main__":
    main()
