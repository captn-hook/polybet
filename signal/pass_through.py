from typing import Any

from signal_common import emit_signal_error, run_signal_entrypoint


def _build_pass_through(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    return {
        "sentiment_side": "YES",
        "sentiment_confidence": 0.5,
        "meta": {
            "pass_through": True,
            "source_event": "market.new_question.v1",
        },
    }, "pass_through"


async def _emit_signal_error(nats_url: str, error_code: str, message: str, context: dict[str, Any]) -> None:
    await emit_signal_error("signal_pass_through", nats_url, error_code, message, context)


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_pass_through",
        default_signal_id="signal-pass-through",
        default_signal_kind="pass_through",
        builder=_build_pass_through,
    )


if __name__ == "__main__":
    main()
