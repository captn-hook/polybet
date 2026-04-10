from typing import Any

from signal_common import _extract_market_question, emit_signal_error, run_signal_entrypoint


def _build_question(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    question = _extract_market_question(event)
    if not question:
        return None, "missing_question"
    return {"question": str(question)}, "ok"


async def _emit_signal_error(nats_url: str, error_code: str, message: str, context: dict[str, Any]) -> None:
    await emit_signal_error("signal_question", nats_url, error_code, message, context)


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_question",
        default_signal_id="signal-question",
        default_signal_kind="question",
        builder=_build_question,
    )


if __name__ == "__main__":
    main()
