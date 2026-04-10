import os


def _run_backfill_db_only() -> None:
    """Write backfill signals to DB (no NATS). Fast — skips if already backfilled."""
    db_dsn = os.getenv("DATABASE_URL")
    if not db_dsn or os.getenv("BACKFILL_ENABLED", "1") != "1":
        return
    try:
        import asyncio
        from backfill import MarketBackfiller
        backfiller = MarketBackfiller(db_dsn=db_dsn, nats_client=None)

        async def _run():
            return await backfiller.run(emit_nats=False)
        result = asyncio.run(_run())
        if result.markets_processed > 0:
            print(f"[backfill] db-only: {result}")
    except Exception:
        import traceback
        print(f"[backfill] error (non-fatal): {traceback.format_exc()}")


def main() -> None:
    _run_backfill_db_only()

    module = os.getenv("EXPERIMENT_MODULE", "")
    if module == "strategies.kmeans_clustering":
        from kmeans_runner import run
    else:
        from exp_runner import run
    run()


if __name__ == "__main__":
    main()
