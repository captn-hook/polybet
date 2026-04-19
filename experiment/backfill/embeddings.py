#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "httpx>=0.27.0",
#   "psycopg[binary]>=3.2.0",
# ]
# ///
"""Backfill question embeddings for all markets that don't have one yet.

Usage:
    uv run experiment/backfill/embeddings.py
    uv run experiment/backfill/embeddings.py --batch-size 100 --model nomic-embed-text
    DATABASE_URL=postgresql://... uv run experiment/backfill/embeddings.py
"""

import argparse
import os
import time

import httpx
import psycopg
from psycopg.rows import dict_row

_DEFAULT_DSN = (
    "postgresql://polybet_observe:polybet_observe_dev_password@localhost:5432/polybet"
)
_DEFAULT_OLLAMA = "http://localhost:11434"
_DEFAULT_MODEL = "nomic-embed-text"
_DEFAULT_BATCH = 50


def _embed_batch(client: httpx.Client, texts: list[str], model: str) -> list[list[float]]:
    resp = client.post(
        "/api/embed",
        json={"model": model, "input": texts},
        timeout=120.0,
    )
    resp.raise_for_status()
    return resp.json()["embeddings"]


def backfill(dsn: str, ollama_base: str, model: str, batch_size: int) -> None:
    # autocommit=True so every INSERT commits immediately — no silent rollback on crash
    with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as db:
        # Count pending
        row = db.execute("""
            SELECT count(*) AS n FROM markets m
            WHERE m.question IS NOT NULL AND m.question != ''
              AND NOT EXISTS (
                  SELECT 1 FROM market_embeddings e
                  WHERE e.market_id = m.market_id AND e.model = %s
              )
        """, (model,)).fetchone()
        total = row["n"]
        print(f"Pending: {total} markets to embed (model={model}, batch={batch_size})")
        if total == 0:
            print("Nothing to do.")
            return

        with httpx.Client(base_url=ollama_base) as http:
            done = 0
            errors = 0
            t0 = time.monotonic()

            while True:
                rows = db.execute("""
                    SELECT m.market_id, m.question FROM markets m
                    WHERE m.question IS NOT NULL AND m.question != ''
                      AND NOT EXISTS (
                          SELECT 1 FROM market_embeddings e
                          WHERE e.market_id = m.market_id AND e.model = %s
                      )
                    LIMIT %s
                """, (model, batch_size)).fetchall()

                if not rows:
                    break

                texts = [r["question"] for r in rows]
                market_ids = [r["market_id"] for r in rows]

                try:
                    embeddings = _embed_batch(http, texts, model)
                except Exception as exc:
                    print(f"  ollama error on batch: {exc} — skipping")
                    errors += len(rows)
                    time.sleep(2)
                    continue

                # Reconnect if the connection dropped (long-running script)
                try:
                    db.execute("SELECT 1")
                except psycopg.OperationalError:
                    print("  DB connection lost — reconnecting…")
                    try:
                        db.close()
                    except Exception:
                        pass
                    db = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)

                for market_id, question, embedding in zip(market_ids, texts, embeddings):
                    vec_literal = "[" + ",".join(f"{v:.8g}" for v in embedding) + "]"
                    db.execute("""
                        INSERT INTO market_embeddings
                            (market_id, market_question, model, embedding)
                        VALUES (%s, %s, %s, %s::vector)
                        ON CONFLICT (market_id, model) DO UPDATE
                            SET market_question = EXCLUDED.market_question,
                                embedding       = EXCLUDED.embedding,
                                created_at      = now()
                    """, (market_id, question, model, vec_literal))

                done += len(rows)
                elapsed = time.monotonic() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(
                    f"  {done}/{total} embedded "
                    f"({done/total:.0%})  {rate:.1f}/s  ETA {eta/60:.1f}m",
                    end="\r",
                )

        print(f"\nDone. {done} embedded, {errors} errors.")

        count_row = db.execute("SELECT count(*) AS n FROM market_embeddings").fetchone()
        n = count_row["n"]
        print(f"market_embeddings now has {n} rows.")
        if n >= 1000:
            print(
                "Tip: build the IVFFlat index as superuser:\n"
                f"  docker exec polybet-postgres-1 psql -U polybet -d polybet -c \""
                f"CREATE INDEX IF NOT EXISTS idx_market_embeddings_ivfflat "
                f"ON market_embeddings USING ivfflat (embedding vector_cosine_ops) "
                f"WITH (lists = {max(10, min(n // 50, 1000))});\""
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill question embeddings into market_embeddings")
    parser.add_argument("--batch-size", type=int, default=_DEFAULT_BATCH)
    parser.add_argument("--model", default=_DEFAULT_MODEL)
    parser.add_argument("--ollama", default=os.getenv("OLLAMA_BASE_URL", _DEFAULT_OLLAMA))
    args = parser.parse_args()

    backfill(
        dsn=os.getenv("DATABASE_URL", _DEFAULT_DSN),
        ollama_base=args.ollama,
        model=args.model,
        batch_size=args.batch_size,
    )
