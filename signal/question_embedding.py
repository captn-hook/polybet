"""Embed market question text via Ollama and store in market_embeddings.

Two parallel tasks:
  1. NATS listener  — reacts to market.new_question.v1 in real-time.
  2. DB backfill    — on startup and every BACKFILL_INTERVAL_SECONDS, finds
                      markets with questions but no embedding and catches them up.

The vector itself never goes over NATS — only the market_id / model / dim.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Any

import httpx
import psycopg

COMMON_PYTHON = Path(__file__).resolve().parent / "common" / "python"
if str(COMMON_PYTHON) not in sys.path:
    sys.path.insert(0, str(COMMON_PYTHON))

from polybet_nats import PolyNats
from signal_common import (
    _extract_market_id,
    _extract_market_question,
    _iso_now,
    emit_signal_error,
)

_SERVICE = "signal_question_embedding"
_SIGNAL_KIND = "question_embedding"
_SIGNAL_ID = os.getenv("SIGNAL_ID", "signal-question-embedding")
_NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")
_SOURCE_TOPIC = os.getenv("MARKET_NEW_QUESTION_TOPIC", "market.new_question.v1")
_OUTPUT_TOPIC = os.getenv("SIGNAL_OUTPUT_TOPIC", "signal.computed.v1")
_OLLAMA_BASE = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434").rstrip("/")
_EMBED_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
_DB_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://polybet_observe:polybet_observe_dev_password@postgres:5432/polybet",
)
_BACKFILL_INTERVAL_SECONDS = int(os.getenv("EMBEDDING_BACKFILL_INTERVAL_SECONDS", "600"))
_BACKFILL_BATCH_SIZE = int(os.getenv("EMBEDDING_BACKFILL_BATCH_SIZE", "50"))


async def _get_embedding(client: httpx.AsyncClient, text: str) -> list[float]:
    resp = await client.post(
        f"{_OLLAMA_BASE}/api/embed",
        json={"model": _EMBED_MODEL, "input": text},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()["embeddings"][0]


async def _upsert_embedding(
    db: psycopg.AsyncConnection,
    market_id: str,
    question: str,
    model: str,
    embedding: list[float],
) -> None:
    vec_literal = "[" + ",".join(f"{v:.8g}" for v in embedding) + "]"
    await db.execute(
        """
        INSERT INTO market_embeddings (market_id, market_question, model, embedding)
        VALUES (%s, %s, %s, %s::vector)
        ON CONFLICT (market_id, model) DO UPDATE
            SET market_question = EXCLUDED.market_question,
                embedding       = EXCLUDED.embedding,
                created_at      = now()
        """,
        (market_id, question, model, vec_literal),
    )
    await db.commit()


async def _embed_and_publish(
    http: httpx.AsyncClient,
    db: psycopg.AsyncConnection,
    nats: PolyNats,
    market_id: str,
    question: str,
    source: str,
) -> bool:
    """Embed one market question, upsert to DB, and publish metadata signal.

    Returns True on success, False on any error (caller decides whether to skip or retry).
    """
    try:
        embedding = await _get_embedding(http, question)
    except Exception as exc:
        print(f"[{_SERVICE}] ollama error market_id={market_id} source={source}: {exc}")
        return False

    try:
        await _upsert_embedding(db, market_id, question, _EMBED_MODEL, embedding)
    except Exception as exc:
        print(f"[{_SERVICE}] db error market_id={market_id} source={source}: {exc}")
        return False

    dim = len(embedding)
    try:
        await nats.publish_json(
            f"{_OUTPUT_TOPIC}.{_SIGNAL_KIND}",
            {
                "event_type": "signal.computed.v1",
                "emitted_at": _iso_now(),
                "signal_id": _SIGNAL_ID,
                "signal_kind": _SIGNAL_KIND,
                "market_id": market_id,
                "market_question": question,
                "status": "ok",
                "reason": source,
                "model": _EMBED_MODEL,
                "embedding_dim": dim,
            },
        )
    except Exception as exc:
        print(f"[{_SERVICE}] nats publish error market_id={market_id}: {exc}")
        # Embedding was saved — non-fatal.

    print(f"[{_SERVICE}] embedded market_id={market_id} dim={dim} source={source}")
    return True


async def _nats_listener(
    nats: PolyNats,
    http: httpx.AsyncClient,
    db: psycopg.AsyncConnection,
) -> None:
    """Subscribe to market.new_question.v1 and embed in real-time."""
    from nats import errors as nats_errors

    sub = await nats.subscribe(_SOURCE_TOPIC)
    print(f"[{_SERVICE}] nats listener ready source={_SOURCE_TOPIC}")

    while True:
        try:
            msg = await sub.next_msg(timeout=60)
        except nats_errors.TimeoutError:
            continue
        except Exception as exc:
            print(f"[{_SERVICE}] nats recv error: {exc}")
            await asyncio.sleep(1)
            continue

        event = PolyNats.decode_json(msg)
        market_id = _extract_market_id(event)
        question = _extract_market_question(event)
        if not market_id or not question:
            continue

        ok = await _embed_and_publish(http, db, nats, market_id, str(question), "nats")
        if not ok:
            # Reconnect DB on error in case the connection is broken.
            try:
                await db.close()
            except Exception:
                pass
            db = await psycopg.AsyncConnection.connect(_DB_DSN)


async def _backfill_loop(
    nats: PolyNats,
    http: httpx.AsyncClient,
) -> None:
    """On startup and every BACKFILL_INTERVAL_SECONDS, embed markets with no embedding."""
    while True:
        db: psycopg.AsyncConnection | None = None
        try:
            db = await psycopg.AsyncConnection.connect(_DB_DSN)

            rows = await db.execute(
                """
                SELECT m.market_id, m.question
                FROM markets m
                WHERE m.question IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM market_embeddings e
                      WHERE e.market_id = m.market_id AND e.model = %s
                  )
                ORDER BY m.updated_at DESC
                LIMIT %s
                """,
                (_EMBED_MODEL, _BACKFILL_BATCH_SIZE),
            )
            pending = await rows.fetchall()

            if pending:
                print(f"[{_SERVICE}] backfill: {len(pending)} markets without embeddings")
                for market_id, question in pending:
                    await _embed_and_publish(http, db, nats, market_id, question, "backfill")
                    # Small yield so real-time NATS messages aren't starved.
                    await asyncio.sleep(0.1)
            else:
                print(f"[{_SERVICE}] backfill: all markets have embeddings")

        except Exception as exc:
            print(f"[{_SERVICE}] backfill error: {exc}")
        finally:
            if db is not None:
                try:
                    await db.close()
                except Exception:
                    pass

        await asyncio.sleep(_BACKFILL_INTERVAL_SECONDS)


async def run() -> None:
    nats = await PolyNats.connect(_NATS_URL)

    print(
        f"[{_SERVICE}] starting ollama={_OLLAMA_BASE} model={_EMBED_MODEL} "
        f"backfill_interval={_BACKFILL_INTERVAL_SECONDS}s batch={_BACKFILL_BATCH_SIZE}"
    )

    async with httpx.AsyncClient() as http:
        db = await psycopg.AsyncConnection.connect(_DB_DSN)
        try:
            await asyncio.gather(
                _nats_listener(nats, http, db),
                _backfill_loop(nats, http),
            )
        finally:
            await db.close()


if __name__ == "__main__":
    nats_url = _NATS_URL
    try:
        asyncio.run(run())
    except Exception as exc:
        import traceback
        print(f"[{_SERVICE}] fatal: {exc}", file=sys.stderr)
        traceback.print_exc()
        asyncio.run(
            emit_signal_error(
                _SERVICE,
                nats_url,
                "signal.runtime.crash",
                str(exc),
                {"traceback": traceback.format_exc()},
            )
        )
        raise
