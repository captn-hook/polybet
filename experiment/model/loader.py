"""Shared DB loader: resolved market signals and embeddings."""
from __future__ import annotations

import numpy as np
import psycopg

# Used by runners, model standalone, and analysis tools.
# pre_resolution_only=True filters to signals created before the market resolved,
# avoiding data-leakage in training.  Set False for backfill / plot utilities that
# want the full signal history.
_SQL_PRE_RESOLUTION = """
    SELECT DISTINCT ON (so.market_id, so.signal_kind)
      so.market_id, so.signal_kind, so.payload_json, mo.winning_side
    FROM signal_outputs so
    JOIN market_outcomes mo ON mo.market_id = so.market_id
    WHERE mo.winning_side IN ('YES', 'NO')
      AND so.signal_kind = ANY(%s)
      AND so.created_at < mo.resolved_at
    ORDER BY so.market_id, so.signal_kind, so.created_at DESC
"""

_SQL_ALL = """
    SELECT so.market_id, so.signal_kind, so.payload_json, mo.winning_side
    FROM signal_outputs so
    JOIN market_outcomes mo ON mo.market_id = so.market_id
    WHERE mo.winning_side IN ('YES', 'NO')
      AND so.signal_kind = ANY(%s)
    ORDER BY so.market_id, so.signal_kind
"""


def load_resolved_signals(
    dsn: str,
    signal_kinds: list[str],
    pre_resolution_only: bool = True,
) -> tuple[dict[str, tuple[dict, str]], dict[str, np.ndarray]]:
    """Load resolved market signals and embeddings from DB.

    Args:
        dsn: PostgreSQL connection string.
        signal_kinds: List of signal kinds to load.
        pre_resolution_only: If True, only load signals created before resolution
            (prevents data leakage). If False, load all signals.

    Returns:
        markets:    {market_id: ({signal_kind: payload_dict}, winning_side)}
        embeddings: {market_id: np.ndarray}
    """
    sql = _SQL_PRE_RESOLUTION if pre_resolution_only else _SQL_ALL

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (signal_kinds,))
            rows = cur.fetchall()
            cur.execute("SELECT market_id, embedding::float4[] FROM market_embeddings")
            raw_embeddings: dict[str, np.ndarray] = {
                r[0]: np.array(r[1], dtype=np.float32) for r in cur.fetchall()
            }

    markets: dict[str, tuple[dict, str]] = {}
    for market_id, signal_kind, payload_json, winning_side in rows:
        if market_id not in markets:
            markets[market_id] = ({}, winning_side)
        markets[market_id][0][signal_kind] = payload_json if isinstance(payload_json, dict) else {}

    return markets, raw_embeddings
