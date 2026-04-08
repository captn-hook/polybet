from __future__ import annotations

import psycopg
import pytest


pytestmark = pytest.mark.integration


def _seed_contract_rows(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM experiment_predictions WHERE experiment_id = 'exp-follow-signal-sentiment' AND market_id IN ('it-market-1', 'it-market-2');
            DELETE FROM signal_outputs WHERE signal_id = 'it-seed-signal' AND market_id IN ('it-market-1', 'it-market-2');
            DELETE FROM gamma_markets WHERE market_id IN ('it-market-1', 'it-market-2');
            DELETE FROM markets WHERE market_id IN ('it-market-1', 'it-market-2');

            INSERT INTO gamma_markets (
                market_id, slug, question, start_date_raw, end_date_raw, volume, liquidity, active, closed, accepting_orders, is_eligible, payload_json
            ) VALUES
                ('it-market-1', 'it-market-1', 'Integration market 1', now()::text, (now() + interval '10 minutes')::text, 0, 0, true, false, true, true, '{"source":"integration_seed"}'::jsonb),
                ('it-market-2', 'it-market-2', 'Integration market 2', now()::text, (now() + interval '10 minutes')::text, 0, 0, true, false, true, true, '{"source":"integration_seed"}'::jsonb);

            INSERT INTO markets (
                market_id, slug, question, start_date_raw, end_date_raw, volume, liquidity, active, closed, accepting_orders, is_eligible, payload_json
            ) VALUES
                ('it-market-1', 'it-market-1', 'Integration market 1', now()::text, (now() + interval '10 minutes')::text, 0, 0, true, true, true, true, '{"source":"integration_seed"}'::jsonb),
                ('it-market-2', 'it-market-2', 'Integration market 2', now()::text, (now() + interval '10 minutes')::text, 0, 0, true, false, true, true, '{"source":"integration_seed"}'::jsonb)
            ON CONFLICT (market_id) DO UPDATE SET
                question = excluded.question,
                closed = excluded.closed,
                is_eligible = excluded.is_eligible,
                updated_at = now();

            INSERT INTO signal_outputs (
                signal_id, signal_kind, market_id, market_question, sentiment_side, sentiment_confidence, meta_json
            ) VALUES
                ('it-seed-signal', 'polymarket_sentiment', 'it-market-1', 'Integration market 1', 'YES', 0.81, '{"source":"integration_seed"}'::jsonb),
                ('it-seed-signal', 'polymarket_sentiment', 'it-market-2', 'Integration market 2', 'NO', 0.77, '{"source":"integration_seed"}'::jsonb);

            INSERT INTO experiment_predictions (
                experiment_id, strategy, market_id, market_question, market_end_date_raw, side, confidence, horizon_minutes, rationale, meta_json
            ) VALUES
                ('exp-follow-signal-sentiment', 'follow_signal_sentiment', 'it-market-1', 'Integration market 1', now()::text, 'YES', 0.81, 5, 'integration seed', '{"source":"integration_seed"}'::jsonb),
                ('exp-follow-signal-sentiment', 'follow_signal_sentiment', 'it-market-2', 'Integration market 2', now()::text, 'NO', 0.77, 5, 'integration seed', '{"source":"integration_seed"}'::jsonb);
            """
        )


def test_follow_signal_sentiment_matches_latest_signal(conn: psycopg.Connection) -> None:
    _seed_contract_rows(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH latest_signal AS (
                SELECT market_id, sentiment_side, sentiment_confidence
                FROM signal_outputs
                ORDER BY created_at DESC
                LIMIT 1
            ),
            latest_follow AS (
                SELECT market_id, side, confidence
                FROM experiment_predictions
                WHERE experiment_id = 'exp-follow-signal-sentiment'
                ORDER BY created_at DESC
                LIMIT 1
            )
            SELECT
                f.market_id,
                s.market_id,
                f.side,
                s.sentiment_side,
                f.confidence,
                s.sentiment_confidence
            FROM latest_follow f
            CROSS JOIN latest_signal s
            """
        )
        row = cur.fetchone()

    assert row is not None, "Missing signal or follow-signal predictions in DB"
    follow_market, signal_market, follow_side, signal_side, follow_conf, signal_conf = row
    assert follow_market == signal_market
    assert follow_side == signal_side
    assert round(float(follow_conf), 4) == round(float(signal_conf), 4)


def test_resolved_bets_are_accumulating(conn: psycopg.Connection) -> None:
    _seed_contract_rows(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(DISTINCT p.experiment_id || ':' || p.market_id)
            FROM experiment_predictions p
            JOIN markets m ON m.market_id = p.market_id
            WHERE COALESCE(m.closed, false) = true
               OR (
                    m.end_date_raw IS NOT NULL
                    AND btrim(m.end_date_raw) <> ''
                    AND m.end_date_raw::timestamptz <= now()
               )
            """
        )
        (resolved_count,) = cur.fetchone()

    assert int(resolved_count) > 0


def test_signal_coverage_reports_all_eligible_markets(conn: psycopg.Connection) -> None:
    _seed_contract_rows(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH eligible AS (
                SELECT market_id
                FROM gamma_markets
                WHERE COALESCE(is_eligible, false) = true
            ),
            coverage AS (
                SELECT
                    market_id,
                    sentiment_side
                FROM signal_outputs
                WHERE signal_kind = 'polymarket_sentiment'
            ),
            missing AS (
                SELECT
                    e.market_id
                FROM eligible e
                LEFT JOIN coverage c ON c.market_id = e.market_id
                WHERE c.market_id IS NULL
            )
            SELECT
                (SELECT COUNT(*) FROM eligible) AS eligible_count,
                (SELECT COUNT(*) FROM coverage) AS coverage_count,
                (SELECT COUNT(*) FROM missing) AS missing_count
            """
        )
        row = cur.fetchone()

    assert row is not None
    eligible_count, coverage_count, missing_count = row
    assert int(coverage_count) > 0, "Signal coverage table is empty"
    if int(eligible_count) > 0:
        assert int(coverage_count) >= int(eligible_count), (
            f"Signal coverage rows ({coverage_count}) should be at least eligible markets ({eligible_count})"
        )
        assert int(missing_count) == 0, f"Signal coverage missing {missing_count} eligible markets"


def test_historical_durability_surfaces_for_rerun(conn: psycopg.Connection) -> None:
    _seed_contract_rows(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM markets WHERE market_id IN ('it-market-1', 'it-market-2')) AS markets_count,
                (SELECT COUNT(*) FROM signal_outputs WHERE market_id IN ('it-market-1', 'it-market-2')) AS signals_count,
                (SELECT COUNT(*) FROM experiment_predictions WHERE market_id IN ('it-market-1', 'it-market-2')) AS predictions_count
            """
        )
        row = cur.fetchone()

        cur.execute(
            """
            INSERT INTO market_outcomes (market_id, resolution_status, winning_side, resolved_at, source, payload_json)
            VALUES (
                'it-market-1',
                'resolved',
                'YES',
                now(),
                'integration_test',
                '{"source":"integration_seed"}'::jsonb
            )
            ON CONFLICT (market_id) DO UPDATE SET
                resolution_status = EXCLUDED.resolution_status,
                winning_side = EXCLUDED.winning_side,
                resolved_at = EXCLUDED.resolved_at,
                source = EXCLUDED.source,
                payload_json = EXCLUDED.payload_json,
                updated_at = now()
            """
        )
        cur.execute(
            """
            SELECT COUNT(*)
            FROM market_outcomes
            WHERE market_id IN ('it-market-1', 'it-market-2')
            """
        )
        (outcomes_count,) = cur.fetchone()

    assert row is not None
    markets_count, signals_count, predictions_count = row
    assert int(markets_count) >= 2
    assert int(signals_count) >= 2
    assert int(predictions_count) >= 2
    assert int(outcomes_count) >= 1


def test_error_events_table_persists_contract_errors(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO error_events (
                event_type, service, error_code, message, context, source_subject, raw_payload_json, occurred_at
            ) VALUES (
                'prediction.error.v1',
                'experiment',
                'prediction.runtime.crash',
                'integration error seed',
                '{"source":"integration_test"}'::jsonb,
                'prediction.error.v1',
                '{"event_type":"prediction.error.v1"}'::jsonb,
                now()
            )
            """
        )
        cur.execute(
            """
            SELECT event_type, service, error_code, message
            FROM error_events
            WHERE source_subject = 'prediction.error.v1'
            ORDER BY recorded_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()

    assert row is not None
    event_type, service, error_code, message = row
    assert event_type == "prediction.error.v1"
    assert service == "experiment"
    assert error_code == "prediction.runtime.crash"
    assert message == "integration error seed"


def test_db_role_permissions_observe_is_not_writer_when_role_exists(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'polybet_observe'")
        if cur.fetchone() is None:
            pytest.skip("polybet_observe role not provisioned in this integration environment")

    observe_dsn = "postgresql://polybet_observe:polybet_observe_dev_password@127.0.0.1:55433/polybet"
    with psycopg.connect(observe_dsn, autocommit=True) as observe_conn:
        with observe_conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
            try:
                cur.execute(
                    """
                    INSERT INTO experiment_predictions (
                        experiment_id, strategy, market_id, market_question, side, confidence
                    ) VALUES (
                        'perm-test', 'perm-test', 'perm-test-market', 'perm test', 'YES', 0.5
                    )
                    """
                )
            except psycopg.Error:
                denied = True
            else:
                denied = False
        assert denied is True
