use sqlx::PgPool;

const ROLE_GRANTS_SQL: &str = r#"
        DO $$
        DECLARE
            role_name TEXT;
        BEGIN
            FOREACH role_name IN ARRAY ARRAY['polybet_input', 'polybet_resolution', 'polybet_observe']
            LOOP
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
                    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', role_name);
                    EXECUTE format('GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO %I', role_name);
                    EXECUTE format(
                        'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %I',
                        role_name
                    );
                    EXECUTE format(
                        'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, USAGE ON SEQUENCES TO %I',
                        role_name
                    );
                END IF;
            END LOOP;

            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'polybet_input') THEN
                EXECUTE 'GRANT INSERT, UPDATE, DELETE ON TABLE
                    markets,
                    market_snapshots,
                    events,
                    event_snapshots,
                    gamma_markets,
                    gamma_events,
                    manager_sync_status,
                    market_new_question_emits,
                    launcher_status,
                    launcher_instances,
                    market_outcomes
                TO polybet_input';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE
                    market_snapshots_id_seq,
                    event_snapshots_id_seq
                TO polybet_input';
                EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, DELETE ON TABLES TO polybet_input';
                EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO polybet_input';
            END IF;

            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'polybet_resolution') THEN
                EXECUTE 'GRANT INSERT, UPDATE, DELETE ON TABLE
                    experiment_predictions,
                    market_outcomes,
                    market_tracking,
                    error_events
                TO polybet_resolution';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE
                    experiment_predictions_id_seq,
                    error_events_id_seq
                TO polybet_resolution';
                EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, DELETE ON TABLES TO polybet_resolution';
                EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO polybet_resolution';
            END IF;
        END
        $$;
        "#;

pub async fn run_migrations(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS markets (
            market_id TEXT PRIMARY KEY,
            slug TEXT NULL,
            question TEXT NULL,
            start_date_raw TEXT NULL,
            end_date_raw TEXT NULL,
            volume DOUBLE PRECISION NULL,
            liquidity DOUBLE PRECISION NULL,
            active BOOLEAN NULL,
            closed BOOLEAN NULL,
            accepting_orders BOOLEAN NULL,
            is_eligible BOOLEAN NOT NULL DEFAULT false,
            payload_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id BIGSERIAL PRIMARY KEY,
            market_id TEXT NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
            observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            slug TEXT NULL,
            question TEXT NULL,
            start_date_raw TEXT NULL,
            end_date_raw TEXT NULL,
            volume DOUBLE PRECISION NULL,
            liquidity DOUBLE PRECISION NULL,
            active BOOLEAN NULL,
            closed BOOLEAN NULL,
            accepting_orders BOOLEAN NULL,
            is_eligible BOOLEAN NOT NULL DEFAULT false,
            payload_json JSONB NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_market_snapshots_market_observed_at
        ON market_snapshots (market_id, observed_at DESC);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            slug TEXT NULL,
            title TEXT NULL,
            start_date_raw TEXT NULL,
            end_date_raw TEXT NULL,
            active BOOLEAN NULL,
            payload_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS event_snapshots (
            id BIGSERIAL PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
            observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            slug TEXT NULL,
            title TEXT NULL,
            start_date_raw TEXT NULL,
            end_date_raw TEXT NULL,
            active BOOLEAN NULL,
            payload_json JSONB NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_event_snapshots_event_observed_at
        ON event_snapshots (event_id, observed_at DESC);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS gamma_markets (
            market_id TEXT PRIMARY KEY,
            slug TEXT NULL,
            question TEXT NULL,
            start_date_raw TEXT NULL,
            end_date_raw TEXT NULL,
            volume DOUBLE PRECISION NULL,
            liquidity DOUBLE PRECISION NULL,
            active BOOLEAN NULL,
            closed BOOLEAN NULL,
            accepting_orders BOOLEAN NULL,
            is_eligible BOOLEAN NOT NULL DEFAULT false,
            payload_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_gamma_markets_updated_at
        ON gamma_markets (updated_at DESC);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_gamma_markets_open_recent
        ON gamma_markets (end_date_raw, updated_at DESC)
        WHERE COALESCE(active, false) = true AND COALESCE(closed, false) = false;
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS gamma_events (
            event_id TEXT PRIMARY KEY,
            slug TEXT NULL,
            title TEXT NULL,
            start_date_raw TEXT NULL,
            end_date_raw TEXT NULL,
            active BOOLEAN NULL,
            payload_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_gamma_events_updated_at
        ON gamma_events (updated_at DESC);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS manager_sync_status (
            id INTEGER PRIMARY KEY,
            last_success TIMESTAMPTZ NULL,
            markets_synced BIGINT NOT NULL DEFAULT 0,
            eligible_markets BIGINT NOT NULL DEFAULT 0,
            zero_eligible_streak BIGINT NOT NULL DEFAULT 0,
            events_synced BIGINT NOT NULL DEFAULT 0,
            data_api_health TEXT NULL,
            last_error TEXT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query("ALTER TABLE markets ADD COLUMN IF NOT EXISTS start_date_raw TEXT NULL")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE markets ADD COLUMN IF NOT EXISTS accepting_orders BOOLEAN NULL")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE markets ADD COLUMN IF NOT EXISTS is_eligible BOOLEAN NOT NULL DEFAULT false")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS start_date_raw TEXT NULL")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS accepting_orders BOOLEAN NULL")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS is_eligible BOOLEAN NOT NULL DEFAULT false")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE gamma_markets ADD COLUMN IF NOT EXISTS start_date_raw TEXT NULL")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE gamma_markets ADD COLUMN IF NOT EXISTS accepting_orders BOOLEAN NULL")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE gamma_markets ADD COLUMN IF NOT EXISTS is_eligible BOOLEAN NOT NULL DEFAULT false")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE manager_sync_status ADD COLUMN IF NOT EXISTS eligible_markets BIGINT NOT NULL DEFAULT 0")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE manager_sync_status ADD COLUMN IF NOT EXISTS zero_eligible_streak BIGINT NOT NULL DEFAULT 0")
        .execute(pool)
        .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_markets_eligible_updated_at
        ON markets (updated_at DESC)
        WHERE is_eligible = true;
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS launcher_status (
            id INTEGER PRIMARY KEY,
            last_success TIMESTAMPTZ NULL,
            desired_instances BIGINT NOT NULL DEFAULT 0,
            running_instances BIGINT NOT NULL DEFAULT 0,
            launched_last_run BIGINT NOT NULL DEFAULT 0,
            stopped_last_run BIGINT NOT NULL DEFAULT 0,
            last_error TEXT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS launcher_instances (
            container_name TEXT PRIMARY KEY,
            experiment_name TEXT NOT NULL,
            seed BIGINT NULL,
            status TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS market_outcomes (
            market_id TEXT PRIMARY KEY REFERENCES markets(market_id),
            resolution_status TEXT NOT NULL,
            winning_side TEXT NULL,
            resolved_at TIMESTAMPTZ NULL,
            source TEXT NULL,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT market_outcomes_winning_side_chk CHECK (
                winning_side IS NULL OR winning_side IN ('YES', 'NO')
            )
        );
        "#,
    )
    .execute(pool)
    .await?;
    // market_outcomes is currently input-owned:
    // - input writes gamma-derived outcome updates during sync

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_market_outcomes_resolved_at
        ON market_outcomes (resolved_at DESC);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS market_tracking (
            market_id TEXT PRIMARY KEY REFERENCES markets(market_id) ON DELETE CASCADE,
            next_check_at TIMESTAMPTZ NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_checked_at TIMESTAMPTZ NULL,
            last_resolution_status TEXT NULL,
            last_error TEXT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_market_tracking_next_check_at
        ON market_tracking (next_check_at ASC);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS signal_outputs (
            id BIGSERIAL PRIMARY KEY,
            signal_id TEXT NOT NULL,
            signal_kind TEXT NOT NULL,
            market_id TEXT NOT NULL,
            market_question TEXT NULL,
            sentiment_side TEXT NOT NULL,
            sentiment_confidence DOUBLE PRECISION NOT NULL,
            meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS experiment_predictions (
            id BIGSERIAL PRIMARY KEY,
            experiment_id TEXT NOT NULL,
            strategy TEXT NOT NULL,
            market_id TEXT NOT NULL,
            market_question TEXT NULL,
            market_end_date_raw TEXT NULL,
            side TEXT NOT NULL,
            confidence DOUBLE PRECISION NOT NULL,
            horizon_minutes INTEGER NULL,
            rationale TEXT NULL,
            meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS market_new_question_emits (
            market_id TEXT PRIMARY KEY REFERENCES markets(market_id) ON DELETE CASCADE,
            first_emitted_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS error_events (
            id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            service TEXT NOT NULL,
            error_code TEXT NOT NULL,
            message TEXT NOT NULL,
            context JSONB NOT NULL DEFAULT '{}'::jsonb,
            source_subject TEXT NULL,
            raw_payload_json JSONB NULL,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_error_events_recorded_at
        ON error_events (recorded_at DESC);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(ROLE_GRANTS_SQL)
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ROLE_GRANTS_SQL;

    #[test]
    fn resolution_role_grants_include_market_outcomes_writes() {
        assert!(ROLE_GRANTS_SQL.contains("TO polybet_resolution"));
        assert!(ROLE_GRANTS_SQL.contains("market_outcomes"));
        assert!(ROLE_GRANTS_SQL.contains("GRANT INSERT, UPDATE, DELETE ON TABLE"));
    }
}
