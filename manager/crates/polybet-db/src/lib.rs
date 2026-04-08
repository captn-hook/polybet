use sqlx::PgPool;

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

    Ok(())
}
