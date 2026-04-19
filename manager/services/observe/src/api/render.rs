use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};

use crate::util::html_escape;

// ---------------------------------------------------------------------------
// Status helper
// ---------------------------------------------------------------------------

pub(super) fn status_class(value: &str) -> &'static str {
    match value.trim().to_ascii_lowercase().as_str() {
        "yes" | "true" | "correct" | "resolved" | "closed" => "ok",
        "no" | "false" | "incorrect" => "bad",
        _ => "warn",
    }
}

// ---------------------------------------------------------------------------
// Row builder return types
// ---------------------------------------------------------------------------

pub(super) struct BetRows {
    pub outstanding_html: String,
    pub resolved_html: String,
    pub exp_outstanding: HashMap<String, i64>,
    pub exp_resolved: HashMap<String, i64>,
}

// ---------------------------------------------------------------------------
// Row builders
// ---------------------------------------------------------------------------

pub(super) fn render_bet_rows(
    latest_bets: Vec<(
        String, String, Option<String>, String, f64, Option<DateTime<Utc>>,
        Option<String>, Option<String>, Option<bool>, Option<String>, Option<String>, Option<DateTime<Utc>>,
    )>,
) -> BetRows {
    let now = Utc::now();
    let mut outstanding_html = String::new();
    let mut resolved_html = String::new();
    let mut exp_outstanding: HashMap<String, i64> = HashMap::new();
    let mut exp_resolved: HashMap<String, i64> = HashMap::new();

    for (
        experiment_id, market_id, market_question, side, confidence, created_at,
        pred_end_raw, gm_end_raw, gm_closed, winning_side, resolution_status, resolved_at,
    ) in latest_bets
    {
        let end_raw = gm_end_raw.or(pred_end_raw);
        let end_utc = end_raw
            .as_deref()
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .map(|v| v.with_timezone(&Utc));
        let closed_flag = gm_closed.unwrap_or(false);
        let outcome_resolved = winning_side.as_deref().map(|s| matches!(s, "YES" | "NO")).unwrap_or(false);

        let end_display = end_utc.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into());
        let created_display = created_at.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into());
        let resolved_at_display = resolved_at.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into());
        let question = html_escape(market_question.as_deref().unwrap_or("-"));

        let resolve_reason = if outcome_resolved {
            "resolved"
        } else if closed_flag {
            "closed-awaiting-outcome"
        } else if end_utc.map(|ts| ts <= now).unwrap_or(false) {
            "end time passed"
        } else {
            "open"
        };
        let resolution_display = resolution_status.unwrap_or_else(|| resolve_reason.to_string());
        let outcome_display = winning_side.clone().unwrap_or_else(|| "-".into());
        let correctness = match winning_side.as_deref() {
            Some(win) if win == side => "correct",
            Some("YES") | Some("NO") => "incorrect",
            _ => "pending",
        };

        let row = format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{:.4}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td class=\"{}\">{}</td><td>{}</td><td class=\"{}\">{}</td></tr>",
            html_escape(&experiment_id), html_escape(&market_id), question,
            status_class(&side), html_escape(&side),
            confidence, end_display, created_display,
            status_class(&resolution_display), html_escape(&resolution_display),
            status_class(&outcome_display), html_escape(&outcome_display),
            resolved_at_display,
            status_class(correctness), html_escape(correctness),
        );

        if outcome_resolved {
            *exp_resolved.entry(experiment_id).or_insert(0) += 1;
            resolved_html.push_str(&row);
        } else {
            *exp_outstanding.entry(experiment_id).or_insert(0) += 1;
            outstanding_html.push_str(&row);
        }
    }

    BetRows { outstanding_html, resolved_html, exp_outstanding, exp_resolved }
}

pub(super) fn render_market_rows(
    rows: Vec<(String, Option<String>, Option<f64>, Option<f64>, Option<bool>, Option<bool>, Option<String>, Option<f64>, bool)>,
) -> String {
    let mut html = String::new();
    for (id, question, volume, liquidity, active, accepting_orders, end_date_raw, minutes_to_end, is_eligible) in rows {
        let active_text = if active.unwrap_or(false) { "true" } else { "false" };
        let accepting_text = if accepting_orders.unwrap_or(false) { "true" } else { "false" };
        let eligible_text = if is_eligible { "true" } else { "false" };
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.2}</td><td>{:.2}</td><td>{:.2}</td><td class=\"{}\">{}</td><td class=\"{}\">{}</td><td class=\"{}\">{}</td></tr>",
            html_escape(&id), html_escape(question.as_deref().unwrap_or("-")),
            html_escape(end_date_raw.as_deref().unwrap_or("-")),
            volume.unwrap_or(0.0), liquidity.unwrap_or(0.0), minutes_to_end.unwrap_or(-1.0),
            status_class(active_text), active_text,
            status_class(accepting_text), accepting_text,
            status_class(eligible_text), eligible_text,
        ));
    }
    html
}

pub(super) fn render_resolved_market_rows(
    rows: Vec<(String, Option<String>, Option<String>, Option<bool>, Option<f64>)>,
) -> String {
    let mut html = String::new();
    for (id, question, end_date_raw, closed, volume) in rows {
        let closed_text = if closed.unwrap_or(false) { "true" } else { "false" };
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{:.2}</td></tr>",
            html_escape(&id), html_escape(question.as_deref().unwrap_or("-")),
            html_escape(end_date_raw.as_deref().unwrap_or("-")),
            status_class(closed_text), closed_text,
            volume.unwrap_or(0.0),
        ));
    }
    html
}

pub(super) fn render_prediction_rows(
    rows: Vec<(String, String, String, Option<String>, f64, Option<DateTime<Utc>>)>,
) -> String {
    let mut html = String::new();
    for (experiment_id, market_id, side, market_question, confidence, created_at) in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{}</td><td>{:.4}</td><td>{}</td></tr>",
            html_escape(&experiment_id), html_escape(&market_id),
            status_class(&side), html_escape(&side),
            html_escape(market_question.as_deref().unwrap_or("-")),
            confidence,
            created_at.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into()),
        ));
    }
    html
}

pub(super) fn render_signal_rows(
    rows: Vec<(String, String, String, Option<DateTime<Utc>>)>,
) -> String {
    let mut html = String::new();
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    let mut count = 0;
    for (signal_id, market_id, signal_kind, created_at) in rows {
        let key = (signal_id.clone(), market_id.clone(), signal_kind.clone());
        if seen.contains(&key) { continue; }
        seen.insert(key);
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            html_escape(&signal_id), html_escape(&market_id), html_escape(&signal_kind),
            created_at.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into()),
        ));
        count += 1;
        if count >= 30 { break; }
    }
    html
}

pub(super) fn render_rollup_rows(
    rows: Vec<(String, i64, f64, f64, Option<DateTime<Utc>>)>,
    exp_outstanding: &HashMap<String, i64>,
    exp_resolved: &HashMap<String, i64>,
) -> String {
    let mut html = String::new();
    for (experiment_id, pred_count, avg_conf, yes_rate, last_prediction) in rows {
        let outstanding = exp_outstanding.get(&experiment_id).copied().unwrap_or(0);
        let resolved = exp_resolved.get(&experiment_id).copied().unwrap_or(0);
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{:.4}</td><td class=\"{}\">{:.2}%</td><td>{}</td></tr>",
            html_escape(&experiment_id), pred_count, outstanding, resolved, avg_conf,
            if yes_rate >= 0.5 { "ok" } else { "bad" }, yes_rate * 100.0,
            last_prediction.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into()),
        ));
    }
    html
}

pub(super) fn render_leaderboard_rows(
    rows: Vec<(String, i64, i64, f64, f64, Option<DateTime<Utc>>)>,
) -> String {
    let mut html = String::new();
    for (experiment_id, resolved_count, correct_count, accuracy, avg_confidence, last_prediction) in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{:.2}%</td><td>{:.4}</td><td>{}</td></tr>",
            html_escape(&experiment_id), resolved_count, correct_count,
            if accuracy >= 0.5 { "ok" } else { "bad" }, accuracy * 100.0,
            avg_confidence,
            last_prediction.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into()),
        ));
    }
    html
}

pub(super) fn render_queue_rows(
    topics: HashMap<String, crate::types::ObserveTopicState>,
) -> String {
    let mut rows: Vec<_> = topics.into_iter().collect();
    rows.sort_by(|a, b| b.1.count.cmp(&a.1.count));
    let mut html = String::new();
    for (topic, state) in rows.into_iter().take(50) {
        let preview: String = state.last_payload_json.unwrap_or_else(|| "-".into()).chars().take(160).collect();
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td><code>{}</code></td></tr>",
            html_escape(&topic),
            state.count,
            state.last_received_at.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into()),
            html_escape(&preview),
        ));
    }
    html
}

pub(super) fn render_launcher_rows(
    rows: Vec<(String, String, Option<i64>, String, Option<DateTime<Utc>>)>,
) -> String {
    let mut html = String::new();
    for (container_name, experiment_name, seed, status, updated_at) in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            html_escape(&container_name), html_escape(&experiment_name),
            seed.map(|s| s.to_string()).unwrap_or_else(|| "-".into()),
            html_escape(&status),
            updated_at.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "-".into()),
        ));
    }
    html
}

// ---------------------------------------------------------------------------
// Page data & template
// ---------------------------------------------------------------------------

pub(super) struct PageData {
    pub market_count: i64,
    pub event_count: i64,
    pub prediction_count: i64,
    pub distinct_experiments: i64,
    pub signal_count: i64,
    pub latest_signal_kind: String,
    pub latest_signal_market: String,
    pub latest_signal_at: Option<DateTime<Utc>>,
    pub markets_synced: i64,
    pub eligible_markets: i64,
    pub zero_eligible_streak: i64,
    pub events_synced: i64,
    pub sync_last_success: Option<DateTime<Utc>>,
    pub sync_last_error: Option<String>,
    pub desired_instances: i64,
    pub running_instances: i64,
    pub launched_last_run: i64,
    pub stopped_last_run: i64,
    pub launcher_last_success: Option<DateTime<Utc>>,
    pub launcher_last_error: Option<String>,
    pub launcher_rows_html: String,
    pub signal_rows_html: String,
    pub rollup_rows_html: String,
    pub leaderboard_rows_html: String,
    pub observe_total_events: u64,
    pub observe_topic_count: usize,
    pub queue_rows_html: String,
    pub outstanding_count: i64,
    pub resolved_count: i64,
    pub outstanding_rows_html: String,
    pub resolved_rows_html: String,
    pub prediction_rows_html: String,
    pub market_rows_html: String,
    pub resolved_market_rows_html: String,
}

pub(super) fn render_page(d: &PageData) -> String {
    // Unpack to locals so format! captures them by name.
    let market_count = d.market_count;
    let event_count = d.event_count;
    let prediction_count = d.prediction_count;
    let distinct_experiments = d.distinct_experiments;
    let signal_count = d.signal_count;
    let latest_signal_kind = html_escape(&d.latest_signal_kind);
    let latest_signal_market = html_escape(&d.latest_signal_market);
    let latest_signal_at = d.latest_signal_at.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "never".into());
    let markets_synced = d.markets_synced;
    let eligible_markets = d.eligible_markets;
    let zero_eligible_streak = d.zero_eligible_streak;
    let events_synced = d.events_synced;
    let sync_last_success = d.sync_last_success.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "never".into());
    let sync_last_error = html_escape(d.sync_last_error.as_deref().unwrap_or("none"));
    let desired_instances = d.desired_instances;
    let running_instances = d.running_instances;
    let launched_last_run = d.launched_last_run;
    let stopped_last_run = d.stopped_last_run;
    let launcher_last_success = d.launcher_last_success.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "never".into());
    let launcher_last_error = html_escape(d.launcher_last_error.as_deref().unwrap_or("none"));
    let launcher_rows_html = &d.launcher_rows_html;
    let signal_rows_html = &d.signal_rows_html;
    let rollup_rows_html = &d.rollup_rows_html;
    let leaderboard_rows_html = &d.leaderboard_rows_html;
    let observe_total_events = d.observe_total_events;
    let observe_topic_count = d.observe_topic_count;
    let queue_rows_html = &d.queue_rows_html;
    let outstanding_count = d.outstanding_count;
    let resolved_count = d.resolved_count;
    let outstanding_rows_html = &d.outstanding_rows_html;
    let resolved_rows_html = &d.resolved_rows_html;
    let prediction_rows_html = &d.prediction_rows_html;
    let market_rows_html = &d.market_rows_html;
    let resolved_market_rows_html = &d.resolved_market_rows_html;

    format!(
        r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Polybet Manager Dashboard</title>
    <style>
      body {{ font-family: system-ui, sans-serif; margin: 20px; }}
      h1, h2, h3 {{ margin: 8px 0; }}
      .cards {{ display: grid; grid-template-columns: repeat(5, minmax(140px, 1fr)); gap: 10px; }}
      .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 10px; }}
      .ok {{ background: #dff7e5; color: #1e5a2d; font-weight: 600; }}
      .bad {{ background: #fde3e3; color: #7f1d1d; font-weight: 600; }}
      .warn {{ background: #fff4d6; color: #6b4e00; font-weight: 600; }}
      td.ok, td.bad, td.warn {{ border-radius: 6px; }}
      .muted {{ color: #666; font-size: 0.9rem; }}
      table {{ width: 100%; border-collapse: collapse; margin-top: 8px; }}
      th, td {{ text-align: left; border-bottom: 1px solid #eee; padding: 7px; font-size: 0.92rem; }}
      .grid {{ display: grid; grid-template-columns: 1fr; gap: 12px; }}
      details.section {{ border: 1px solid #efefef; border-radius: 8px; padding: 6px 10px; }}
      details.section > summary {{ cursor: pointer; font-weight: 600; margin: 4px 0; }}
      .subtle {{ color: #444; font-size: 0.9rem; }}
      .pager {{ display: flex; gap: 8px; align-items: center; margin-top: 8px; }}
      .pager button {{ padding: 4px 8px; border: 1px solid #ccc; background: #fafafa; border-radius: 6px; cursor: pointer; }}
      .pager button:disabled {{ opacity: 0.5; cursor: default; }}
    </style>
    <script>
      document.addEventListener('DOMContentLoaded', () => {{
        const key = 'polybet_dashboard_sections_v1';
        let saved = {{}};
        try {{ saved = JSON.parse(localStorage.getItem(key) || '{{}}'); }} catch (_) {{}}
        document.querySelectorAll('details[data-section-id]').forEach((el) => {{
          const id = el.getAttribute('data-section-id');
          if (saved[id] !== undefined) el.open = !!saved[id];
          el.addEventListener('toggle', () => {{
            saved[id] = el.open;
            localStorage.setItem(key, JSON.stringify(saved));
          }});
        }});

        const paginate = (tableId, pageSize = 15) => {{
          const table = document.getElementById(tableId);
          if (!table) return;
          const body = table.querySelector('tbody');
          if (!body) return;
          const rows = Array.from(body.querySelectorAll('tr'));
          if (rows.length <= pageSize) return;
          let page = 0;
          const pages = Math.ceil(rows.length / pageSize);
          const pager = document.createElement('div');
          pager.className = 'pager';
          pager.innerHTML = `<button type="button">Prev</button><span></span><button type="button">Next</button>`;
          const [prev, info, next] = pager.children;
          table.parentElement.appendChild(pager);
          const render = () => {{
            const start = page * pageSize;
            rows.forEach((r, idx) => {{ r.style.display = idx >= start && idx < start + pageSize ? '' : 'none'; }});
            info.textContent = `Page ${{page + 1}} / ${{pages}}`;
            prev.disabled = page === 0;
            next.disabled = page >= pages - 1;
          }};
          prev.addEventListener('click', () => {{ if (page > 0) {{ page--; render(); }} }});
          next.addEventListener('click', () => {{ if (page < pages - 1) {{ page++; render(); }} }});
          render();
        }};

        ['tbl-launcher','tbl-leaderboard','tbl-event-queue','tbl-signals','tbl-exp-metrics',
         'tbl-bets-outstanding','tbl-bets-resolved','tbl-predictions','tbl-markets','tbl-resolved-markets']
          .forEach(id => paginate(id, 20));

        const live = new EventSource('/api/dashboard/live');
        live.addEventListener('dashboard', (ev) => {{
          try {{
            const payload = JSON.parse(ev.data || '{{}}');
            const sync = payload.sync || {{}};
            const launcher = payload.launcher || {{}};
            const setText = (id, value) => {{
              const el = document.getElementById(id);
              if (el && value != null) el.textContent = String(value);
            }};
            const setClass = (id, cls) => {{
              const el = document.getElementById(id);
              if (!el || !cls) return;
              el.classList.remove('ok','warn','bad','muted');
              if (['ok','warn','bad','muted'].includes(cls)) el.classList.add(cls);
            }};
            setText('s-sync-last-success', sync.last_success ?? 'never');
            setText('s-sync-markets-synced', sync.markets_synced ?? 0);
            setText('s-sync-eligible-markets', sync.eligible_markets ?? 0);
            setText('s-sync-zero-eligible-streak', sync.zero_eligible_streak ?? 0);
            setText('s-sync-events-synced', sync.events_synced ?? 0);
            setText('s-sync-last-error', sync.last_error ?? '-');
            setClass('s-sync-last-error', (sync.last_error && sync.last_error !== '-') ? 'bad' : 'muted');
            setText('s-launcher-last-success', launcher.last_success ?? 'never');
            setText('s-launcher-desired', launcher.desired_instances ?? 0);
            setText('s-launcher-running', launcher.running_instances ?? 0);
            setText('s-launcher-launched', launcher.launched_last_run ?? 0);
            setText('s-launcher-stopped', launcher.stopped_last_run ?? 0);
            setText('s-launcher-last-error', launcher.last_error ?? '-');
            setClass('s-launcher-last-error', (launcher.last_error && launcher.last_error !== '-') ? 'bad' : 'muted');
          }} catch (_) {{}}
        }});
        live.onerror = () => live.close();
      }});
    </script>
  </head>
  <body>
    <h1>Polybet Manager Dashboard</h1>
    <p class="muted">Live updates via server-sent events</p>

    <details class="section" data-section-id="system-overview" open>
      <summary>System Overview</summary>
      <div class="cards">
        <div class="card"><strong>Markets in DB</strong><div>{market_count}</div></div>
        <div class="card"><strong>Events in DB</strong><div>{event_count}</div></div>
        <div class="card"><strong>Total Predictions</strong><div>{prediction_count}</div></div>
        <div class="card"><strong>Active Experiments</strong><div>{distinct_experiments}</div></div>
        <div class="card"><strong>Total Signals</strong><div>{signal_count}</div></div>
      </div>
    </details>

    <details class="section" data-section-id="sync-status" open>
      <summary>Gamma/Data Sync</summary>
      <div class="cards">
        <div class="card"><strong>Last Sync Markets</strong><div id="s-sync-markets-synced">{markets_synced}</div></div>
        <div class="card"><strong>Eligible Markets</strong><div id="s-sync-eligible-markets">{eligible_markets}</div></div>
        <div class="card"><strong>Zero Eligible Streak</strong><div id="s-sync-zero-eligible-streak">{zero_eligible_streak}</div></div>
        <div class="card"><strong>Last Sync Events</strong><div id="s-sync-events-synced">{events_synced}</div></div>
        <div class="card"><strong>Last Success</strong><div id="s-sync-last-success">{sync_last_success}</div></div>
        <div class="card"><strong>Latest Signal Kind</strong><div>{latest_signal_kind}</div></div>
        <div class="card"><strong>Latest Signal Market</strong><div>{latest_signal_market}</div></div>
      </div>
      <p><strong>Sync Error:</strong> <span id="s-sync-last-error">{sync_last_error}</span></p>
    </details>

    <details class="section" data-section-id="launcher" open>
      <summary>Launcher</summary>
      <div class="cards">
        <div class="card"><strong>Desired</strong><div id="s-launcher-desired">{desired_instances}</div></div>
        <div class="card"><strong>Running</strong><div id="s-launcher-running">{running_instances}</div></div>
        <div class="card"><strong>Launched (last)</strong><div id="s-launcher-launched">{launched_last_run}</div></div>
        <div class="card"><strong>Stopped (last)</strong><div id="s-launcher-stopped">{stopped_last_run}</div></div>
        <div class="card"><strong>Last Reconcile</strong><div id="s-launcher-last-success">{launcher_last_success}</div></div>
      </div>
      <p><strong>Launcher Error:</strong> <span id="s-launcher-last-error">{launcher_last_error}</span></p>
      <table id="tbl-launcher">
        <thead><tr><th>Container</th><th>Experiment</th><th>Seed</th><th>Status</th><th>Updated</th></tr></thead>
        <tbody>{launcher_rows_html}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="signal-feed">
      <summary>Signal Feed</summary>
      <div class="cards">
        <div class="card"><strong>Latest Kind</strong><div>{latest_signal_kind}</div></div>
        <div class="card"><strong>Latest Market ID</strong><div>{latest_signal_market}</div></div>
        <div class="card"><strong>Latest Signal Time</strong><div>{latest_signal_at}</div></div>
        <div class="card"><strong>Signals Written</strong><div>{signal_count}</div></div>
      </div>
      <table id="tbl-signals">
        <thead><tr><th>Signal ID</th><th>Market ID</th><th>Signal Kind</th><th>Created</th></tr></thead>
        <tbody>{signal_rows_html}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="experiment-summary" open>
      <summary>Experiment Metrics</summary>
      <table id="tbl-exp-metrics">
        <thead><tr><th>Experiment</th><th>Predictions</th><th>Outstanding Bets</th><th>Resolved Bets</th><th>Avg Confidence</th><th>YES Rate</th><th>Last Prediction</th></tr></thead>
        <tbody>{rollup_rows_html}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="experiment-leaderboard" open>
      <summary>Experiment Leaderboard (Resolved Markets)</summary>
      <table id="tbl-leaderboard">
        <thead><tr><th>Experiment</th><th>Resolved</th><th>Correct</th><th>Accuracy</th><th>Avg Confidence</th><th>Last Prediction</th></tr></thead>
        <tbody>{leaderboard_rows_html}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="event-queue" open>
      <summary>Live Event Queue View</summary>
      <div class="cards">
        <div class="card"><strong>Total Observed Events</strong><div>{observe_total_events}</div></div>
        <div class="card"><strong>Observed Topics</strong><div>{observe_topic_count}</div></div>
      </div>
      <table id="tbl-event-queue">
        <thead><tr><th>Topic</th><th>Count</th><th>Last Received</th><th>Last Payload Preview</th></tr></thead>
        <tbody>{queue_rows_html}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="bets-outstanding" open>
      <summary>Outstanding Bets</summary>
      <p class="subtle">Latest prediction per experiment+market where market appears open.</p>
      <div class="cards">
        <div class="card"><strong>Outstanding Count</strong><div>{outstanding_count}</div></div>
        <div class="card"><strong>Resolved Count</strong><div>{resolved_count}</div></div>
      </div>
      <table id="tbl-bets-outstanding">
        <thead><tr><th>Experiment</th><th>Market</th><th>Question</th><th>Side</th><th>Confidence</th><th>End Time</th><th>Predicted At</th><th>Resolution</th><th>Winning Side</th><th>Resolved At</th><th>Result</th></tr></thead>
        <tbody>{outstanding_rows_html}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="bets-resolved">
      <summary>Resolved Bets</summary>
      <p class="subtle">Latest prediction per experiment+market where market appears closed or expired.</p>
      <table id="tbl-bets-resolved">
        <thead><tr><th>Experiment</th><th>Market</th><th>Question</th><th>Side</th><th>Confidence</th><th>End Time</th><th>Predicted At</th><th>Resolution</th><th>Winning Side</th><th>Resolved At</th><th>Result</th></tr></thead>
        <tbody>{resolved_rows_html}</tbody>
      </table>
    </details>

    <div class="grid">
      <details class="section" data-section-id="recent-predictions">
        <summary>Recent Predictions</summary>
        <table id="tbl-predictions">
          <thead><tr><th>Experiment</th><th>Market ID</th><th>Side</th><th>Question</th><th>Confidence</th><th>Created</th></tr></thead>
          <tbody>{prediction_rows_html}</tbody>
        </table>
      </details>
      <details class="section" data-section-id="recent-markets">
        <summary>Soonest Closing Open Markets</summary>
        <table id="tbl-markets">
          <thead><tr><th>Market ID</th><th>Question</th><th>End Date</th><th>Volume</th><th>Liquidity</th><th>Minutes to End</th><th>Active</th><th>Accepting</th><th>Eligible</th></tr></thead>
          <tbody>{market_rows_html}</tbody>
        </table>
      </details>
      <details class="section" data-section-id="recent-resolved-markets">
        <summary>Recent Resolved/Expired Markets</summary>
        <table id="tbl-resolved-markets">
          <thead><tr><th>Market ID</th><th>Question</th><th>End Date</th><th>Closed</th><th>Volume</th></tr></thead>
          <tbody>{resolved_market_rows_html}</tbody>
        </table>
      </details>
    </div>
  </body>
</html>"#
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::status_class;

    #[test]
    fn status_class_maps_boolean_variants() {
        assert_eq!(status_class("true"), "ok");
        assert_eq!(status_class("false"), "bad");
        assert_eq!(status_class("YES"), "ok");
        assert_eq!(status_class("NO"), "bad");
    }

    #[test]
    fn status_class_maps_pending_variants() {
        assert_eq!(status_class("pending"), "warn");
        assert_eq!(status_class("undecided"), "warn");
        assert_eq!(status_class("unknown"), "warn");
    }
}
