use crate::state::AppState;
use crate::error::BridgeError;
use tracing::instrument;
use sqlx::Row;

const MAX_ROWS: usize = 500;

fn is_read_only(sql: &str) -> bool {
    let normalized = sql.trim().to_uppercase();
    normalized.starts_with("SELECT") || normalized.starts_with("WITH")
}

// ─────────────────────────────────────────
// Outil générique
// ─────────────────────────────────────────

#[instrument(skip(state), fields(sql = %sql))]
pub async fn sql_read_query(state: &AppState, sql: &str) -> Result<String, BridgeError> {
    if sql.is_empty() {
        return Ok("Query is empty".into());
    }
    if !is_read_only(sql) {
        return Err(BridgeError::Api(
            "Read-only mode: only SELECT and WITH...SELECT queries are allowed.".into(),
        ));
    }
    let wrapped = format!("SELECT row_to_json(q) AS r FROM ({sql}) q LIMIT {MAX_ROWS}");
    let rows: Vec<serde_json::Value> = sqlx::query_scalar::<_, serde_json::Value>(&wrapped)
        .fetch_all(&state.pool)
        .await?;

    let count = rows.len();
    let json = serde_json::to_string_pretty(&serde_json::Value::Array(rows))
        .unwrap_or_else(|_| "[]".into());

    Ok(if count == MAX_ROWS {
        format!("{json}\n\n⚠️ Results truncated to {MAX_ROWS} rows.")
    } else {
        json
    })
}

#[instrument(skip(state))]
pub async fn list_tables(state: &AppState) -> Result<String, BridgeError> {
    let rows = sqlx::query(
        "SELECT table_name, table_type \
         FROM information_schema.tables \
         WHERE table_schema = 'public' ORDER BY table_name",
    )
    .fetch_all(&state.pool)
    .await?;

    let result: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let name: String = row.try_get("table_name").unwrap_or_default();
            let ttype: String = row.try_get("table_type").unwrap_or_default();
            serde_json::json!({ "table": name, "type": ttype })
        })
        .collect();

    Ok(serde_json::to_string_pretty(&result).unwrap_or_else(|_| "[]".into()))
}

#[instrument(skip(state), fields(table = %table))]
pub async fn describe_table(state: &AppState, table: &str) -> Result<String, BridgeError> {
    if table.is_empty() || !table.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(BridgeError::Api(format!("Invalid table name: '{table}'")));
    }
    let rows = sqlx::query(
        "SELECT column_name, data_type, is_nullable, column_default \
         FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(&state.pool)
    .await?;

    if rows.is_empty() {
        return Ok(format!("Table '{table}' not found in public schema."));
    }

    let result: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let col: String = row.try_get("column_name").unwrap_or_default();
            let dtype: String = row.try_get("data_type").unwrap_or_default();
            let nullable: String = row.try_get("is_nullable").unwrap_or_default();
            let default: Option<String> = row.try_get("column_default").unwrap_or(None);
            serde_json::json!({
                "column": col,
                "type": dtype,
                "nullable": nullable == "YES",
                "default": default,
            })
        })
        .collect();

    Ok(serde_json::to_string_pretty(&result).unwrap_or_else(|_| "[]".into()))
}

// ─────────────────────────────────────────
// Outils métier
// ─────────────────────────────────────────

/// UC-02 — Performance globale et par compte
#[instrument(skip(state))]
pub async fn portfolio_performance(state: &AppState) -> Result<String, BridgeError> {
    let sql = "
        SELECT
            a.name                                               AS account_name,
            a.type                                               AS account_type,
            h.ticker,
            ast.name                                             AS asset_name,
            ast.sector,
            h.quantity,
            h.avg_price,
            q.close                                              AS current_price,
            q.date                                               AS price_date,
            ROUND((h.quantity * q.close)::numeric, 2)           AS current_value,
            ROUND((h.quantity * h.avg_price)::numeric, 2)       AS cost_basis,
            ROUND(((q.close - h.avg_price) * h.quantity)::numeric, 2)
                                                                 AS unrealized_pnl,
            ROUND(((q.close - h.avg_price) / h.avg_price * 100)::numeric, 2)
                                                                 AS pnl_pct
        FROM holdings h
        JOIN accounts a   ON h.account_id = a.id
        JOIN assets   ast ON h.ticker     = ast.ticker
        JOIN LATERAL (
            SELECT close, date FROM quotes
            WHERE ticker = h.ticker
            ORDER BY date DESC LIMIT 1
        ) q ON true
        ORDER BY a.name, pnl_pct ASC
    ";
    query_to_json(state, sql).await
}

/// UC-02 / UC-07 — Positions à risque
/// drawdown_threshold : perte en % à partir de laquelle une position est signalée (ex: 10.0)
/// Le seuil sentiment est fixé à -0.5 conformément à l'architecture
#[instrument(skip(state), fields(threshold = %drawdown_threshold))]
pub async fn at_risk_positions(
    state: &AppState,
    drawdown_threshold: f64,
) -> Result<String, BridgeError> {
    if drawdown_threshold <= 0.0 || drawdown_threshold > 100.0 {
        return Err(BridgeError::Api(
            "drawdown_threshold must be between 0.0 and 100.0".into(),
        ));
    }
    // On passe le seuil via format! car sqlx ne supporte pas bind sur les
    // expressions arithmétiques dans WHERE avec des casts. La valeur est
    // validée juste au-dessus (f64 borné), pas de risque d'injection.
    let sql = format!(
        "
        SELECT
            a.name                                               AS account_name,
            h.ticker,
            ast.name                                             AS asset_name,
            ast.sector,
            h.quantity,
            h.avg_price,
            q.close                                              AS current_price,
            q.date                                               AS price_date,
            ROUND(((q.close - h.avg_price) / h.avg_price * 100)::numeric, 2)
                                                                 AS drawdown_pct,
            ROUND(sentiment.avg_score::numeric, 3)              AS avg_sentiment_7d,
            sig.rsi_14,
            sig.sma_50,
            sig.sma_200,
            CASE
                WHEN ((q.close - h.avg_price) / h.avg_price * 100) < -{drawdown_threshold}
                     THEN 'drawdown'
                WHEN sentiment.avg_score < -0.5
                     THEN 'negative_sentiment'
                ELSE 'both'
            END                                                  AS risk_reason
        FROM holdings h
        JOIN accounts a   ON h.account_id = a.id
        JOIN assets   ast ON h.ticker     = ast.ticker
        JOIN LATERAL (
            SELECT close, date FROM quotes
            WHERE ticker = h.ticker
            ORDER BY date DESC LIMIT 1
        ) q ON true
        LEFT JOIN LATERAL (
            SELECT AVG(sentiment_score) AS avg_score FROM news
            WHERE ticker = h.ticker
              AND date >= NOW() - INTERVAL '7 days'
        ) sentiment ON true
        LEFT JOIN LATERAL (
            SELECT rsi_14, sma_50, sma_200 FROM signals
            WHERE ticker = h.ticker
            ORDER BY date DESC LIMIT 1
        ) sig ON true
        WHERE
            ((q.close - h.avg_price) / h.avg_price * 100) < -{drawdown_threshold}
            OR sentiment.avg_score < -0.5
        ORDER BY drawdown_pct ASC
        "
    );
    query_to_json(state, &sql).await
}

/// UC-03 — Exposition sectorielle et concentration
#[instrument(skip(state))]
pub async fn sector_exposure(state: &AppState) -> Result<String, BridgeError> {
    let sql = "
        SELECT
            COALESCE(ast.sector, 'Unknown')                      AS sector,
            COUNT(DISTINCT h.ticker)                             AS nb_positions,
            ROUND(SUM(h.quantity * q.close)::numeric, 2)        AS total_value,
            ROUND(
                SUM(h.quantity * q.close)
                / SUM(SUM(h.quantity * q.close)) OVER () * 100,
                2
            )                                                    AS allocation_pct
        FROM holdings h
        JOIN assets ast ON h.ticker = ast.ticker
        JOIN LATERAL (
            SELECT close FROM quotes
            WHERE ticker = h.ticker
            ORDER BY date DESC LIMIT 1
        ) q ON true
        GROUP BY ast.sector
        ORDER BY total_value DESC
    ";
    query_to_json(state, sql).await
}

// ─────────────────────────────────────────
// Helper interne
// ─────────────────────────────────────────

async fn query_to_json(state: &AppState, sql: &str) -> Result<String, BridgeError> {
    let wrapped = format!("SELECT row_to_json(q) AS r FROM ({sql}) q LIMIT {MAX_ROWS}");
    let rows: Vec<serde_json::Value> = sqlx::query_scalar::<_, serde_json::Value>(&wrapped)
        .fetch_all(&state.pool)
        .await?;
    let count = rows.len();
    let json = serde_json::to_string_pretty(&serde_json::Value::Array(rows))
        .unwrap_or_else(|_| "[]".into());
    Ok(if count == MAX_ROWS {
        format!("{json}\n\n⚠️ Results truncated to {MAX_ROWS} rows.")
    } else {
        json
    })
}
