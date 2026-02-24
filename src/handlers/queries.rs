use crate::state::AppState;
use crate::error::BridgeError;
use tracing::instrument;
use sqlx::Row;

const MAX_ROWS: usize = 500;

fn is_read_only(sql: &str) -> bool {
    let normalized = sql.trim().to_uppercase();
    normalized.starts_with("SELECT") || normalized.starts_with("WITH")
}

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
    // Validation: alphanumeric + underscore uniquement (anti-injection)
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
