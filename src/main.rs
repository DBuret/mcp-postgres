mod error;
mod mcp;
mod state;
mod handlers;

use axum::{
    extract::State,
    http::{StatusCode, HeaderMap},
    response::{sse::{Event, KeepAlive, Sse}, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use std::{sync::Arc, net::SocketAddr, time::Duration};
use tokio::sync::broadcast;
use futures::stream::{self, Stream};
use tracing::{info, warn, error};
use tower_http::trace::TraceLayer;
use serde_json::{json, Value};
use crate::state::AppState;
use crate::mcp::{McpRequest, McpResponse};
use crate::handlers::queries::{sql_read_query, list_tables, describe_table};

#[tokio::main]
async fn main() {
    let log_level = std::env::var("MCP_PG_LOG").unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(log_level).init();

    let (tx, _) = broadcast::channel(100);
    let state = Arc::new(AppState::init(tx).await);

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/sse", get(sse_handler).post(messages_handler))
        .route("/messages", post(messages_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let port: u16 = std::env::var("MCP_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3001);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("🚀 MCP PostgreSQL Bridge started on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn sse_handler(State(state): State<Arc<AppState>>) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = state.tx.subscribe();
    let stream = stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(msg) => Some((Ok(Event::default().data(msg)), rx)),
            Err(_) => None,
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::new())
}

async fn messages_handler(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Json(payload): Json<McpRequest>,
) -> impl IntoResponse {
    let tx = state.tx.clone();
    let method = payload.method.clone();
    let request_id = payload.id.clone().unwrap_or(Value::Null);

    if method == "initialize" {
        info!("Handling 'initialize' via direct HTTP response");
        let result = handle_initialize();
        let response = McpResponse { jsonrpc: "2.0".into(), id: request_id, result };
        return (StatusCode::OK, Json(response)).into_response();
    }

    tokio::spawn(async move {
        if request_id.is_null() && method != "notifications/initialized" { return; }

        let result = match method.as_str() {
            "tools/list" => handle_list_tools(),
            "tools/call" => {
                let tool_name = payload.params.as_ref()
                    .and_then(|p| p.get("name")?.as_str())
                    .unwrap_or("");
                let args = payload.params.as_ref().and_then(|p| p.get("arguments"));
                let res = match tool_name {
                    "sql_read_query" => {
                        let sql = args.and_then(|a| a.get("sql")?.as_str()).unwrap_or("");
                        sql_read_query(&state, sql).await
                    }
                    "list_tables" => list_tables(&state).await,
                    "describe_table" => {
                        let table = args.and_then(|a| a.get("table")?.as_str()).unwrap_or("");
                        describe_table(&state, table).await
                    }
                    _ => Err(crate::error::BridgeError::Api(format!("Unknown tool: {tool_name}"))),
                };
                match res {
                    Ok(t) => json!({ "content": [{ "type": "text", "text": t }] }),
                    Err(e) => {
                        error!(error = %e, "Tool call failed");
                        json!({ "isError": true, "content": [{ "type": "text", "text": e.to_string() }] })
                    }
                }
            }
            "notifications/initialized" => return,
            _ => json_error(&format!("Method {method} not supported")),
        };

        let response = McpResponse { jsonrpc: "2.0".into(), id: request_id, result };
        if let Ok(json_msg) = serde_json::to_string(&response) {
            let mut delivered = false;
            for _ in 0..3 {
                if tx.send(json_msg.clone()).is_ok() { delivered = true; break; }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if !delivered {
                warn!("Could not deliver {method} via SSE (no client connected)");
            }
        }
    });

    StatusCode::ACCEPTED.into_response()
}

fn handle_initialize() -> Value {
    json!({
        "protocolVersion": "2024-11-05",
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": "mcp-postgres-bridge", "version": "0.1.0" }
    })
}

fn handle_list_tools() -> Value {
    json!({ "tools": [
        {
            "name": "sql_read_query",
            "description": "Execute a read-only SELECT query on the pAItrimony PostgreSQL database. Returns a JSON array. Only SELECT and WITH...SELECT are allowed.",
            "inputSchema": {
                "type": "object",
                "properties": { "sql": { "type": "string", "description": "A valid SELECT or WITH...SELECT SQL query" } },
                "required": ["sql"]
            }
        },
        {
            "name": "list_tables",
            "description": "List all tables in the pAItrimony database (public schema).",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "describe_table",
            "description": "Get column definitions (name, type, nullable, default) for a given table.",
            "inputSchema": {
                "type": "object",
                "properties": { "table": { "type": "string", "description": "Table name (e.g. 'quotes', 'holdings')" } },
                "required": ["table"]
            }
        }
    ]})
}

fn json_error(msg: &str) -> Value {
    json!({ "isError": true, "content": [{ "type": "text", "text": msg }] })
}
