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
use crate::handlers::queries::{
    sql_read_query, list_tables, describe_table,
    portfolio_performance, at_risk_positions, sector_exposure,
};

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

async fn sse_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
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
        if request_id.is_null() && method != "notifications/initialized" {
            return;
        }

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
                    "portfolio_performance" => portfolio_performance(&state).await,
                    "sector_exposure" => sector_exposure(&state).await,
                    "at_risk_positions" => {
                        let threshold = args
                            .and_then(|a| a.get("drawdown_threshold")?.as_f64())
                            .unwrap_or(10.0);
                        at_risk_positions(&state, threshold).await
                    }
                    _ => Err(crate::error::BridgeError::Api(
                        format!("Unknown tool: {tool_name}")
                    )),
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
                if tx.send(json_msg.clone()).is_ok() {
                    delivered = true;
                    break;
                }
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
        "serverInfo": { "name": "mcp-postgres", "version": "0.1.0" }
    })
}

fn handle_list_tools() -> Value {
    json!({ "tools": [
        {
            "name": "sql_read_query",
            "description": concat!(
                "Query the pAItrimony financial database. ",
                "Use this tool to answer any question about the user's investment portfolio: ",
                "account balances, holdings, historical stock prices (OHLCV), ",
                "technical indicators (RSI, SMA, Bollinger Bands), or news sentiment scores. ",
                "Input must be a valid SELECT query. ",
                "IMPORTANT: holdings and quotes are linked via 'isin', not 'ticker'. ",
                "For portfolio queries, prefer using view_portfolio_summary directly. ",
                "The database schema is: ",
                "accounts(id, name, type[PEA|CTO|CRYPTO|SCPI], currency, broker); ",
                "assets(isin, ticker, name, type[STOCK|ETF|CRYPTO], sector, industry, country, currency); ",
                "holdings(id, account_id, isin, quantity, avg_price, currency); ",
                "quotes(isin, date, open, high, low, close, adjusted_close, volume); ",
                "signals(ticker, date, rsi_14, sma_50, sma_200, bb_upper, bb_lower, atr_14); ",
                "news(id, ticker, date, title, url, summary, sentiment_score[-1..1]); ",
                "alerts_log(id, ticker, alert_type, message, triggered_at); ",
                "analyst_ratings(ticker, date, consensus_rating, target_price_avg, analyst_count); ",
                "corporate_events(id, ticker, event_type, event_date, value, description); ",
                "portfolio_snapshots(id, account_id, snapshot_date, total_value, daily_pnl, daily_pnl_pct); ",
                "view_portfolio_summary(account_name, isin, ticker, asset_name, quantity, avg_price, ",
                "  last_price, quote_date, current_value, unrealized_pnl, pnl_pct). ",
                "Example — portfolio with latest price: ",
                "SELECT h.isin, a.ticker, a.name, h.quantity, h.avg_price ",
                "FROM holdings h ",
                "JOIN assets a ON h.isin = a.isin ",
                "JOIN accounts acc ON h.account_id = acc.id; ",
                "Example — latest quote per asset: ",
                "SELECT q.isin, q.close, q.date FROM quotes q ",
                "WHERE q.date = (SELECT MAX(date) FROM quotes WHERE isin = q.isin);"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A valid SELECT or WITH...SELECT SQL query against the schema above."
                    }
                },
                "required": ["sql"]
            }
        },
        {
            "name": "list_tables",
            "description": concat!(
                "List all tables and views available in the financial database. ",
                "Call this first if you are unsure which tables exist."
            ),
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "describe_table",
            "description": concat!(
                "Get the column definitions of a specific table or view in the financial database. ",
                "Use this before writing a sql_read_query if you need to know ",
                "the exact column names and types. ",
                "Key tables: quotes, holdings, signals, news, view_portfolio_summary."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": concat!(
                            "Table or view name, e.g. 'quotes', 'holdings', ",
                            "'signals', 'news', 'view_portfolio_summary'"
                        )
                    }
                },
                "required": ["table"]
            }
        },
        {
            "name": "portfolio_performance",
            "description": concat!(
                "Returns the current state of the user's investment portfolio: ",
                "all positions across all accounts (PEA, CTO, Crypto, SCPI) ",
                "with current market value, cost basis, unrealized profit/loss in currency and percentage. ",
                "Uses view_portfolio_summary internally (isin-based joins). ",
                "Use this to answer questions like: ",
                "'How is my portfolio doing?', ",
                "'What are my best/worst performing positions?', ",
                "'What is my total portfolio value?'"
            ),
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "at_risk_positions",
            "description": concat!(
                "Returns positions that are currently at risk, defined as: ",
                "a loss exceeding the drawdown threshold (default: -10%) ",
                "OR a negative average news sentiment over the last 7 days (below -0.5). ",
                "Also returns RSI and moving averages (SMA50, SMA200) for each flagged position. ",
                "Note: signals are joined via ticker, news via ticker. ",
                "Use this to answer questions like: ",
                "'What positions should I be worried about?', ",
                "'Are there any alerts in my portfolio?', ",
                "'Which stocks have bad news sentiment?'"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "drawdown_threshold": {
                        "type": "number",
                        "description": "Loss percentage threshold (default: 10.0 — flags positions down more than 10%)"
                    }
                },
                "required": []
            }
        },
        {
            "name": "sector_exposure",
            "description": concat!(
                "Returns the user's portfolio allocation broken down by market sector ",
                "(Technology, Finance, Energy, etc.): number of positions per sector, ",
                "total invested value, and percentage of the total portfolio. ",
                "Joins via isin: holdings → assets. ",
                "Use this to answer questions like: ",
                "'Am I too exposed to Tech?', ",
                "'What is my sector diversification?', ",
                "'Should I rebalance my portfolio?'"
            ),
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        }
    ]})
}


fn json_error(msg: &str) -> Value {
    json!({ "isError": true, "content": [{ "type": "text", "text": msg }] })
}