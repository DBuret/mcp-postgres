use std::env;
use sqlx::PgPool;
use tokio::sync::broadcast;
use tracing::info;

pub struct AppState {
    pub pool: PgPool,
    pub tx: broadcast::Sender<String>,
}

impl AppState {
    pub async fn init(tx: broadcast::Sender<String>) -> Self {
        let prefix = "MCP_PG";
        let database_url = env::var(format!("{}_DATABASE_URL", prefix))
            .unwrap_or_else(|_| "postgresql://postgres:postgres@192.168.1.49:5432/paitrimony".into());

        info!("Connecting to PostgreSQL...");
        let pool = PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to PostgreSQL");
        info!("PostgreSQL connection pool established");

        Self { pool, tx }
    }
}
