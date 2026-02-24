use axum::response::{IntoResponse, Response};
use axum::http::StatusCode;

#[derive(thiserror::Error, Debug)]
pub enum BridgeError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Backend API error: {0}")]
    Api(String),
}

impl IntoResponse for BridgeError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}
