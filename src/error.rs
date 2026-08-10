use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    error_type: &'static str,
    code: &'static str,
    param: Option<String>,
    message: String,
}

impl ApiError {
    pub(crate) fn invalid(param: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error_type: "invalid_request_error",
            code: "invalid_request_error",
            param: Some(param.into()),
            message: message.into(),
        }
    }

    pub(crate) fn invalid_api_key() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            error_type: "invalid_request_error",
            code: "invalid_api_key",
            param: None,
            message: "Invalid API key".to_owned(),
        }
    }

    pub(crate) fn quota_exceeded() -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            error_type: "invalid_request_error",
            code: "weekly_quota_exceeded",
            param: None,
            message: "The configured weekly quota has been exceeded".to_owned(),
        }
    }

    pub(crate) fn gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            error_type: "upstream_error",
            code: "upstream_error",
            param: None,
            message: message.into(),
        }
    }

    pub(crate) fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "server_error",
            code: "internal_error",
            param: None,
            message: "Internal server error".to_owned(),
        }
    }

    pub(crate) fn shutdown() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error_type: "server_error",
            code: "server_shutting_down",
            param: None,
            message: "Server is shutting down".to_owned(),
        }
    }

    pub(crate) fn body(&self) -> Value {
        json!({
            "error": {
                "message": self.message,
                "type": self.error_type,
                "param": self.param,
                "code": self.code,
            }
        })
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body())).into_response()
    }
}

pub(crate) fn websocket_error(
    code: &'static str,
    param: Option<&str>,
    message: impl Into<String>,
) -> Value {
    json!({
        "type": "error",
        "code": code,
        "param": param,
        "message": message.into(),
    })
}
