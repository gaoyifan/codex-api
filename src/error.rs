use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    error_type: String,
    code: String,
    param: Option<String>,
    message: String,
}

impl ApiError {
    pub(crate) fn session_busy() -> Self {
        Self {
            status: StatusCode::CONFLICT,
            error_type: "invalid_request_error".to_owned(),
            code: "session_busy".to_owned(),
            param: None,
            message: "This session already has a request in progress".to_owned(),
        }
    }

    pub(crate) fn invalid(param: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error_type: "invalid_request_error".to_owned(),
            code: "invalid_request_error".to_owned(),
            param: Some(param.into()),
            message: message.into(),
        }
    }

    pub(crate) fn invalid_api_key() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            error_type: "invalid_request_error".to_owned(),
            code: "invalid_api_key".to_owned(),
            param: None,
            message: "Invalid API key".to_owned(),
        }
    }

    pub(crate) fn quota_exceeded() -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            error_type: "invalid_request_error".to_owned(),
            code: "weekly_quota_exceeded".to_owned(),
            param: None,
            message: "The configured weekly quota has been exceeded".to_owned(),
        }
    }

    pub(crate) fn model_not_found(model: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error_type: "invalid_request_error".to_owned(),
            code: "model_not_found".to_owned(),
            param: Some("model".to_owned()),
            message: format!("The model {model:?} does not exist or is not available"),
        }
    }

    pub(crate) fn gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            error_type: "upstream_error".to_owned(),
            code: "upstream_error".to_owned(),
            param: None,
            message: message.into(),
        }
    }

    pub(crate) fn upstream(error: &Value) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            error_type: error["type"]
                .as_str()
                .unwrap_or("upstream_error")
                .to_owned(),
            code: error["code"]
                .as_str()
                .unwrap_or("upstream_error")
                .to_owned(),
            param: error["param"].as_str().map(str::to_owned),
            message: error["message"]
                .as_str()
                .unwrap_or("Upstream request failed")
                .to_owned(),
        }
    }

    pub(crate) fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "server_error".to_owned(),
            code: "internal_error".to_owned(),
            param: None,
            message: "Internal server error".to_owned(),
        }
    }

    pub(crate) fn shutdown() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error_type: "server_error".to_owned(),
            code: "server_shutting_down".to_owned(),
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
