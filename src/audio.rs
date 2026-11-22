use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json,
    body::{Body, Bytes, to_bytes},
    extract::{FromRequest, Multipart, Request, State, multipart::MultipartError},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{auth::authenticate, error::ApiError, state::AppState};

pub(crate) const MAX_TRANSCRIPTION_BODY: usize = 25 * 1024 * 1024;
const MAX_TRANSCRIPT: usize = 64 * 1024;
const MAX_SPEECH_RESPONSE: usize = 16 * 1024 * 1024;
const MAX_SPEECH_CHARACTERS: usize = 4096;
const AUDIO_TIMEOUT: Duration = Duration::from_secs(55);

#[derive(Deserialize)]
struct SpeechRequest {
    input: String,
    response_format: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct Transcript {
    text: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EncodedSpeech {
    base64: String,
    content_type: String,
}

pub(crate) async fn authenticate_and_log(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let identity = authenticate(request.headers(), &state.config)?;
    let endpoint = request.uri().path().to_owned();
    let started = Instant::now();
    let response = next.run(request).await;
    let status = response.status().as_u16();
    if let Err(error) = state
        .store
        .record_audio_request(&identity.id, &endpoint, started.elapsed(), status)
        .await
    {
        tracing::warn!(%endpoint, status, %error, "failed to write audio request log");
    }
    Ok(response)
}

pub(crate) async fn voices(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authenticate(&headers, &state.config)?;
    let upstream = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => return Err(ApiError::shutdown()),
        result = state.upstream_http.voices() => {
            result.map_err(|_| ApiError::gateway("Upstream voices request failed"))?
        }
    };
    let selected = upstream.get("selected").cloned();
    let mut voices = upstream
        .get("voices")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| ApiError::gateway("Upstream voices response was invalid"))?;
    for voice in &mut voices {
        let object = voice
            .as_object_mut()
            .ok_or_else(|| ApiError::gateway("Upstream voices response was invalid"))?;
        let id = object
            .get("voice")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| ApiError::gateway("Upstream voices response was invalid"))?
            .to_owned();
        object.insert("id".to_owned(), Value::String(id));
    }
    Ok(Json(json!({
        "object": "list",
        "data": voices,
        "selected": selected,
    })))
}

pub(crate) async fn speech(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Result<Response, ApiError> {
    let Json(request) = Json::<SpeechRequest>::from_request(request, &state)
        .await
        .map_err(|error| {
            ApiError::invalid("request", error.body_text()).with_status(error.status())
        })?;
    if request.input.trim().is_empty() || request.input.chars().count() > MAX_SPEECH_CHARACTERS {
        return Err(ApiError::invalid(
            "input",
            format!("input must contain between 1 and {MAX_SPEECH_CHARACTERS} characters"),
        ));
    }
    if request.response_format.as_deref().unwrap_or("mp3") != "mp3" {
        return Err(ApiError::invalid(
            "response_format",
            "only mp3 is supported",
        ));
    }
    tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => Err(ApiError::shutdown()),
        _ = tokio::time::sleep(AUDIO_TIMEOUT) => Err(ApiError::gateway("Upstream speech timed out")),
        result = synthesize(&state, &request.input) => result,
    }
}

async fn synthesize(state: &AppState, text: &str) -> Result<Response, ApiError> {
    let upstream = state
        .upstream_http
        .speech(text)
        .await
        .map_err(|_| ApiError::gateway("Upstream speech request failed"))?;
    let upstream = check_upstream(upstream)?;
    let content_type = media_type(
        upstream
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
    )
    .to_ascii_lowercase();
    let audio = match content_type.as_str() {
        "audio/mpeg" | "audio/mp3" => read_limited(upstream, MAX_SPEECH_RESPONSE).await?,
        "application/json" => {
            let body = read_limited(upstream, MAX_SPEECH_RESPONSE).await?;
            let encoded: EncodedSpeech = serde_json::from_slice(&body)
                .map_err(|_| ApiError::gateway("Upstream speech response was invalid"))?;
            if !matches!(
                media_type(&encoded.content_type)
                    .to_ascii_lowercase()
                    .as_str(),
                "audio/mpeg" | "audio/mp3"
            ) {
                return Err(ApiError::gateway("Upstream speech response was not MP3"));
            }
            STANDARD
                .decode(encoded.base64)
                .map(Bytes::from)
                .map_err(|_| ApiError::gateway("Upstream speech response was invalid"))?
        }
        _ => return Err(ApiError::gateway("Upstream speech response was not MP3")),
    };
    if audio.is_empty() {
        return Err(ApiError::gateway("Upstream speech response was empty"));
    }
    Ok(([(CONTENT_TYPE, "audio/mpeg")], audio).into_response())
}

pub(crate) async fn transcriptions(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Result<impl IntoResponse, ApiError> {
    let mut form = Multipart::from_request(request, &state)
        .await
        .map_err(|error| ApiError::invalid("request", error.body_text()))?;
    let mut file = None;
    while let Some(field) = form.next_field().await.map_err(multipart_error)? {
        match field.name() {
            Some("file") if file.is_none() => {
                // Match the filenames and media types sent by Codex dictation.
                let (filename, mime) = match media_type(field.content_type().unwrap_or_default()) {
                    "audio/wav" => ("audio.wav", "audio/wav"),
                    "audio/mpeg" => ("audio.mp3", "audio/mpeg"),
                    "audio/mp4" | "audio/m4a" => ("audio.m4a", "audio/mp4"),
                    "audio/webm" => ("audio.webm", "audio/webm"),
                    "audio/ogg" => ("audio.ogg", "audio/ogg"),
                    _ => return Err(ApiError::invalid("file", "unsupported audio format")),
                };
                let body = field.bytes().await.map_err(multipart_error)?;
                if body.is_empty() {
                    return Err(ApiError::invalid("file", "audio file is empty"));
                }
                file = Some((body, filename, mime));
            }
            Some("file") => {
                return Err(ApiError::invalid(
                    "file",
                    "only one audio file is supported",
                ));
            }
            Some("response_format") => {
                if field.bytes().await.map_err(multipart_error)? != "json" {
                    return Err(ApiError::invalid(
                        "response_format",
                        "only json is supported",
                    ));
                }
            }
            _ => {}
        }
    }
    let (body, filename, mime) =
        file.ok_or_else(|| ApiError::invalid("file", "audio file is required"))?;
    tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => Err(ApiError::shutdown()),
        _ = tokio::time::sleep(AUDIO_TIMEOUT) => Err(ApiError::gateway("Upstream transcription timed out")),
        result = transcribe(&state, body, filename, mime) => result.map(Json),
    }
}

async fn transcribe(
    state: &AppState,
    audio: Bytes,
    filename: &'static str,
    mime: &str,
) -> Result<Transcript, ApiError> {
    let upstream = state
        .upstream_http
        .transcribe(audio, filename, mime)
        .await
        .map_err(|_| ApiError::gateway("Upstream transcription request failed"))?;
    let body = read_limited(check_upstream(upstream)?, MAX_TRANSCRIPT).await?;
    serde_json::from_slice(&body)
        .map_err(|_| ApiError::gateway("Upstream transcription response was invalid"))
}

fn multipart_error(error: MultipartError) -> ApiError {
    ApiError::invalid("request", error.body_text()).with_status(error.status())
}

fn media_type(content_type: &str) -> &str {
    content_type.split(';').next().unwrap_or_default().trim()
}

fn check_upstream(response: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let error = ApiError::gateway(format!("Upstream audio request failed (HTTP {status})"));
    Err(if status == StatusCode::TOO_MANY_REQUESTS {
        error.with_status(status)
    } else {
        error
    })
}

async fn read_limited(response: reqwest::Response, limit: usize) -> Result<Bytes, ApiError> {
    to_bytes(Body::from_stream(response.bytes_stream()), limit)
        .await
        .map_err(|_| ApiError::gateway("Failed to read upstream audio response"))
}
