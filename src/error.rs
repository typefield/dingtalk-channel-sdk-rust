//! Structured errors (SPEC: types/errors.go port).

use std::fmt;

/// Structured error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    TargetRevoked,
    PermissionDenied,
    FormatError,
    RateLimited,
    SendTimeout,
    QpsLimited,
    SsrfBlocked,
    Unknown,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::TargetRevoked => "target_revoked",
            ErrorCode::PermissionDenied => "permission_denied",
            ErrorCode::FormatError => "format_error",
            ErrorCode::RateLimited => "rate_limited",
            ErrorCode::SendTimeout => "send_timeout",
            ErrorCode::QpsLimited => "qps_limited",
            ErrorCode::SsrfBlocked => "ssrf_blocked",
            ErrorCode::Unknown => "unknown",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// DingTalk OpenAPI error (Go `apiError`).
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub msg: String,
    pub body: String,
}

impl ApiError {
    pub fn is_qps_limit(&self) -> bool {
        self.status == 403 && self.code.contains("QpsLimit")
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "dingtalk api error: http={} code={} msg={}",
            self.status, self.code, self.msg
        )
    }
}

impl std::error::Error for ApiError {}

/// SDK-level error (Go `channelError` / `ChannelError` merged).
#[derive(Debug, Clone)]
pub struct ChannelError {
    pub message: String,
}

impl ChannelError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ChannelError {}

/// Unified error type for the crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Channel(#[from] ChannelError),
    #[error("{0}")]
    Api(#[from] ApiError),
    /// Classified outbound/send error with structured code.
    #[error("ChannelError(code={code}): {message}")]
    Classified { code: ErrorCode, message: String },
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Ws(Box<tokio_tungstenite::tungstenite::Error>),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Ws(Box::new(error))
    }
}

impl Error {
    pub fn channel(message: impl Into<String>) -> Self {
        Error::Channel(ChannelError::new(message))
    }

    pub fn classified(code: ErrorCode, message: impl Into<String>) -> Self {
        Error::Classified {
            code,
            message: message.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Classify an error into a structured [`Error::Classified`] (Go `types.ClassifyError`).
pub fn classify_error(err: &Error) -> Error {
    if matches!(err, Error::Classified { .. }) {
        return Error::Classified {
            code: match err {
                Error::Classified { code, .. } => *code,
                _ => unreachable!(),
            },
            message: err.to_string(),
        };
    }
    let code = classify_code(err).unwrap_or(ErrorCode::Unknown);
    Error::classified(code, err.to_string())
}

fn classify_code(err: &Error) -> Option<ErrorCode> {
    if let Error::Api(api) = err {
        if api.is_qps_limit() {
            return Some(ErrorCode::QpsLimited);
        }
        match api.status {
            403 => return Some(ErrorCode::PermissionDenied),
            400 => return Some(ErrorCode::FormatError),
            404 => return Some(ErrorCode::TargetRevoked),
            429 => return Some(ErrorCode::RateLimited),
            _ => {}
        }
    }

    let msg = err.to_string().to_lowercase();
    if msg.contains("status 429") || msg.contains("too many requests") {
        return Some(ErrorCode::RateLimited);
    }
    if msg.contains("status 401") || msg.contains("status 403") {
        return Some(ErrorCode::PermissionDenied);
    }
    if msg.contains("status 400") {
        return Some(ErrorCode::FormatError);
    }
    if msg.contains("status 404") {
        return Some(ErrorCode::TargetRevoked);
    }
    if msg.contains("timeout") || msg.contains("deadline") || msg.contains("timed out") {
        return Some(ErrorCode::SendTimeout);
    }
    None
}

/// Whether the error is retryable with exponential backoff
/// (rate-limit / timeout / unknown are retryable; format errors fail fast).
pub fn is_retryable(err: &Error) -> bool {
    match err {
        Error::Classified { code, .. } => matches!(
            code,
            ErrorCode::RateLimited
                | ErrorCode::QpsLimited
                | ErrorCode::Unknown
                | ErrorCode::SendTimeout
        ),
        _ => true,
    }
}

/// Whether the reply target has been revoked (webhook target gone).
pub fn is_reply_target_gone(err: &Error) -> bool {
    matches!(
        err,
        Error::Classified {
            code: ErrorCode::TargetRevoked,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_api_status() {
        let e = Error::Api(ApiError {
            status: 404,
            code: "x".into(),
            msg: "".into(),
            body: "".into(),
        });
        assert!(is_reply_target_gone(&classify_error(&e)));

        let e = Error::Api(ApiError {
            status: 400,
            code: "x".into(),
            msg: "".into(),
            body: "".into(),
        });
        assert!(!is_retryable(&classify_error(&e)));

        let e = Error::Api(ApiError {
            status: 403,
            code: "QpsLimit exceeded".into(),
            msg: "".into(),
            body: "".into(),
        });
        assert!(is_retryable(&classify_error(&e)));
    }
}
