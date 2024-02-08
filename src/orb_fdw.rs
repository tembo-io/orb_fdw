#![allow(clippy::module_inception)]
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use std::num::ParseIntError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrbFdwError {
    #[error("invalid service account key: {0}")]
    InvalidServiceAccount(#[from] std::io::Error),

    #[error("Orb object '{0}' not implemented")]
    ObjectNotImplemented(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("invalid timestamp format: {0}")]
    InvalidTimestampFormat(String),

    #[error("invalid Orb response: {0}")]
    InvalidResponse(String),

    #[error("invalid api_key header")]
    InvalidApiKeyHeader,

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("`limit` option must be an integer: {0}")]
    LimitOptionParseError(#[from] ParseIntError),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),
}

impl From<OrbFdwError> for ErrorReport {
    fn from(value: OrbFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

pub type _OrbFdwResult<T> = Result<T, OrbFdwError>;
