#![allow(clippy::module_inception)]
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrbFdwError {
    #[error("Orb object '{0}' not implemented")]
    ObjectNotImplemented(String),

    #[error("invalid timestamp format: {0}")]
    InvalidTimestampFormat(String),

    #[error("api_key key not found")]
    ApiKeyNotFound,

    #[error("JSON serialization error: {0}")]
    JsonSerializationError(#[from] serde_json::Error),

    #[error("Missing required option: '{0}'")]
    MissingRequiredOption(String),
}

impl From<OrbFdwError> for ErrorReport {
    fn from(value: OrbFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

pub type OrbFdwResult<T> = Result<T, OrbFdwError>;
