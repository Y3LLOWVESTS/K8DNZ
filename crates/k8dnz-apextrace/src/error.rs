use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApexError {
    #[error("apextrace format error: {0}")]
    Format(String),

    #[error("apextrace validation error: {0}")]
    Validation(String),
}

pub type Result<T> = std::result::Result<T, ApexError>;