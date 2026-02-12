use thiserror::Error;

pub type Result<T> = std::result::Result<T, K8Error>;

#[derive(Debug, Error)]
pub enum K8Error {
    #[error("validation error: {0}")]
    Validation(String),

    #[error("recipe format error: {0}")]
    RecipeFormat(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
