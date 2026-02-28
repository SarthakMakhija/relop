use regex::Error;

/// `PlanningError` represents errors that occur during the logical planning phase.
#[derive(Debug, PartialEq)]
pub enum PlanningError {
    /// Indicates that a provided regular expression in a LIKE clause is invalid.
    InvalidRegex(String),
    /// Indicates that a column reference could not be resolved.
    ColumnNotFound(String),
    /// Indicates a catalog-related error during planning (e.g., table not found).
    Catalog(crate::catalog::error::CatalogError),
}

impl From<Error> for PlanningError {
    fn from(error: Error) -> Self {
        PlanningError::InvalidRegex(error.to_string())
    }
}
