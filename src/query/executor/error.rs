use crate::catalog::error::CatalogError;

/// Represents errors that can occur during query execution.
#[derive(Debug)]
pub enum ExecutionError {
    /// Errors related to catalog operations during execution (e.g., table lookup).
    Catalog(CatalogError),
}
