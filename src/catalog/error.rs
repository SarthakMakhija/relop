use crate::schema::error::SchemaError;

/// Represents errors that can occur during catalog operations.
#[derive(Debug, PartialEq)]
pub enum CatalogError {
    /// Indicates that a table with the given name already exists.
    TableAlreadyExists(String),
    /// Indicates that a table with the given name does not exist.
    TableDoesNotExist(String),
}

/// Represents errors that can occur during data insertion.
#[derive(Debug, PartialEq)]
pub enum InsertError {
    /// Errors related to catalog operations (e.g., table not found).
    Catalog(CatalogError),
    /// Errors related to schema validation (e.g., type mismatch).
    Schema(SchemaError),
}
