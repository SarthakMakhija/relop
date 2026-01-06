#[derive(Debug, PartialEq)]
pub enum SchemaError {
    DuplicatePrimaryKeyColumnName(String),
    DuplicateColumnName(String),
    PrimaryKeyColumnNotFound(String),
    PrimaryKeyAlreadyDefined,
}