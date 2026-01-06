#[derive(Debug, PartialEq)]
pub enum SchemaError {
    DuplicateColumnName(String),
    PrimaryKeyColumnNotFound(String),
    PrimaryKeyAlreadyDefined,
}