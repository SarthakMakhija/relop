use crate::types::column_type::ColumnType;

#[derive(Debug, PartialEq)]
pub enum SchemaError {
    DuplicatePrimaryKeyColumnName(String),
    DuplicateColumnName(String),
    PrimaryKeyColumnNotFound(String),
    PrimaryKeyAlreadyDefined,
    EmptyPrimaryKeyColumns,
    ColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    ColumnTypeMismatch {
        column: String,
        expected: ColumnType,
        actual: ColumnType,
    },
}
