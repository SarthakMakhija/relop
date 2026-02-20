use crate::types::column_type::ColumnType;

/// Represents errors that occur during schema validation or manipulation.
///
/// # Examples
///
/// ```
/// use relop::schema::error::SchemaError;
///
/// let error = SchemaError::DuplicateColumnName("id".to_string());
/// println!("{:?}", error);
/// ```
#[derive(Debug, PartialEq)]
pub enum SchemaError {
    /// A primary key column name is duplicated.
    DuplicatePrimaryKeyColumnName(String),
    /// A column name is duplicated in the table definition.
    DuplicateColumnName(String),
    /// The specified primary key column was not found in the table columns.
    PrimaryKeyColumnNotFound(String),
    /// A primary key has already been defined for this table.
    PrimaryKeyAlreadyDefined,
    /// No columns specified for the primary key.
    EmptyPrimaryKeyColumns,
    /// The number of columns does not match the expected count.
    ColumnCountMismatch {
        /// The expected number of columns.
        expected: usize,
        /// The actual number of columns provided.
        actual: usize,
    },
    /// The type of column does not match the expected type.
    ColumnTypeMismatch {
        /// The name of the column with the type mismatch.
        column: String,
        /// The expected data type.
        expected: ColumnType,
        /// The actual data type encountered.
        actual: ColumnType,
    },
    /// The column name is ambiguous because it matches multiple columns.
    AmbiguousColumnName(String),
    /// The table name or alias used as a prefix does not exist in the current scope.
    TableAliasNotFound(String),
}
