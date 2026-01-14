/// Represents errors that can occur during batch operations.
#[derive(Debug, PartialEq)]
pub enum BatchError {
    /// Indicates that a duplicate primary key was encountered in the batch.
    DuplicatePrimaryKey,
}

/// Represents errors that can occur during comparison of row views.
#[derive(Debug, PartialEq)]
pub enum RowViewComparatorError {
    /// Error related unknown column during row view comparison.
    UnknownColumn(String),
}
