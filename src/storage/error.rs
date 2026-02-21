/// Represents errors that can occur during comparison of row views.
#[derive(Debug, PartialEq)]
pub enum RowViewComparatorError {
    /// Error related unknown column during row view comparison.
    UnknownColumn(String),
}
