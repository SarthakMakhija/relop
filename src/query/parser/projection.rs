/// `Projection` represents the columns to be selected in a `SELECT` statement.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Projection {
    /// Select all columns (`*`).
    All,
    /// Select specific columns by name.
    Columns(Vec<String>),
}
