use crate::catalog::Catalog;
use crate::query::executor::error::ExecutionError;
use crate::storage::row::Row;
use crate::storage::row_view::RowView;
use crate::types::column_value::ColumnValue;

/// Inserts a single row into the specified table, unwrapping the result.
pub fn insert_row(catalog: &Catalog, table_name: &str, row: Row) {
    catalog.insert_into(table_name, row).unwrap();
}

/// Inserts multiple rows into the specified table, unwrapping the result.
pub fn insert_rows(catalog: &Catalog, table_name: &str, rows: Vec<Row>) {
    catalog.insert_all_into(table_name, rows).unwrap();
}

/// Creates an `OrderingKey` for ascending order.
///
/// # Examples
///
/// ```
/// use relop::asc;
/// // let key = asc!("id");
/// ```
#[macro_export]
macro_rules! asc {
    ( $x:expr ) => {
        $crate::query::parser::ordering_key::OrderingKey::ascending_by($x)
    };
}

/// Creates an `OrderingKey` for descending order.
///
/// # Examples
///
/// ```
/// use relop::desc;
/// // let key = desc!("id");
/// ```
#[macro_export]
macro_rules! desc {
    ( $x:expr ) => {
        $crate::query::parser::ordering_key::OrderingKey::descending_by($x)
    };
}

/// Asserts that the next row in the iterator matches the specified columns and values.
///
/// # Examples
///
/// ```
/// use relop::assert_next_row;
/// // assert_row!(iter, "id" => 1, "name" => "relop");
/// ```
#[macro_export]
macro_rules! assert_next_row {
    ($iterator:expr $(, $col:literal => $val:expr )* $(, ! $missing:literal )* $(,)?) => {{
        $crate::test_utils::assert_row($iterator)
            $(
                .match_column($col, $val)
            )*
            $(
                .does_not_have_column($missing)
            )*
    }};
}

/// A helper struct for asserting properties of a single `RowView`.
///
/// `RowAssertion` provides a fluent interface for checking expected values of columns
/// within a row. It is typically created via the [`assert_row`] function.
pub(crate) struct RowAssertion<'a>(RowView<'a>);

/// Creates a `RowAssertion` for the next row in the given iterator.
///
/// This function advances the iterator, unwraps the result, and returns a `RowAssertion`
/// that can be used to verify the content of the retrieved row.
///
/// # Panics
///
/// Panics if the iterator yields `None` (no more rows) or if it yields an `Err`.
pub(crate) fn assert_row<'a>(
    iterator: &'a mut dyn Iterator<Item = Result<RowView, ExecutionError>>,
) -> RowAssertion<'a> {
    let row_view = iterator.next().unwrap().unwrap();
    RowAssertion(row_view)
}

/// Asserts that there are no more rows in the iterator.
///
/// # Examples
///
/// ```
/// use relop::assert_no_more_rows;
/// // assert_no_more_rows!(iter);
/// ```
#[macro_export]
macro_rules! assert_no_more_rows {
    ($iterator:expr) => {
        $crate::test_utils::assert_no_more_rows($iterator)
    };
}

/// Asserts that there are no more rows in the iterator.
///
/// # Arguments
///
/// * `iterator` - The iterator to check.
///
/// # Panics
///
/// Panics if the iterator yields `Some` (meaning there are more rows).
pub(crate) fn assert_no_more_rows(
    iterator: &mut dyn Iterator<Item = Result<RowView, ExecutionError>>,
) {
    assert!(iterator.next().is_none(), "Expected no more rows");
}

impl RowAssertion<'_> {
    /// Asserts that a column exists and has the expected value.
    ///
    /// # Arguments
    ///
    /// * `column` - The name of the column to check.
    /// * `expected` - The expected value. `ColumnValue` implements `From` for common types,
    ///   so you can pass `10`, `"string"`, etc. directly.
    ///
    /// # Panics
    ///
    /// Panics if the column does not exist or if the value does not match.
    pub(crate) fn match_column<V: Into<ColumnValue>>(self, column: &str, expected: V) -> Self {
        let actual = self
            .0
            .column_value_by(column)
            .expect("Column lookup failed")
            .expect("Column not found");
        assert_eq!(actual, &expected.into(), "Mismatch in column '{}'", column);
        self
    }

    /// Asserts that a column does not exist in the row.
    ///
    /// # Arguments
    ///
    /// * `column` - The name of the column to check for absence.
    ///
    /// # Panics
    ///
    /// Panics if the column exists (is not `None`).
    pub(crate) fn does_not_have_column(self, column: &str) -> Self {
        assert!(self.0.column_value_by(column).unwrap().is_none());
        self
    }
}
