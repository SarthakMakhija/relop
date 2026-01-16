use crate::catalog::Catalog;
use crate::query::executor::error::ExecutionError;
use crate::schema::primary_key::PrimaryKey;
use crate::schema::Schema;
use crate::storage::row::Row;
use crate::storage::row_view::RowView;
use crate::types::column_type::ColumnType;
use crate::types::column_value::ColumnValue;

/// Creates a new `Schema` with the given columns.
///
/// # Arguments
///
/// * `columns` - A slice of tuples, where each tuple contains the column name and type.
pub fn create_schema(columns: &[(&str, ColumnType)]) -> Schema {
    let mut schema = Schema::new();
    for (name, col_type) in columns {
        schema = schema.add_column(name, col_type.clone()).unwrap();
    }
    schema
}

/// Creates a new `Schema` with the given columns and primary key.
/// # Arguments
///
/// * `columns` - A slice of tuples, where each tuple contains the column name and type.
/// * `primary_key` - Primary key column name.
pub fn create_schema_with_primary_key(columns: &[(&str, ColumnType)], primary_key: &str) -> Schema {
    let mut schema = Schema::new();
    for (name, col_type) in columns {
        schema = schema.add_column(name, col_type.clone()).unwrap();
    }
    schema
        .add_primary_key(PrimaryKey::single(primary_key))
        .unwrap()
}

/// Inserts a single row into the specified table, unwrapping the result.
pub fn insert_row(catalog: &Catalog, table_name: &str, row: Row) {
    catalog.insert_into(table_name, row).unwrap();
}

/// Inserts multiple rows into the specified table, unwrapping the result.
pub fn insert_rows(catalog: &Catalog, table_name: &str, rows: Vec<Row>) {
    catalog.insert_all_into(table_name, rows).unwrap();
}

/// Creates a `Row` from a list of values.
///
/// This macro simplifies row creation in tests by automatically converting
/// provided values into `ColumnValue`s using `From` implementations.
///
/// # Examples
///
/// ```
/// use relop::row;
/// use relop::storage::row::Row;
/// use relop::types::column_value::ColumnValue;
///
/// let row = row![1, "text"];
/// let expected = Row::filled(vec![
///     ColumnValue::int(1),
///     ColumnValue::text("text")
/// ]);
/// assert_eq!(row, expected);
/// ```
#[macro_export]
macro_rules! row {
    ( $( $x:expr ),* ) => {
        {
            use $crate::storage::row::Row;
            use $crate::types::column_value::ColumnValue;
            Row::filled(vec![
                $( ColumnValue::from($x) ),*
            ])
        }
    };
}

/// Creates a `Vec<Row>` from a list of row definitions.
///
/// # Examples
///
/// ```
/// use relop::rows;
/// // use relop::row;
///
/// let batch = rows![[1, "a"], [2, "b"]];
/// ```
#[macro_export]
macro_rules! rows {
    ( $( [ $( $x:expr ),* ] ),* ) => {
        vec![
            $(
                $crate::row![ $( $x ),* ]
            ),*
        ]
    };
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

/// A helper struct for asserting properties of a single `RowView`.
///
/// `RowAssertion` provides a fluent interface for checking expected values of columns
/// within a row. It is typically created via the [`assert_row`] function.
pub struct RowAssertion<'a>(RowView<'a>);

/// Creates a `RowAssertion` for the next row in the given iterator.
///
/// This function advances the iterator, unwraps the result, and returns a `RowAssertion`
/// that can be used to verify the content of the retrieved row.
///
/// # Panics
///
/// Panics if the iterator yields `None` (no more rows) or if it yields an `Err`.
pub fn assert_row<'a>(
    iterator: &'a mut dyn Iterator<Item = Result<RowView, ExecutionError>>,
) -> RowAssertion<'a> {
    let row_view = iterator.next().unwrap().unwrap();
    RowAssertion(row_view)
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
    pub fn match_column<V: Into<ColumnValue>>(self, column: &str, expected: V) -> Self {
        let actual = self.0.column_value_by(column).expect("Column not found");
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
    pub fn does_not_have_column(self, column: &str) -> Self {
        assert!(self.0.column_value_by(column).is_none());
        self
    }
}
