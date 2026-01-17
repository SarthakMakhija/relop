/// Creates a `Row` from a list of values.
///
/// This macro simplifies row creation by automatically converting
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
///
/// let expected = Row::filled(vec![
///     ColumnValue::int(1),
///     ColumnValue::text("text")
/// ]);
/// assert_eq!(row, expected);
///
/// ```
#[macro_export]
macro_rules! row {
    ( $( $x:expr ),* ) => {
        {
            use $crate::storage::row::Row;
            Row::filled(vec![
                $( $x.into() ),*
            ])
        }
    };
}

/// Creates a `Vec<Row>` from a list of row definitions.
///
/// # Examples
///
/// ```
/// use relop::{row, rows};
///
/// let rows: Vec<_> = rows![[1, "a"], [2, "b"]];
/// assert_eq!(2, rows.len());
///
/// let row1 = row![1, "a"];
/// assert_eq!(&row1, rows.get(0).unwrap());
///
/// let row2 = row![2, "b"];
/// assert_eq!(&row2, rows.get(1).unwrap());
///
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

/// Creates a `Schema` from a list of column definitions.
///
/// # Returns
///
/// Returns a `Result<Schema, SchemaError>`.
///
/// # Examples
///
/// ```
/// use relop::schema;
/// use relop::types::column_type::ColumnType;
///
/// let schema = schema![
///     "id" => ColumnType::Int,
///     "name" => ColumnType::Text
/// ].unwrap();
///
/// assert_eq!(2, schema.column_count());
/// ```
#[macro_export]
macro_rules! schema {
    ($($name:expr => $ty:expr),* $(,)?) => {{
        use $crate::schema::Schema;
        use $crate::schema::error::SchemaError;
        // Move the logic into a closure so we can use `?` safely
        let schema_creation = || -> Result<Schema, SchemaError> {
            let mut schema = Schema::new();
            $(
                schema = schema.add_column($name, $ty)?;
            )*
            Ok(schema)
        };
        schema_creation()
    }};
}
