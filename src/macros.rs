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
///
/// let row = row![1, "text"];
/// /*
/// let expected = Row::filled(vec![
///     ColumnValue::int(1),
///     ColumnValue::text("text")
/// ]);
/// assert_eq!(row, expected);
/// */
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
