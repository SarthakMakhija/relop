use crate::types::column_value::ColumnValue;

/// Represents a single row of data in a table, consisting of multiple column values.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Row {
    values: Vec<ColumnValue>,
}

impl Row {
    /// Creates a row with a single column value.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::storage::row::Row;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let row = Row::single(ColumnValue::int(42));
    /// ```
    pub fn single(value: ColumnValue) -> Row {
        Self::filled(vec![value])
    }

    /// Creates a row with multiple column values.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::storage::row::Row;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let row = Row::filled(vec![
    ///     ColumnValue::int(1),
    ///     ColumnValue::text("alice")
    /// ]);
    /// ```
    pub fn filled(values: Vec<ColumnValue>) -> Row {
        Self { values }
    }

    /// Appends a column value to the row.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::storage::row::Row;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let row = Row::single(ColumnValue::int(1))
    ///     .insert(ColumnValue::text("alice"));
    /// ```
    pub fn insert(mut self, value: ColumnValue) -> Self {
        self.values.push(value);
        self
    }

    /// Returns all column values in the row.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::storage::row::Row;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let row = Row::filled(vec![ColumnValue::int(1)]);
    /// let values = row.column_values();
    /// assert_eq!(1, values.len());
    /// ```
    pub fn column_values(&self) -> &[ColumnValue] {
        &self.values
    }

    /// Returns the column value at the specified index.
    ///
    /// Returns `None` if the index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::storage::row::Row;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let row = Row::single(ColumnValue::int(42));
    /// assert_eq!(Some(&ColumnValue::int(42)), row.column_value_at(0));
    /// assert_eq!(None, row.column_value_at(1));
    /// ```
    pub fn column_value_at(&self, index: usize) -> Option<&ColumnValue> {
        if index < self.values.len() {
            return Some(&self.values[index]);
        }
        None
    }
}

#[cfg(test)]
impl Row {
    fn columns(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::row::{ColumnValue, Row};

    #[test]
    fn create_a_row_with_a_single_column_value() {
        let row = Row::single(ColumnValue::text("relop"));

        assert_eq!(1, row.columns());
        assert_eq!(&ColumnValue::text("relop"), row.column_value_at(0).unwrap());
    }

    #[test]
    fn create_a_row_with_two_column_values() {
        let row = Row::single(ColumnValue::text("relop")).insert(ColumnValue::int(100));

        assert_eq!(2, row.columns());
        assert_eq!(&ColumnValue::text("relop"), row.column_value_at(0).unwrap());
        assert_eq!(&ColumnValue::int(100), row.column_value_at(1).unwrap());
    }

    #[test]
    fn create_a_filled_row_with_two_column_values() {
        let row = Row::filled(vec![ColumnValue::text("relop"), ColumnValue::int(200)]);

        assert_eq!(2, row.columns());
        assert_eq!(&ColumnValue::text("relop"), row.column_value_at(0).unwrap());
        assert_eq!(&ColumnValue::int(200), row.column_value_at(1).unwrap());
    }

    #[test]
    fn column_value_at_index() {
        let row = Row::filled(vec![ColumnValue::text("relop"), ColumnValue::int(200)]);
        let column_value = row.column_value_at(0).unwrap();

        assert_eq!(&ColumnValue::text("relop"), column_value);
    }

    #[test]
    fn attempt_to_get_column_value_at_index_beyond_the_colum_count() {
        let row = Row::filled(vec![ColumnValue::text("relop"), ColumnValue::int(200)]);
        let column_value = row.column_value_at(2);

        assert!(column_value.is_none());
    }
}
