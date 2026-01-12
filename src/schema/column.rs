use crate::types::column_type::ColumnType;

/// Represents a column in a table schema, including its name and type.
#[derive(Debug, PartialEq, Eq)]
pub struct Column {
    name: String,
    column_type: ColumnType,
}

impl Column {
    /// Creates a new `Column` with the given name and type.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::column::Column;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let col = Column::new("age", ColumnType::Int);
    /// ```
    pub fn new<N: Into<String>>(name: N, column_type: ColumnType) -> Column {
        Column {
            name: name.into(),
            column_type,
        }
    }

    /// Returns the name of the column.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::column::Column;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let col = Column::new("age", ColumnType::Int);
    /// assert_eq!(col.name(), "age");
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::column::Column;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let col = Column::new("age", ColumnType::Int);
    /// assert_eq!(col.column_type(), &ColumnType::Int);
    /// ```
    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    /// Checks if the column name matches the given name, ignoring case.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::column::Column;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let col = Column::new("age", ColumnType::Int);
    /// assert!(col.matches_name("AGE"));
    /// ```
    pub fn matches_name(&self, name: &str) -> bool {
        self.name.eq_ignore_ascii_case(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_column_name() {
        assert!(Column::new("id", ColumnType::Int).matches_name("id"));
    }

    #[test]
    fn matches_column_name_with_ignored_case() {
        assert!(Column::new("id", ColumnType::Int).matches_name("Id"));
    }

    #[test]
    fn does_not_match_column_name() {
        assert!(!Column::new("id", ColumnType::Int).matches_name("first_name"));
    }
}
