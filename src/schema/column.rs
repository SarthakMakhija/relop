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

    /// Checks if this column is a candidate match for the given search name.
    /// This handles both exact matches and suffix matches for unqualified search names.
    pub fn is_match(&self, search_name: &str) -> bool {
        if self.name.eq_ignore_ascii_case(search_name) {
            return true;
        }

        if !search_name.contains('.') {
            if let Some(dot_index) = self.name.rfind('.') {
                let suffix = &self.name[dot_index + 1..];
                return suffix.eq_ignore_ascii_case(search_name);
            }
        }
        false
    }

    /// Checks if this column's name has the given prefix (e.g. table name or alias).
    pub fn has_prefix(&self, prefix: &str) -> bool {
        if let Some(dot_index) = self.name.rfind('.') {
            let stored_prefix = &self.name[..dot_index];
            return stored_prefix.eq_ignore_ascii_case(prefix);
        }
        false
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

    #[test]
    fn is_match_with_exact_name() {
        assert!(Column::new("id", ColumnType::Int).is_match("id"));
        assert!(Column::new("employees.id", ColumnType::Int).is_match("employees.id"));
    }

    #[test]
    fn is_match_with_ignored_case() {
        assert!(Column::new("id", ColumnType::Int).is_match("ID"));
        assert!(Column::new("employees.id", ColumnType::Int).is_match("EMPLOYEES.ID"));
    }

    #[test]
    fn is_match_with_suffix_for_unqualified_name() {
        assert!(Column::new("employees.id", ColumnType::Int).is_match("id"));
    }

    #[test]
    fn is_match_does_not_match_different_names() {
        assert!(!Column::new("id", ColumnType::Int).is_match("name"));
        assert!(!Column::new("employees.id", ColumnType::Int).is_match("employees.name"));
        assert!(!Column::new("employees.id", ColumnType::Int).is_match("departments.id"));
    }

    #[test]
    fn has_prefix_matches_correct_prefix() {
        assert!(Column::new("employees.id", ColumnType::Int).has_prefix("employees"));
    }

    #[test]
    fn has_prefix_is_case_insensitive() {
        assert!(Column::new("employees.id", ColumnType::Int).has_prefix("EMPLOYEES"));
    }

    #[test]
    fn has_prefix_returns_false_for_wrong_prefix() {
        assert!(!Column::new("employees.id", ColumnType::Int).has_prefix("departments"));
    }

    #[test]
    fn has_prefix_returns_false_for_unqualified_column() {
        assert!(!Column::new("id", ColumnType::Int).has_prefix("employees"));
    }
}
