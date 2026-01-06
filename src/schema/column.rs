pub(crate) struct Column {
    name: String,
    column_type: ColumnType,
}

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Int,
    String,
}

impl Column {
    pub(crate) fn new(name: &str, column_type: ColumnType) -> Column {
        Column { name: name.to_string(), column_type }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
    pub(crate) fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    pub(crate) fn matches_name(&self, name: &str) -> bool {
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