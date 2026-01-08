use crate::values::column_value::ColumnValue;

pub(crate) struct Column {
    name: String,
    column_type: ColumnType,
}

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Int,
    Text,
}

impl ColumnType {
    pub fn accepts(&self, value: &ColumnValue) -> bool {
        matches!(
            (self, value),
            (ColumnType::Int, ColumnValue::Int(_)) | (ColumnType::Text, ColumnValue::Text(_))
        )
    }
}

impl Column {
    pub(crate) fn new(name: &str, column_type: ColumnType) -> Column {
        Column {
            name: name.to_string(),
            column_type,
        }
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

#[cfg(test)]
mod column_type_tests {
    use super::*;

    #[test]
    fn column_type_accepts_same_type_text_column_value() {
        let column_type = ColumnType::Text;
        let column_value = ColumnValue::Text("relop".to_string());

        assert!(column_type.accepts(&column_value));
    }

    #[test]
    fn column_type_accepts_same_type_int_column_value() {
        let column_type = ColumnType::Int;
        let column_value = ColumnValue::Int(20);

        assert!(column_type.accepts(&column_value));
    }

    #[test]
    fn column_type_does_not_accept_different_column_value() {
        let column_type = ColumnType::Int;
        let column_value = ColumnValue::Text("relop".to_string());

        assert!(!column_type.accepts(&column_value));
    }
}
