use crate::types::column_value::ColumnValue;

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Int,
    Text,
}

impl ColumnType {
    pub(crate) fn accepts(&self, value: &ColumnValue) -> bool {
        matches!(
            (self, value),
            (ColumnType::Int, ColumnValue::Int(_)) | (ColumnType::Text, ColumnValue::Text(_))
        )
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
