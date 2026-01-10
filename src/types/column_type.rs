use crate::types::column_value::ColumnValue;

/// Represents the supported data types for columns in the database.
///
/// # Examples
///
/// ```
/// use relop::types::column_type::ColumnType;
///
/// let int_type = ColumnType::Int;
/// let text_type = ColumnType::Text;
/// ```
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnType {
    /// Integer 64-bit signed type.
    Int,
    /// String type.
    Text,
}

impl ColumnType {
    /// Checks if the given `ColumnValue` matches this `ColumnType`.
    ///
    /// This is an internal helper to validate data insertion compatibility.
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
