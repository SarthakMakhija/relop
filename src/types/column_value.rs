#[derive(Debug, PartialEq, Hash, Eq, Clone)]
pub enum ColumnValue {
    Int(i64),
    Text(String),
}

impl ColumnValue {
    pub(crate) fn int_value(&self) -> Option<i64> {
        if let ColumnValue::Int(value) = self {
            return Some(*value);
        }
        None
    }

    pub(crate) fn text_value(&self) -> Option<&str> {
        if let ColumnValue::Text(ref value) = self {
            return Some(value);
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn int_value() {
        let column_value = ColumnValue::Int(100);
        assert_eq!(Some(100), column_value.int_value());
    }

    #[test]
    fn attempt_to_get_int_value_for_a_non_int_column_type() {
        let column_value = ColumnValue::Text("relop".to_string());
        assert_eq!(None, column_value.int_value());
    }

    #[test]
    fn text_value() {
        let column_value = ColumnValue::Text("relop".to_string());
        assert_eq!(Some("relop"), column_value.text_value());
    }

    #[test]
    fn attempt_to_get_text_value_for_a_non_text_column_type() {
        let column_value = ColumnValue::Int(100);
        assert_eq!(None, column_value.text_value());
    }
}
