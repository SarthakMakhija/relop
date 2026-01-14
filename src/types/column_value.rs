use crate::types::column_type::ColumnType;

/// Represents the value stored in a column.
///
/// # Examples
///
/// ```
/// use relop::types::column_value::ColumnValue;
///
/// let int_val = ColumnValue::Int(42);
/// let text_val = ColumnValue::Text("hello".to_string());
/// ```
#[derive(Debug, PartialEq, Hash, Eq, Clone, PartialOrd, Ord)]
pub enum ColumnValue {
    /// Integer 64-bit value.
    Int(i64),
    /// String value.
    Text(String),
}

impl ColumnValue {
    /// Extracts the integer value if this is an `Int` variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let val = ColumnValue::Int(42);
    /// assert_eq!(val.int_value(), Some(42));
    ///
    /// let text = ColumnValue::Text("s".to_string());
    /// assert_eq!(text.int_value(), None);
    /// ```
    pub fn int_value(&self) -> Option<i64> {
        if let ColumnValue::Int(value) = self {
            return Some(*value);
        }
        None
    }

    /// Extracts the string slice if this is a `Text` variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let val = ColumnValue::Text("hello".to_string());
    /// assert_eq!(val.text_value(), Some("hello"));
    ///
    /// let int = ColumnValue::Int(42);
    /// assert_eq!(int.text_value(), None);
    /// ```
    pub fn text_value(&self) -> Option<&str> {
        if let ColumnValue::Text(ref value) = self {
            return Some(value);
        }
        None
    }

    /// Returns the corresponding [`ColumnType`] for this value.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::types::column_value::ColumnValue;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let val = ColumnValue::Int(42);
    /// assert_eq!(val.column_type(), ColumnType::Int);
    /// ```
    pub fn column_type(&self) -> ColumnType {
        match self {
            ColumnValue::Int(_) => ColumnType::Int,
            ColumnValue::Text(_) => ColumnType::Text,
        }
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

    #[test]
    fn get_column_type_as_int() {
        let column_value = ColumnValue::Int(100);
        assert_eq!(column_value.column_type(), ColumnType::Int);
    }

    #[test]
    fn get_column_type_as_text() {
        let column_value = ColumnValue::Text("relop".to_string());
        assert_eq!(column_value.column_type(), ColumnType::Text);
    }
}
