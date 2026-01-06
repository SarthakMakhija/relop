#[derive(Debug, PartialEq, Eq)]
pub struct Row {
    values: Vec<ColumnValue>
}

#[derive(Debug, PartialEq, Eq)]
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

impl Row {
    pub fn empty() -> Row {
        Self {values: Vec::new()}
    }

    pub fn filled(values: Vec<ColumnValue>) -> Row {
        Self {values }
    }

    pub fn add(mut self, value: ColumnValue) -> Self {
        self.values.push(value);
        self
    }

    pub(crate) fn column_values(&self) -> &[ColumnValue] {
        &self.values
    }

    fn column_value_count(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
impl Row {
    fn column_value_at(&self, index: usize) -> Option<&ColumnValue> {
        self.values.get(index)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::row::{ColumnValue, Row};

    #[test]
    fn create_a_row_with_a_single_column_value() {
        let row = Row::empty().add(ColumnValue::Text("relop".to_string()));

        assert_eq!(1, row.column_value_count());
        assert_eq!(&ColumnValue::Text("relop".to_string()), row.column_value_at(0).unwrap());
    }

    #[test]
    fn create_a_row_with_two_column_values() {
        let row = Row::empty().add(ColumnValue::Text("relop".to_string())).add(ColumnValue::Int(100));

        assert_eq!(2, row.column_value_count());
        assert_eq!(&ColumnValue::Text("relop".to_string()), row.column_value_at(0).unwrap());
        assert_eq!(&ColumnValue::Int(100), row.column_value_at(1).unwrap());
    }

    #[test]
    fn create_a_filled_row_with_two_column_values() {
        let row = Row::filled(vec![ColumnValue::Text("relop".to_string()), ColumnValue::Int(200)]);

        assert_eq!(2, row.column_value_count());
        assert_eq!(&ColumnValue::Text("relop".to_string()), row.column_value_at(0).unwrap());
        assert_eq!(&ColumnValue::Int(200), row.column_value_at(1).unwrap());
    }
}

#[cfg(test)]
mod column_value_tests {
    use crate::storage::row::ColumnValue;

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